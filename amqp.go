// Package messaging provides the logic and data structures that the services
// will need to communicate with each other over AMQP (as implemented
// by RabbitMQ).
package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/streadway/amqp"

	"go.opentelemetry.io/otel"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"
)

// TimeLimitRequestKey returns the formatted binding key based on the passed in
// job InvocationID.
func TimeLimitRequestKey(invID string) string {
	return fmt.Sprintf("%s.%s", TimeLimitRequestsKey, invID)
}

// TimeLimitRequestQueueName returns the formatted queue name for time limit
// requests. It is based on the passed in job InvocationID.
func TimeLimitRequestQueueName(invID string) string {
	return fmt.Sprintf("road-runner-%s-tl-request", invID)
}

// TimeLimitResponsesKey returns the formatted binding key based on the passed in
// job InvocationID.
func TimeLimitResponsesKey(invID string) string {
	return fmt.Sprintf("%s.%s", TimeLimitResponseKey, invID)
}

// TimeLimitResponsesQueueName returns the formatted queue name for time limit
// responses. It is based on the passed in job InvocationID.
func TimeLimitResponsesQueueName(invID string) string {
	return fmt.Sprintf("road-runner-%s-tl-response", invID)
}

// TimeLimitDeltaRequestKey returns the binding key formatted correctly for the
// jobs exchange based on the InvocationID passed in.
func TimeLimitDeltaRequestKey(invID string) string {
	return fmt.Sprintf("%s.%s", TimeLimitDeltaKey, invID)
}

// TimeLimitDeltaQueueName returns the correctly formatted queue name for time
// limit delta requests. It's based on the passed in string, which is assumed to
// be the InvocationID for a job, but there's no reason that is required to be
// the case.
func TimeLimitDeltaQueueName(invID string) string {
	return fmt.Sprintf("road-runner-%s-tl-delta", invID)
}

// StopRequestKey returns the binding key formatted correctly for the jobs
// exchange based on the InvocationID passed in.
func StopRequestKey(invID string) string {
	return fmt.Sprintf("%s.%s", StopsKey, invID)
}

// StopQueueName returns the formatted queue name for job stop requests. It's
// based on the passed in string, which is assumed to be the InvocationID for a
// job, but there's no reason that is required to the case.
func StopQueueName(invID string) string {
	return fmt.Sprintf("road-runner-%s-stops-request", invID)
}

// MessageHandler defines a type for amqp.Delivery handlers.
type MessageHandler func(context.Context, amqp.Delivery)

type aggregationMessage struct {
	handler  MessageHandler
	queue    string
	delivery amqp.Delivery
}

type consumer struct {
	exchange        string
	exchangeType    string
	queue           string
	keys            []string
	handler         MessageHandler
	queueDurable    bool
	queueAutoDelete bool
	prefetchCount   int
}

type consumeradder struct {
	consumer consumer
	latch    chan int
}

type publisher struct {
	exchange string
	channel  *amqp.Channel
}

// Client encapsulates the information needed to interact via AMQP.
type Client struct {
	uri             string
	connection      *amqp.Connection
	aggregationChan chan aggregationMessage
	errors          chan *amqp.Error
	consumers       []*consumer
	consumersChan   chan consumeradder
	publisher       *publisher
	Reconnect       bool
}

// NewClient returns a new *Client. It will block until the connection succeeds.
func NewClient(uri string, reconnect bool) (*Client, error) {
	c := &Client{}
	randomizer := rand.New(rand.NewSource(time.Now().UnixNano()))
	c.uri = uri
	c.Reconnect = reconnect
	Info.Println("Attempting AMQP connection...")
	var connection *amqp.Connection
	var err error
	if c.Reconnect {
		for {
			connection, err = amqp.Dial(c.uri)
			if err != nil {
				Error.Print(err)
				waitFor := randomizer.Intn(10)
				Info.Printf("Re-attempting connection in %d seconds", waitFor)
				time.Sleep(time.Duration(waitFor) * time.Second)
			} else {
				Info.Println("Successfully connected to the AMQP broker")
				break
			}
		}
	} else {
		connection, err = amqp.Dial(c.uri)
		if err != nil {
			return nil, err
		}
		Info.Println("Successfully connected to the AMQP broker")
	}
	c.connection = connection
	c.consumersChan = make(chan consumeradder)
	c.aggregationChan = make(chan aggregationMessage)
	c.errors = c.connection.NotifyClose(make(chan *amqp.Error))
	return c, nil
}

// Listen will wait for messages and pass them off to handlers, which run in
// their own goroutine.
func (c *Client) Listen() {
	var consumers []*consumer
	// init := func() {
	// 	for _, cs := range c.consumers {
	// 		c.initconsumer(cs)
	// 	}
	// }
	// init()
	// for _, cs := range c.consumers {
	// 	consumers = append(consumers, cs)
	// }
	for {
		select {
		case cs := <-c.consumersChan:
			Info.Println("A new consumer is being added")
			_ = c.initconsumer(&cs.consumer)
			consumers = append(consumers, &cs.consumer)
			Info.Println("Done adding a new consumer")
			cs.latch <- 1
		case err := <-c.errors:
			Error.Printf("An error in the connection to the AMQP broker occurred:\n%s", err)
			if c.Reconnect {
				closeErr := c.connection.Close()
				if closeErr != nil && closeErr != amqp.ErrClosed {
					Error.Printf("An error closing the old connection occurred:\n%s", closeErr)
				}
				c, _ = NewClient(c.uri, c.Reconnect)
				c.consumers = consumers
				for _, cs := range c.consumers {
					cerr := c.initconsumer(cs)
					if cerr != nil {
						Error.Printf("An error re-establishing an AMQP consumer occurred:\n%s", cerr)
					}
				}
				if c.publisher != nil {
					perr := c.SetupPublishing(c.publisher.exchange)
					if perr != nil {
						Error.Printf("An error re-establishing AMQP publishing occurred:\n%s", perr)
					}
				}
			} else {
				os.Exit(-1)
			}
		case msg := <-c.aggregationChan:
			go func(deliveryMsg aggregationMessage) {
				ctx := otel.GetTextMapPropagator().Extract(context.Background(), AMQPHeaderCarrier(deliveryMsg.delivery.Headers))
				tracer := newTracer(otel.GetTracerProvider())
				ctx, span := tracer.Start(ctx, deliveryMsg.queue+" process", trace.WithSpanKind(trace.SpanKindConsumer))
				defer span.End()

				span.SetAttributes(
					semconv.MessagingSystemKey.String("rabbitmq"),
					semconv.MessagingProtocolKey.String("AMQP"),
					semconv.MessagingProtocolVersionKey.String("0.9.1"),
					semconv.MessagingRabbitmqRoutingKeyKey.String(deliveryMsg.delivery.RoutingKey),
					semconv.MessagingOperationKey.String("process"),
					semconv.MessagingDestinationKey.String(deliveryMsg.delivery.Exchange),
					semconv.MessagingConsumerIDKey.String(deliveryMsg.delivery.ConsumerTag),
				)

				deliveryMsg.handler(ctx, deliveryMsg.delivery)
			}(msg)
		}
	}
}

// Close closes the connection to the AMQP broker.
func (c *Client) Close() {
	c.connection.Close()
}

// AddConsumerMulti adds a consumer to the list of consumers that need to be created
// each time the client is set up. Note that this just adds the consumers to a
// list, it doesn't actually start handling messages yet. You need to call
// Listen() for that.
func (c *Client) AddConsumerMulti(exchange, exchangeType, queue string, keys []string, handler MessageHandler, prefetchCount int) {
	cs := consumer{
		exchange:        exchange,
		exchangeType:    exchangeType,
		queue:           queue,
		keys:            keys,
		handler:         handler,
		queueDurable:    true,
		queueAutoDelete: false,
		prefetchCount:   prefetchCount,
	}
	adder := consumeradder{
		consumer: cs,
		latch:    make(chan int),
	}
	c.consumersChan <- adder
	<-adder.latch
}

// AddConsumer adds a consumer with only one binding, which is usually what you need
func (c *Client) AddConsumer(exchange, exchangeType, queue, key string, handler MessageHandler, prefetchCount int) {
	c.AddConsumerMulti(exchange, exchangeType, queue, []string{key}, handler, prefetchCount)
}

// AddDeletableConsumer adds a consumer to the list of consumers that need to be
// created each time the client is set up. Unlike AddConsumer(), the new
// consumer will have auto-delete set to true and durable set to false. Make
// sure that Listen() has been called before calling this function.
// This only supports a single bind key, for now.
func (c *Client) AddDeletableConsumer(exchange, exchangeType, queue, key string, handler MessageHandler) {
	cs := consumer{
		exchange:        exchange,
		exchangeType:    exchangeType,
		queue:           queue,
		keys:            []string{key},
		handler:         handler,
		queueDurable:    false,
		queueAutoDelete: true,
	}
	adder := consumeradder{
		consumer: cs,
		latch:    make(chan int),
	}
	c.consumersChan <- adder
	<-adder.latch
}

// CreateQueue creates a queue with the given name, durability, and auto-delete
// settings. It then binds it to the given exchange with the provided key. This
// function does not declare the exchange.
func (c *Client) CreateQueue(name, exchange, key string, durable, autoDelete bool) (*amqp.Channel, error) {
	channel, err := c.connection.Channel()
	if err != nil {
		return nil, err
	}

	if _, err = channel.QueueDeclare(
		name,
		durable,
		autoDelete,
		false, //internal
		false, //no wait
		nil,   //args
	); err != nil {
		return nil, err
	}

	if err = channel.QueueBind(
		name,
		key,
		exchange,
		false, //no wait
		nil,   //args
	); err != nil {
		return nil, err
	}
	return channel, nil
}

// QueueExists returns true if the given queue name exists, false or an error
// otherwise.
func (c *Client) QueueExists(name string) (bool, error) {
	channel, err := c.connection.Channel()
	if err != nil {
		return false, err
	}
	defer channel.Close()
	if _, err = channel.QueueInspect(name); err != nil {
		if strings.Contains(err.Error(), "404") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// DeleteQueue deletes the queue with the given name without regards to safety.
func (c *Client) DeleteQueue(name string) error {
	channel, err := c.connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()
	_, err = channel.QueueDelete(name, false, false, false)
	return err
}

// PurgeQueue purges messages from the queue without regards to safety.
func (c *Client) PurgeQueue(name string) error {
	channel, err := c.connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()
	_, err = channel.QueuePurge(name, true)
	return err
}

func (c *Client) initconsumer(cs *consumer) error {
	channel, err := c.connection.Channel()
	if err != nil {
		return err
	}
	// for consumers, if the channel closes, refresh everything
	c.errors = channel.NotifyClose(c.errors)

	if cs.prefetchCount > 0 {
		err = channel.Qos(
			cs.prefetchCount, // prefetchCount
			0,                // prefetchSize
			false,            // global: false => count applied separately to each new consumer on the channel
		)
		if err != nil {
			Error.Printf("Error setting QOS: %v", err)
		}
	}
	err = channel.ExchangeDeclare(
		cs.exchange,     //name
		cs.exchangeType, //kind
		true,            //durable
		false,           //auto-delete
		false,           //internal
		false,           //no-wait
		nil,             //args
	)
	if err != nil {
		Error.Printf("ExchangeDeclare Error: %v", err)
	}
	_, err = channel.QueueDeclare(
		cs.queue,
		cs.queueDurable,    //durable
		cs.queueAutoDelete, //auto-delete
		false,              //internal
		false,              //no-wait
		nil,                //args
	)
	if err != nil {
		Error.Printf("QueueDeclare Error: %v", err)
	}

	for _, key := range cs.keys {
		err = channel.QueueBind(
			cs.queue,
			key,
			cs.exchange,
			false, //no-wait
			nil,   //args
		)
	}
	if err != nil {
		Error.Printf("QueueBind Error: %v", err)
	}

	d, err := channel.Consume(
		cs.queue,
		"",    //consumer tag - auto-assigned in this case
		false, //auto-ack
		false, //exclusive
		false, //no-local
		false, //no-wait
		nil,   //args
	)
	if err != nil {
		return err
	}
	go func() {
		for msg := range d {
			c.aggregationChan <- aggregationMessage{
				handler:  cs.handler,
				queue:    cs.queue,
				delivery: msg,
			}
		}
	}()
	return err
}

// SetupPublishing initializes the publishing functionality of the client.
// Call this before calling Publish.
func (c *Client) SetupPublishing(exchange string) error {
	channel, err := c.connection.Channel()
	if err != nil {
		return err
	}
	// If the publishing channel closes, re-establish everything.
	c.errors = channel.NotifyClose(c.errors)
	err = channel.ExchangeDeclare(
		exchange, //name
		"topic",  //kind
		true,     //durable
		false,    //auto-delete
		false,    //internal
		false,    //no-wait
		nil,      //args
	)
	if err != nil {
		return err
	}
	p := &publisher{
		exchange: exchange,
		channel:  channel,
	}
	c.publisher = p
	return err
}

// PublishingOpts contains a set of options for publishing AMQP messages.
type PublishingOpts struct {
	DeliveryMode uint8
	ContentType  string
}

// DefaultPublishingOpts defines the set of publishing options used by default.
var DefaultPublishingOpts = &PublishingOpts{
	DeliveryMode: amqp.Persistent,
	ContentType:  "text/plain",
}

// JSONPublishingOpts defines the set of publishing options used for JSON message
// bodies.
var JSONPublishingOpts = &PublishingOpts{
	DeliveryMode: amqp.Persistent,
	ContentType:  "application/json",
}

func newTracer(tp trace.TracerProvider) trace.Tracer {
	return tp.Tracer("github.com/cyverse-de/messaging")
}

// PublishCtxOpts sends a message to the configured exchange, using context and
// the options specified
func (c *Client) PublishContextOpts(ctx context.Context, key string, body []byte, opts *PublishingOpts) error {
	var tracer trace.Tracer
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		tracer = newTracer(span.TracerProvider())
	} else {
		tracer = newTracer(otel.GetTracerProvider())
	}

	ctx, span := tracer.Start(ctx, c.publisher.exchange+" send", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	span.SetAttributes(
		semconv.MessagingSystemKey.String("rabbitmq"),
		semconv.MessagingProtocolKey.String("AMQP"),
		semconv.MessagingProtocolVersionKey.String("0.9.1"),
		semconv.MessagingRabbitmqRoutingKeyKey.String(key),
		semconv.MessagingDestinationKey.String(c.publisher.exchange),
	)

	var headers = make(amqp.Table)

	otel.GetTextMapPropagator().Inject(ctx, AMQPHeaderCarrier(headers))

	msg := amqp.Publishing{
		DeliveryMode: opts.DeliveryMode,
		Timestamp:    time.Now(),
		ContentType:  opts.ContentType,
		Body:         body,
		Headers:      headers,
	}
	err := c.publisher.channel.Publish(
		c.publisher.exchange,
		key,
		false, //mandatory
		false, //immediate
		msg,
	)
	return err
}

// PublishCtxOpts sends a message to the configured exchange, using context and
// default options
func (c *Client) PublishContext(ctx context.Context, key string, body []byte) error {
	return c.PublishContextOpts(ctx, key, body, DefaultPublishingOpts)
}

// PublishOpts sends a message to the configured exchange with options specified
// in an options structure.
func (c *Client) PublishOpts(key string, body []byte, opts *PublishingOpts) error {
	return c.PublishContextOpts(context.Background(), key, body, opts)
}

// Publish sends a message to the configured exchange.
func (c *Client) Publish(key string, body []byte) error {
	return c.PublishContextOpts(context.Background(), key, body, DefaultPublishingOpts)
}

// PublishJobUpdateContext sends a message to the configured exchange with a routing key of
// "jobs.updates"
func (c *Client) PublishJobUpdateContext(context context.Context, u *UpdateMessage) error {
	if u.SentOn == "" {
		u.SentOn = strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10)
	}
	msgJSON, err := json.Marshal(u)
	if err != nil {
		return err
	}
	return c.PublishContext(context, UpdatesKey, msgJSON)
}

func (c *Client) PublishJobUpdate(u *UpdateMessage) error {
	return c.PublishJobUpdateContext(context.Background(), u)
}

// PublishEmailRequestContext sends a message to the configured exchange with a
// key of "email.requests"
func (c *Client) PublishEmailRequestContext(context context.Context, e *EmailRequest) error {
	msgJSON, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return c.PublishContext(context, EmailRequestPublishingKey, msgJSON)
}

func (c *Client) PublishEmailRequest(e *EmailRequest) error {
	return c.PublishEmailRequestContext(context.Background(), e)
}

// PublishNotificationMessageContext sends a message to the configured exchange with a
// key of "notification.{user}", where "{user}" is the username of the person
// receiving the notification.
func (c *Client) PublishNotificationMessageContext(context context.Context, n *WrappedNotificationMessage) error {
	routingKey := fmt.Sprintf("notification.%s", n.Message.User)
	msgJSON, err := json.Marshal(n)
	if err != nil {
		return err
	}
	return c.PublishContextOpts(context, routingKey, msgJSON, JSONPublishingOpts)
}

func (c *Client) PublishNotificationMessage(n *WrappedNotificationMessage) error {
	return c.PublishNotificationMessageContext(context.Background(), n)
}

// SendTimeLimitRequestContext sends out a message to the job on the
// "jobs.timelimits.requests.<invocationID>" topic. This should trigger the job
// to emit a TimeLimitResponse.
func (c *Client) SendTimeLimitRequestContext(context context.Context, invID string) error {
	req := &TimeLimitRequest{
		InvocationID: invID,
	}
	msg, err := json.Marshal(req)
	if err != nil {
		return err
	}
	return c.PublishContext(context, TimeLimitRequestKey(invID), msg)
}

// SendTimeLimitRequest is SendTimeLimitRequestContext with a default context
func (c *Client) SendTimeLimitRequest(invID string) error {
	return c.SendTimeLimitRequestContext(context.Background(), invID)
}

// SendTimeLimitResponseContext sends out a message to the
// jobs.timelimits.responses.<invocationID> topic containing the remaining time
// for the job.
func (c *Client) SendTimeLimitResponseContext(context context.Context, invID string, timeRemaining int64) error {
	resp := &TimeLimitResponse{
		InvocationID:          invID,
		MillisecondsRemaining: timeRemaining,
	}
	msg, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	return c.PublishContext(context, TimeLimitResponsesKey(invID), msg)
}

// SendTimeLimitResponse is SendTimeLimitResponseContext with a default context
func (c *Client) SendTimeLimitResponse(invID string, timeRemaining int64) error {
	return c.SendTimeLimitResponseContext(context.Background(), invID, timeRemaining)
}

// SendTimeLimitDeltaContext sends out a message to the
// jobs.timelimits.deltas.<invocationID> topic containing how the job should
// adjust its timelimit.
func (c *Client) SendTimeLimitDeltaContext(context context.Context, invID, delta string) error {
	d := &TimeLimitDelta{
		InvocationID: invID,
		Delta:        delta,
	}
	msg, err := json.Marshal(d)
	if err != nil {
		return err
	}
	return c.PublishContext(context, TimeLimitDeltaRequestKey(invID), msg)
}

// SendTimeLimitDelta is SendTimeLimitDeltaContext with a default context
func (c *Client) SendTimeLimitDelta(invID, delta string) error {
	return c.SendTimeLimitDeltaContext(context.Background(), invID, delta)
}

// SendStopRequestContext sends out a message to the jobs.stops.<invocation_id> topic
// telling listeners to stop their job.
func (c *Client) SendStopRequestContext(context context.Context, invID, user, reason string) error {
	s := NewStopRequest()
	s.Username = user
	s.Reason = reason
	s.InvocationID = invID
	msg, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return c.PublishContext(context, StopRequestKey(invID), msg)
}

// SendStopRequest is SendStopRequestContext with a default context
func (c *Client) SendStopRequest(invID, user, reason string) error {
	return c.SendStopRequestContext(context.Background(), invID, user, reason)
}
