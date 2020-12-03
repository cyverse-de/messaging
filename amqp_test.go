package messaging

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/streadway/amqp"
)

var client *Client

func GetClient(t *testing.T) *Client {
	var err error
	if client != nil {
		return client
	}
	client, err = NewClient(uri(), false)
	if err != nil {
		t.Error(err)
	}
	_ = client.SetupPublishing(exchange())
	go client.Listen()
	return client
}

func shouldrun() bool {
	return os.Getenv("RUN_INTEGRATION_TESTS") != ""
}

func uri() string {
	return "amqp://guest:guest@rabbit:5672/%2fde"
}

func exchange() string {
	return "de"
}

func exchangeType() string {
	return "topic"
}

func TestConstants(t *testing.T) {
	expected := 0
	actual := int(Launch)
	if actual != expected {
		t.Errorf("Launch was %d instead of %d", actual, expected)
	}
	expected = 1
	actual = int(Stop)
	if actual != expected {
		t.Errorf("Stop was %d instead of %d", actual, expected)
	}
	expected = 0
	actual = int(Success)
	if actual != expected {
		t.Errorf("Success was %d instead of %d", actual, expected)
	}
}

func TestNewStopRequest(t *testing.T) {
	actual := NewStopRequest()
	expected := &StopRequest{Version: 0}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("NewStopRequest returned:\n%#v\n\tinstead of:\n%#v", actual, expected)
	}
}
func TestNewLaunchRequest(t *testing.T) {
	actual := NewLaunchRequest("")
	expected := &JobRequest{
		Version: 1,
		Command: Launch,
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("NewLaunchRequest returned:\n%#v\n\tinstead of:\n%#v", actual, expected)
	}
}

func TestNewClient(t *testing.T) {
	if !shouldrun() {
		return
	}
	actual, err := NewClient(uri(), false)
	if err != nil {
		t.Error(err)
	}
	defer actual.Close()
	expected := uri()
	if actual.uri != expected {
		t.Errorf("Client's uri was %s instead of %s", actual.uri, expected)
	}
}

func runPublishingTest(t *testing.T, queue, key string, publish func(*Client), check func([]byte)) {
	if !shouldrun() {
		return
	}

	// Set up the handler function along with a channel to avoid race conditions.
	actual := make([]byte, 0)
	coord := make(chan int)
	handler := func(d amqp.Delivery) {
		_ = d.Ack(false)
		actual = d.Body
		coord <- 1
	}

	// Set up the AMQP client.
	client := GetClient(t)
	client.AddConsumer(exchange(), exchangeType(), queue, key, handler, 0)

	// Publish the message and check the value that was actually published.
	publish(client)
	<-coord
	check(actual)
}

func TestClient(t *testing.T) {
	queue := "test_queue"
	key := "tests"
	expected := []byte("this is a test")

	publish := func(c *Client) {
		_ = c.Publish(key, expected)
	}

	check := func(actual []byte) {
		if string(actual) != string(expected) {
			t.Errorf("handler received %s instead of %s", actual, expected)
		}
	}

	runPublishingTest(t, queue, key, publish, check)
}

func TestSendTimeLimitRequest(t *testing.T) {
	queue := "test_queue1"
	key := TimeLimitRequestKey("test")

	publish := func(c *Client) {
		_ = client.SendTimeLimitRequest("test")
	}

	check := func(actual []byte) {
		req := &TimeLimitRequest{}
		err := json.Unmarshal(actual, req)
		if err != nil {
			t.Error(err)
		}
		if req.InvocationID != "test" {
			t.Errorf("TimeLimitRequest's InvocationID was %s instead of test", req.InvocationID)
		}
	}

	runPublishingTest(t, queue, key, publish, check)
}

func TestSendTimeLimitResponse(t *testing.T) {
	queue := "test_queue2"
	key := TimeLimitResponsesKey("test")

	publish := func(c *Client) {
		_ = client.SendTimeLimitResponse("test", 0)
	}

	check := func(actual []byte) {
		resp := &TimeLimitResponse{}
		err := json.Unmarshal(actual, resp)
		if err != nil {
			t.Error(err)
		}
		if resp.InvocationID != "test" {
			t.Errorf("TimeLimitRequest's InvocationID was %s instead of test", resp.InvocationID)
		}
	}

	runPublishingTest(t, queue, key, publish, check)
}

func TestSendTimeLimitDelta(t *testing.T) {
	queue := "test_queue3"
	key := TimeLimitDeltaRequestKey("test")

	publish := func(c *Client) {
		_ = client.SendTimeLimitDelta("test", "10s")
	}

	check := func(actual []byte) {
		delta := &TimeLimitDelta{}
		err := json.Unmarshal(actual, delta)
		if err != nil {
			t.Error(err)
		}
		if delta.InvocationID != "test" {
			t.Errorf("TimeLimitDelta's InvocationID was %s instead of test", delta.InvocationID)
		}
		if delta.Delta != "10s" {
			t.Errorf("TimeLimitDelta's Delta was %s instead of 10s", delta.Delta)
		}
	}

	runPublishingTest(t, queue, key, publish, check)
}

func TestSendStopRequest(t *testing.T) {
	queue := "test_queue4"
	invID := "test"
	key := StopRequestKey(invID)

	publish := func(c *Client) {
		_ = client.SendStopRequest(invID, "test_user", "this is a test")
	}

	check := func(actual []byte) {
		req := &StopRequest{}
		if err := json.Unmarshal(actual, req); err != nil {
			t.Error(err)
		}
		if req.Reason != "this is a test" {
			t.Errorf("Reason was '%s' instead of '%s'", req.Reason, "this is a test")
		}
		if req.InvocationID != invID {
			t.Errorf("InvocationID was %s instead of %s", req.InvocationID, invID)
		}
		if req.Username != "test_user" {
			t.Errorf("Username was %s instead of %s", req.Username, "test_user")
		}
	}

	runPublishingTest(t, queue, key, publish, check)
}

func TestCreateQueue(t *testing.T) {
	if !shouldrun() {
		return
	}
	client := GetClient(t)
	actual, err := client.CreateQueue("test_queue5", exchange(), "test_key5", true, false)
	if err != nil {
		t.Error(err)
	}
	if actual == nil {
		t.Error("channel is nil")
	}
	if _, err = actual.QueueInspect("test_queue5"); err != nil {
		t.Error(err)
	}
	if err = actual.Close(); err != nil {
		t.Error(err)
	}
}

func TestQueueExists(t *testing.T) {
	if !shouldrun() {
		return
	}
	client := GetClient(t)
	actual, err := client.CreateQueue("test_queue5", exchange(), "test_key5", true, false)
	if err != nil {
		t.Error(err)
	}
	if actual == nil {
		t.Error("channel is nil")
	}
	exists, err := client.QueueExists("test_queue5")
	if err != nil {
		t.Error(err)
	}
	if !exists {
		t.Error("Queue 'test_queue5' was not found")
	}
	if err = actual.Close(); err != nil {
		t.Error(err)
	}
}

func TestDeleteQueue(t *testing.T) {
	if !shouldrun() {
		return
	}
	client := GetClient(t)
	actual, err := client.CreateQueue("test_queue6", exchange(), "test_key5", true, false)
	if err != nil {
		t.Error(err)
	}
	if actual == nil {
		t.Error("channel is nil")
	}
	exists, err := client.QueueExists("test_queue6")
	if err != nil {
		t.Error(err)
	}
	if !exists {
		t.Error("Queue 'test_queue6' was not found")
	}

	actual, err = client.CreateQueue("test_queue7", exchange(), "test_key6", true, false)
	if err != nil {
		t.Error(err)
	}
	if actual == nil {
		t.Error("channel is nil")
	}
	exists, err = client.QueueExists("test_queue7")
	if err != nil {
		t.Error(err)
	}
	if !exists {
		t.Error("Queue 'test_queue7' was not found")
	}

	actual, err = client.CreateQueue("test_queue8", exchange(), "test_key7", true, false)
	if err != nil {
		t.Error(err)
	}
	if actual == nil {
		t.Error("channel is nil")
	}
	exists, err = client.QueueExists("test_queue8")
	if err != nil {
		t.Error(err)
	}
	if !exists {
		t.Error("Queue 'test_queue8' was not found")
	}

	if err = client.DeleteQueue("test_queue6"); err != nil {
		t.Error(err)
	}
	exists, err = client.QueueExists("test_queue6")
	if err != nil {
		t.Error(err)
	}
	if exists {
		t.Error("Queue 'test_queue6' was found")
	}

	if err = client.DeleteQueue("test_queue7"); err != nil {
		t.Error(err)
	}
	exists, err = client.QueueExists("test_queue7")
	if err != nil {
		t.Error(err)
	}
	if exists {
		t.Error("Queue 'test_queue7' was found")
	}

	if err = client.DeleteQueue("test_queue8"); err != nil {
		t.Error(err)
	}
	exists, err = client.QueueExists("test_queue8")
	if err != nil {
		t.Error(err)
	}
	if exists {
		t.Error("Queue 'test_queue8' was found")
	}

	if err = actual.Close(); err != nil {
		t.Error(err)
	}
}

func TestTimeLimitRequestKey(t *testing.T) {
	invID := "test"
	actual := TimeLimitRequestKey(invID)
	expected := fmt.Sprintf("%s.%s", TimeLimitRequestsKey, invID)
	if actual != expected {
		t.Errorf("TimeLimitRequestKey returned %s instead of %s", actual, expected)
	}
}

func TestTimeLimitRequestQueueName(t *testing.T) {
	invID := "test"
	actual := TimeLimitRequestQueueName(invID)
	expected := fmt.Sprintf("road-runner-%s-tl-request", invID)
	if actual != expected {
		t.Errorf("TimeLimitRequestQueueName returned %s instead of %s", actual, expected)
	}
}

func TestTimeLimitResponsesKey(t *testing.T) {
	invID := "test"
	actual := TimeLimitResponsesKey(invID)
	expected := fmt.Sprintf("%s.%s", TimeLimitResponseKey, invID)
	if actual != expected {
		t.Errorf("TimeLimitResponsesKey returned %s instead of %s", actual, expected)
	}
}

func TestTimeLimitResponsesQueueName(t *testing.T) {
	invID := "test"
	actual := TimeLimitResponsesQueueName(invID)
	expected := fmt.Sprintf("road-runner-%s-tl-response", invID)
	if actual != expected {
		t.Errorf("TimeLimitResponsesQueueName returned %s instead of %s", actual, expected)
	}
}

func TestTimeLimitDeltaRequestKey(t *testing.T) {
	invID := "test"
	actual := TimeLimitDeltaRequestKey(invID)
	expected := fmt.Sprintf("%s.%s", TimeLimitDeltaKey, invID)
	if actual != expected {
		t.Errorf("TimeLimitDeltaRequestKey returned %s instead of %s", actual, expected)
	}
}

func TestStopRequestKey(t *testing.T) {
	invID := "test"
	actual := StopRequestKey(invID)
	expected := fmt.Sprintf("%s.%s", StopsKey, invID)
	if actual != expected {
		t.Errorf("StopRequestKey returned %s instead of %s", actual, expected)
	}
}

func TestTimeLimitDeltaQueueName(t *testing.T) {
	invID := "test"
	actual := TimeLimitDeltaQueueName(invID)
	expected := fmt.Sprintf("road-runner-%s-tl-delta", invID)
	if actual != expected {
		t.Errorf("TimeLimitDeltaQueueName returned %s instead of %s", actual, expected)
	}
}

func TestStopQueueName(t *testing.T) {
	invID := "test"
	actual := StopQueueName(invID)
	expected := fmt.Sprintf("road-runner-%s-stops-request", invID)
	if actual != expected {
		t.Errorf("StopQueueName returneed %s instead of %s", actual, expected)
	}
}
