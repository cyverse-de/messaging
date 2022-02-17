// Package messaging provides the logic and data structures that the services
// will need to communicate with each other over AMQP (as implemented
// by RabbitMQ).
package messaging

// JobDetails describes only the fields of a Job required for messages.
type JobDetails struct {
	InvocationID string `json:"uuid"`
	CondorID     string `json:"condor_id"`
}

// JobRequest is a generic request type for job related requests.
type JobRequest struct {
	Job     JobDetails
	Command Command
	Message string
	Version int
}

// StopRequest contains the information needed to stop a job
type StopRequest struct {
	Reason       string
	Username     string
	Version      int
	InvocationID string
}

// UpdateMessage contains the information needed to broadcast a change in state
// for a job.
type UpdateMessage struct {
	Job     JobDetails
	Version int
	State   JobState
	Message string
	SentOn  string // Should be the milliseconds since the epoch
	Sender  string // Should be the hostname of the box sending the message.
}

// TimeLimitRequest is the message that is sent to road-runner to get it to
// broadcast its current time limit.
type TimeLimitRequest struct {
	InvocationID string
}

// EmailRequest defines the structure of a request to be sent to iplant-email.
type EmailRequest struct {
	TemplateName        string                 `json:"template"`
	TemplateValues      map[string]interface{} `json:"values"`
	Subject             string                 `json:"subject"`
	ToAddress           string                 `json:"to"`
	CourtesyCopyAddress string                 `json:"cc,omitempty"`
	FromAddress         string                 `json:"from-addr,omitempty"`
	FromName            string                 `json:"from-name,omitempty"`
}

// NotificationMessage defines the structure of a notification message sent to
// the Discovery Environment UI.
type NotificationMessage struct {
	Deleted       bool                   `json:"deleted"`
	Email         bool                   `json:"email"`
	EmailTemplate string                 `json:"email_template"`
	Message       map[string]interface{} `json:"message"`
	Payload       interface{}            `json:"payload"`
	Seen          bool                   `json:"seen"`
	Subject       string                 `json:"subject"`
	Type          string                 `json:"type"`
	User          string                 `json:"user"`
}

// WrappedNotificationMessage defines a wrapper around a notification message
// sent to the Discovery Environment UI. The wrapper contains an unread message
// count in addition to the message itself.
type WrappedNotificationMessage struct {
	Total   int64                `json:"total"`
	Message *NotificationMessage `json:"message"`
}
