package queue

import (
	"encoding/json"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// Frankfurt region.
const sqsRegion = "eu-central-1"

// Default suffix for dead letter queue.
const deadLetterQueueSuffix = "-deadMessages"

// MaxReceiveCountBeforeDead is the receive count before a message is sent to a dead letter queue.
const MaxReceiveCountBeforeDead = 5

// A Queue represents an SQS queue.
type Queue struct {
	Name               string
	URL                string
	DeadLetterQueueURL string
}

// A RedrivePolicy is an sqs policy of a dead letter queue.
type RedrivePolicy struct {
	MaxReceiveCount     int    `json:"maxReceiveCount"`
	DeadLetterTargetArn string `json:"deadLetterTargetArn"`
}

// New returns a prepared SQS queue.
func New(name string) (*Queue, error) {
	queue := Queue{Name: name}
	err := queue.Init()

	return &queue, err
}

// Init will create the actual queue and set a Client with a live session to it.
func (queue *Queue) Init() (err error) {
	client := queue.GetClient()

	params := &sqs.CreateQueueInput{
		QueueName: aws.String(queue.Name + deadLetterQueueSuffix),
		Attributes: map[string]*string{
			"MessageRetentionPeriod": aws.String("1209600"),
		},
	}
	resp, err := client.CreateQueue(params)
	if err != nil {
		log.WithFields(log.Fields{
			"queueName": queue.Name,
			"error":     err,
		}).Error("Createing the dead letter queue")
		return
	}

	queue.DeadLetterQueueURL = *resp.QueueUrl
	log.WithFields(log.Fields{
		"QueueUrl": queue.DeadLetterQueueURL,
	}).Info("Dead Letter Queue initialized")

	queueArnAttributeName := "QueueArn"
	deadLetterQueueAttributes, err := queue.GetAttributesByQueueURL(queue.DeadLetterQueueURL, []*string{&queueArnAttributeName})
	if err != nil {
		return
	}
	redrivePolicy := &RedrivePolicy{
		MaxReceiveCount:     MaxReceiveCountBeforeDead,
		DeadLetterTargetArn: *deadLetterQueueAttributes.Attributes[queueArnAttributeName],
	}
	redrivePolicyString, err := redrivePolicy.GetAsAWSString()
	if err != nil {
		return
	}
	params = &sqs.CreateQueueInput{
		QueueName: aws.String(queue.Name),
		Attributes: map[string]*string{
			"RedrivePolicy":          redrivePolicyString,
			"MessageRetentionPeriod": aws.String("1209600"),
		},
	}
	resp, err = client.CreateQueue(params)
	if err != nil {
		log.WithFields(log.Fields{
			"queueName": queue.Name,
			"error":     err,
		}).Error("Createing the queue")
		return
	}

	queue.URL = *resp.QueueUrl
	log.WithFields(log.Fields{
		"QueueUrl": queue.URL,
	}).Info("Queue initialized")

	return
}

// GetClient returns an SQS client with a live session.
func (queue *Queue) GetClient() *sqs.SQS {
	config := &aws.Config{
		Region: aws.String(sqsRegion),
	}
	return sqs.New(session.New(config))
}

// SendMessage will send message to the queue with the file path.
func (queue *Queue) SendMessage(messageBody interface{}) (resp *sqs.SendMessageOutput, err error) {
	msg, err := json.Marshal(messageBody)
	if err != nil {
		log.WithFields(log.Fields{
			"queueName":   queue.Name,
			"error":       err,
			"messageBody": messageBody,
		}).Error("Marshal the message body for the queue")
		return
	}
	client := queue.GetClient()
	params := &sqs.SendMessageInput{
		MessageBody: aws.String(string(msg)),
		QueueUrl:    aws.String(queue.URL),
	}
	resp, err = client.SendMessage(params)

	if err != nil {
		log.WithFields(log.Fields{
			"queueName": queue.Name,
			"error":     err,
		}).Error("Sending message to queue")
		return
	}

	log.WithFields(log.Fields{
		"messageID": resp.MessageId,
	}).Info("Message sent to the queue")

	return
}

// ReceiveMessage will return one message and it's body from the queue.
func (queue *Queue) ReceiveMessage() (message *sqs.Message, err error) {
	client := queue.GetClient()
	params := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queue.URL),
		MaxNumberOfMessages: aws.Int64(1),
		VisibilityTimeout:   aws.Int64(600),
		WaitTimeSeconds:     aws.Int64(20),
	}

	resp, err := client.ReceiveMessage(params)
	if err != nil {
		log.WithFields(log.Fields{
			"queueName": queue.Name,
			"error":     err,
		}).Error("Receiving message from queue")
		return
	}

	if len(resp.Messages) < 1 {
		return
	}

	message = resp.Messages[0]

	return
}

// DeleteMessage removes a message from the Queue.
func (queue *Queue) DeleteMessage(message *sqs.Message) (resp *sqs.DeleteMessageOutput, err error) {
	resp, err = queue.DeleteMessageByReceiptHandle(message.ReceiptHandle)
	if err != nil {
		log.WithFields(log.Fields{
			"queueName": queue.Name,
			"messageID": message.MessageId,
			"error":     err,
		}).Error("Deleting message from queue")
		return
	}

	log.WithFields(log.Fields{
		"queueName": queue.Name,
		"messageID": message.MessageId,
	}).Info("Message deleted from queue")

	return
}

// DeleteMessageByReceiptHandle removes a message from the Queue by it's receiptHandle.
func (queue *Queue) DeleteMessageByReceiptHandle(receiptHandle *string) (resp *sqs.DeleteMessageOutput, err error) {
	client := queue.GetClient()
	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queue.URL),
		ReceiptHandle: aws.String(*receiptHandle),
	}
	resp, err = client.DeleteMessage(params)

	return
}

// GetAttributesByQueueURL returns queue attributes by it's URL.
func (queue *Queue) GetAttributesByQueueURL(url string, attributeNames []*string) (resp *sqs.GetQueueAttributesOutput, err error) {
	client := queue.GetClient()
	params := &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(url),
		AttributeNames: attributeNames,
	}
	resp, err = client.GetQueueAttributes(params)

	if err != nil {
		log.WithFields(log.Fields{
			"queueName": queue.Name,
			"queueUrl":  url,
			"error":     err,
		}).Error("Getting queue attributes")
		return
	}

	return
}

// GetAsAWSString returns the RedrivePolicy as a JSON string poninter for sqs attribute.
func (policy RedrivePolicy) GetAsAWSString() (policyString *string, err error) {
	jsonBytes, err := json.Marshal(policy)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("Marshal the RedrivePolicy")
		return
	}

	policyString = aws.String(string(jsonBytes))
	return
}
