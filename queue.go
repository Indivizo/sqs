package queue

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/pkg/errors"
)

// Message is one received SQS message.
type Message struct {
	ID            string
	Body          string
	ReceiptHandle string // identifies this delivery for Delete
}

// Send puts body on the queue and returns SQS's message id.
func (q *Queue) Send(ctx context.Context, body string) (string, error) {
	out, err := q.api.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(q.URL),
		MessageBody: aws.String(body),
	})
	if err != nil {
		return "", errors.Wrapf(err, "send message to queue %q", q.Name)
	}
	return aws.ToString(out.MessageId), nil
}

// SendJSON marshals v and sends it as the message body.
func (q *Queue) SendJSON(ctx context.Context, v any) (string, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return "", errors.Wrapf(err, "marshal message for queue %q", q.Name)
	}
	return q.Send(ctx, string(body))
}

// Receive makes a single long poll and returns at most one message. A nil
// message with a nil error means the poll elapsed with nothing queued,
// which is the common case rather than a failure; polling again is the
// caller's decision.
func (q *Queue) Receive(ctx context.Context) (*Message, error) {
	out, err := q.api.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(q.URL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     q.opts.WaitTimeSeconds,
		VisibilityTimeout:   q.opts.VisibilityTimeout,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "receive message from queue %q", q.Name)
	}
	if len(out.Messages) == 0 {
		return nil, nil
	}
	m := out.Messages[0]
	return &Message{
		ID:            aws.ToString(m.MessageId),
		Body:          aws.ToString(m.Body),
		ReceiptHandle: aws.ToString(m.ReceiptHandle),
	}, nil
}

// Delete acknowledges a message so SQS does not redeliver it once its
// visibility timeout lapses.
func (q *Queue) Delete(ctx context.Context, receiptHandle string) error {
	_, err := q.api.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.URL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	if err != nil {
		return errors.Wrapf(err, "delete message from queue %q", q.Name)
	}
	return nil
}
