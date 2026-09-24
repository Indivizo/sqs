package queue

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/pkg/errors"
)

// DeadLetterQueueSuffix is appended to a queue's name to name its
// dead-letter queue.
const DeadLetterQueueSuffix = "-deadMessages"

// maxQueueNameLength is SQS's own limit on a queue name.
const maxQueueNameLength = 80

// ErrQueueNameTooLong is returned by Ensure when the name, or its
// dead-letter sibling's name, would exceed SQS's 80-character limit.
var ErrQueueNameTooLong = errors.New("queue name too long for its dead-letter queue")

// Options configure a queue. The creation-time attributes
// (MaxReceiveCount, RetentionSeconds) must stay equal for an existing
// queue across restarts: SQS refuses CreateQueue when they differ from the
// stored ones.
type Options struct {
	// MaxReceiveCount is how many times a message is received before SQS
	// moves it to the dead-letter queue.
	MaxReceiveCount int32
	// RetentionSeconds is how long an unconsumed message is kept, on both
	// the queue and its dead-letter queue.
	RetentionSeconds int32
	// VisibilityTimeout is how long, in seconds, a received message stays
	// hidden from other receivers before it is redelivered.
	VisibilityTimeout int32
	// WaitTimeSeconds is the long-polling window of a single Receive.
	WaitTimeSeconds int32
}

// DefaultOptions returns the values v0.1.0 hardcoded. Services that
// already created their queues with v0.1.0 must keep them, or their next
// start fails.
func DefaultOptions() Options {
	return Options{
		MaxReceiveCount:   5,
		RetentionSeconds:  1209600, // 14 days, SQS's maximum
		VisibilityTimeout: 600,
		WaitTimeSeconds:   20,
	}
}

// Queue is an SQS queue that exists and is bound to a client.
type Queue struct {
	Name               string
	URL                string
	DeadLetterQueueURL string

	api  API
	opts Options
}

// redrivePolicy mirrors v0.1.0's struct, field order and JSON tags
// included, so the marshalled attribute is byte-identical to what
// existing queues store.
type redrivePolicy struct {
	MaxReceiveCount     int32  `json:"maxReceiveCount"`
	DeadLetterTargetArn string `json:"deadLetterTargetArn"`
}

// Ensure creates the named queue and its dead-letter queue, or binds to
// them when they already exist with the same attributes. CreateQueue is
// idempotent in that case, so Ensure is safe to call on every start.
//
// The dead-letter queue is created first: the main queue's redrive policy
// needs its ARN.
func Ensure(ctx context.Context, api API, name string, opts Options) (*Queue, error) {
	dlqName := name + DeadLetterQueueSuffix
	if len(dlqName) > maxQueueNameLength {
		return nil, errors.Wrapf(ErrQueueNameTooLong, "%q", dlqName)
	}

	retention := strconv.Itoa(int(opts.RetentionSeconds))

	dlqURL, err := createQueue(ctx, api, dlqName, map[string]string{
		string(types.QueueAttributeNameMessageRetentionPeriod): retention,
	})
	if err != nil {
		return nil, err
	}

	attrs, err := api.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(dlqURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return nil, errors.Wrapf(err, "get arn of queue %q", dlqName)
	}

	policy, err := json.Marshal(redrivePolicy{
		MaxReceiveCount:     opts.MaxReceiveCount,
		DeadLetterTargetArn: attrs.Attributes[string(types.QueueAttributeNameQueueArn)],
	})
	if err != nil {
		return nil, errors.Wrap(err, "marshal redrive policy")
	}

	url, err := createQueue(ctx, api, name, map[string]string{
		string(types.QueueAttributeNameMessageRetentionPeriod): retention,
		string(types.QueueAttributeNameRedrivePolicy):          string(policy),
	})
	if err != nil {
		return nil, err
	}

	return &Queue{Name: name, URL: url, DeadLetterQueueURL: dlqURL, api: api, opts: opts}, nil
}

func createQueue(ctx context.Context, api API, name string, attributes map[string]string) (string, error) {
	out, err := api.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String(name),
		Attributes: attributes,
	})
	if err != nil {
		return "", errors.Wrapf(err, "create queue %q", name)
	}
	return aws.ToString(out.QueueUrl), nil
}
