// Package queue is a helper for Amazon SQS queues that a service creates
// for itself at startup, each paired with a dead-letter queue.
package queue

import (
	"context"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/pkg/errors"
)

// DefaultRegion is the region every Indivizo queue lives in.
const DefaultRegion = "eu-central-1"

// API is the subset of *sqs.Client this package calls. Depending on it
// rather than on the client lets callers fake SQS in their own tests.
type API interface {
	CreateQueue(ctx context.Context, params *sqs.CreateQueueInput, optFns ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error)
	GetQueueAttributes(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

var _ API = (*sqs.Client)(nil)

// NewClient returns an SQS client whose credentials come from the SDK's
// default chain (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, shared config,
// instance role). An empty region means DefaultRegion.
func NewClient(ctx context.Context, region string) (*sqs.Client, error) {
	if region == "" {
		region = DefaultRegion
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, errors.Wrap(err, "load aws config")
	}
	return sqs.NewFromConfig(cfg), nil
}
