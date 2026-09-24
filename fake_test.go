package queue

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// fakeAPI is a hand-written API. It records every call in order and
// answers from scripted responses, so a test can assert both what was
// sent to SQS and in which sequence.
type fakeAPI struct {
	calls []string

	createdQueues []*sqs.CreateQueueInput
	createErrs    map[string]error // keyed by queue name

	attributeRequests []*sqs.GetQueueAttributesInput
	queueArn          string
	attributesErr     error
}

var _ API = (*fakeAPI)(nil)

func newFakeAPI() *fakeAPI {
	return &fakeAPI{
		createErrs: map[string]error{},
		queueArn:   "arn:aws:sqs:eu-central-1:000000000000:test-deadMessages",
	}
}

func (f *fakeAPI) CreateQueue(_ context.Context, params *sqs.CreateQueueInput, _ ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error) {
	name := aws.ToString(params.QueueName)
	f.calls = append(f.calls, "CreateQueue:"+name)
	f.createdQueues = append(f.createdQueues, params)
	if err := f.createErrs[name]; err != nil {
		return nil, err
	}
	return &sqs.CreateQueueOutput{QueueUrl: aws.String(fakeQueueURL(name))}, nil
}

func (f *fakeAPI) GetQueueAttributes(_ context.Context, params *sqs.GetQueueAttributesInput, _ ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	f.calls = append(f.calls, "GetQueueAttributes:"+aws.ToString(params.QueueUrl))
	f.attributeRequests = append(f.attributeRequests, params)
	if f.attributesErr != nil {
		return nil, f.attributesErr
	}
	return &sqs.GetQueueAttributesOutput{Attributes: map[string]string{"QueueArn": f.queueArn}}, nil
}

func (f *fakeAPI) SendMessage(_ context.Context, _ *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.calls = append(f.calls, "SendMessage")
	return &sqs.SendMessageOutput{}, nil
}

func (f *fakeAPI) ReceiveMessage(_ context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.calls = append(f.calls, "ReceiveMessage")
	return &sqs.ReceiveMessageOutput{}, nil
}

func (f *fakeAPI) DeleteMessage(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.calls = append(f.calls, "DeleteMessage")
	return &sqs.DeleteMessageOutput{}, nil
}

func fakeQueueURL(name string) string {
	return "https://sqs.eu-central-1.amazonaws.com/000000000000/" + name
}
