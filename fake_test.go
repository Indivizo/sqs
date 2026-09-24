package queue

import (
	"context"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
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

	sent    []*sqs.SendMessageInput
	sendErr error

	receives         []*sqs.ReceiveMessageInput
	receiveResponses [][]types.Message // one slice per ReceiveMessage call; empty once exhausted
	receiveErr       error

	deleted   []*sqs.DeleteMessageInput
	deleteErr error
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

func (f *fakeAPI) SendMessage(_ context.Context, params *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.calls = append(f.calls, "SendMessage")
	f.sent = append(f.sent, params)
	if f.sendErr != nil {
		return nil, f.sendErr
	}
	return &sqs.SendMessageOutput{MessageId: aws.String("message-" + strconv.Itoa(len(f.sent)))}, nil
}

func (f *fakeAPI) ReceiveMessage(_ context.Context, params *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.calls = append(f.calls, "ReceiveMessage")
	f.receives = append(f.receives, params)
	if f.receiveErr != nil {
		return nil, f.receiveErr
	}
	var msgs []types.Message
	if i := len(f.receives) - 1; i < len(f.receiveResponses) {
		msgs = f.receiveResponses[i]
	}
	return &sqs.ReceiveMessageOutput{Messages: msgs}, nil
}

func (f *fakeAPI) DeleteMessage(_ context.Context, params *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.calls = append(f.calls, "DeleteMessage")
	f.deleted = append(f.deleted, params)
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	return &sqs.DeleteMessageOutput{}, nil
}

func fakeQueueURL(name string) string {
	return "https://sqs.eu-central-1.amazonaws.com/000000000000/" + name
}
