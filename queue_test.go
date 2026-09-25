package queue

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ensuredQueue returns a Queue built the way callers build one, through
// Ensure, so these tests exercise the public path.
func ensuredQueue(t *testing.T, api *fakeAPI, opts Options) *Queue {
	t.Helper()
	q, err := Ensure(context.Background(), api, "test", opts)
	require.NoError(t, err)
	return q
}

func TestQueue_Send_SendsBodyToQueueURLAndReturnsMessageID(t *testing.T) {
	// given
	api := newFakeAPI()
	q := ensuredQueue(t, api, DefaultOptions())

	// when
	id, err := q.Send(context.Background(), "job-1")

	// then
	require.NoError(t, err)
	assert.Equal(t, "message-1", id)
	require.Len(t, api.sent, 1)
	assert.Equal(t, fakeQueueURL("test"), aws.ToString(api.sent[0].QueueUrl))
	assert.Equal(t, "job-1", aws.ToString(api.sent[0].MessageBody))
}

func TestQueue_Send_ReturnsSendError(t *testing.T) {
	// given
	api := newFakeAPI()
	q := ensuredQueue(t, api, DefaultOptions())
	api.sendErr = errors.New("throttled")

	// when
	id, err := q.Send(context.Background(), "job-1")

	// then
	assert.ErrorIs(t, err, api.sendErr)
	assert.Empty(t, id)
}

func TestQueue_SendJSON_SendsMarshalledValue(t *testing.T) {
	// given
	api := newFakeAPI()
	q := ensuredQueue(t, api, DefaultOptions())
	msg := struct {
		RecordingID string `json:"recordingID"`
		Item        int    `json:"item"`
	}{RecordingID: "abc", Item: 2}

	// when
	_, err := q.SendJSON(context.Background(), msg)

	// then
	require.NoError(t, err)
	require.Len(t, api.sent, 1)
	assert.JSONEq(t, `{"recordingID":"abc","item":2}`, aws.ToString(api.sent[0].MessageBody))
}

func TestQueue_SendJSON_DoesNotSendAValueThatCannotBeMarshalled(t *testing.T) {
	// given
	api := newFakeAPI()
	q := ensuredQueue(t, api, DefaultOptions())

	// when
	_, err := q.SendJSON(context.Background(), make(chan int))

	// then
	assert.Error(t, err)
	assert.Empty(t, api.sent)
}

func TestQueue_Receive_ReturnsFirstMessage(t *testing.T) {
	// given
	api := newFakeAPI()
	q := ensuredQueue(t, api, DefaultOptions())
	api.receiveResponses = [][]types.Message{{{
		MessageId:     aws.String("id-1"),
		Body:          aws.String("job-1"),
		ReceiptHandle: aws.String("receipt-1"),
	}}}

	// when
	msg, err := q.Receive(context.Background())

	// then
	require.NoError(t, err)
	assert.Equal(t, &Message{ID: "id-1", Body: "job-1", ReceiptHandle: "receipt-1"}, msg)
}

func TestQueue_Receive_ReturnsNilWhenThePollIsEmpty(t *testing.T) {
	// given a long poll that elapses with nothing queued
	api := newFakeAPI()
	q := ensuredQueue(t, api, DefaultOptions())

	// when
	msg, err := q.Receive(context.Background())

	// then it is not an error, and only one poll was made
	require.NoError(t, err)
	assert.Nil(t, msg)
	assert.Len(t, api.receives, 1)
}

func TestQueue_Receive_PollsWithTheQueueOptions(t *testing.T) {
	for name, opts := range map[string]Options{
		"defaults": DefaultOptions(),
		"custom":   {MaxReceiveCount: 5, RetentionSeconds: 1209600, VisibilityTimeout: 30, WaitTimeSeconds: 5},
	} {
		t.Run(name, func(t *testing.T) {
			// given
			api := newFakeAPI()
			q := ensuredQueue(t, api, opts)

			// when
			_, err := q.Receive(context.Background())

			// then
			require.NoError(t, err)
			require.Len(t, api.receives, 1)
			in := api.receives[0]
			assert.Equal(t, fakeQueueURL("test"), aws.ToString(in.QueueUrl))
			assert.Equal(t, int32(1), in.MaxNumberOfMessages)
			assert.Equal(t, opts.WaitTimeSeconds, in.WaitTimeSeconds)
			assert.Equal(t, opts.VisibilityTimeout, in.VisibilityTimeout)
		})
	}
}

func TestQueue_Receive_ReturnsReceiveError(t *testing.T) {
	// given
	api := newFakeAPI()
	q := ensuredQueue(t, api, DefaultOptions())
	api.receiveErr = errors.New("connection reset")

	// when
	msg, err := q.Receive(context.Background())

	// then
	assert.ErrorIs(t, err, api.receiveErr)
	assert.Nil(t, msg)
}

func TestQueue_Delete_SendsReceiptHandleToQueueURL(t *testing.T) {
	// given
	api := newFakeAPI()
	q := ensuredQueue(t, api, DefaultOptions())

	// when
	err := q.Delete(context.Background(), "receipt-1")

	// then
	require.NoError(t, err)
	require.Len(t, api.deleted, 1)
	assert.Equal(t, fakeQueueURL("test"), aws.ToString(api.deleted[0].QueueUrl))
	assert.Equal(t, "receipt-1", aws.ToString(api.deleted[0].ReceiptHandle))
}

func TestQueue_Delete_ReturnsDeleteError(t *testing.T) {
	// given
	api := newFakeAPI()
	q := ensuredQueue(t, api, DefaultOptions())
	api.deleteErr = errors.New("receipt expired")

	// when
	err := q.Delete(context.Background(), "receipt-1")

	// then
	assert.ErrorIs(t, err, api.deleteErr)
}
