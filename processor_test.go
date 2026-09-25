package queue

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testMessage struct {
	ID   string `json:"id"`
	Note string `json:"note"`
}

func sqsMessage(n int, body string) types.Message {
	return types.Message{
		MessageId:     aws.String("id-" + strconv.Itoa(n)),
		Body:          aws.String(body),
		ReceiptHandle: aws.String("receipt-" + strconv.Itoa(n)),
	}
}

// runProcessor runs p until it returns, failing the test if that takes
// longer than timeout.
func runProcessor(ctx context.Context, t *testing.T, p *Processor[testMessage], timeout time.Duration) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		t.Fatal("processor did not stop")
		return nil
	}
}

func deletedReceipts(api *fakeAPI) []string {
	var receipts []string
	for _, in := range api.deleted {
		receipts = append(receipts, aws.ToString(in.ReceiptHandle))
	}
	return receipts
}

func TestProcessor_HandsEachMessageToHandlerAndDeletesIt(t *testing.T) {
	// given two queued messages
	api := newFakeAPI()
	q := ensuredQueue(t, api, DefaultOptions())
	api.receiveResponses = [][]types.Message{
		{sqsMessage(1, `{"id":"a"}`)},
		{sqsMessage(2, `{"id":"b"}`)},
	}
	ctx, cancel := context.WithCancel(context.Background())
	var handled []testMessage
	p := &Processor[testMessage]{Queue: q, Handle: func(_ context.Context, m testMessage) error {
		handled = append(handled, m)
		if len(handled) == 2 {
			cancel()
		}
		return nil
	}}

	// when
	err := runProcessor(ctx, t, p, time.Second)

	// then
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, []testMessage{{ID: "a"}, {ID: "b"}}, handled)
	assert.Equal(t, []string{"receipt-1", "receipt-2"}, deletedReceipts(api))
}

// v0.1.0 decoded every message into one shared value, and the JSON
// decoder leaves absent fields untouched, so a message silently inherited
// the previous message's fields.
func TestProcessor_DecodesEachMessageIntoAFreshValue(t *testing.T) {
	// given a second message that omits a field the first one set
	api := newFakeAPI()
	q := ensuredQueue(t, api, DefaultOptions())
	api.receiveResponses = [][]types.Message{
		{sqsMessage(1, `{"id":"a","note":"x"}`)},
		{sqsMessage(2, `{"id":"b"}`)},
	}
	ctx, cancel := context.WithCancel(context.Background())
	var handled []testMessage
	p := &Processor[testMessage]{Queue: q, Handle: func(_ context.Context, m testMessage) error {
		handled = append(handled, m)
		if len(handled) == 2 {
			cancel()
		}
		return nil
	}}

	// when
	_ = runProcessor(ctx, t, p, time.Second)

	// then the second message carries no trace of the first
	require.Len(t, handled, 2)
	assert.Equal(t, testMessage{ID: "b"}, handled[1])
}

// v0.1.0 retried a failed receive immediately, so an SQS outage turned
// into a tight loop of failing calls.
func TestProcessor_BacksOffAfterReceiveError(t *testing.T) {
	// given every receive fails
	api := newFakeAPI()
	q := ensuredQueue(t, api, DefaultOptions())
	api.receiveErr = errors.New("connection reset")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	p := &Processor[testMessage]{
		Queue:   q,
		Backoff: 40 * time.Millisecond,
		Handle:  func(context.Context, testMessage) error { return nil },
	}

	// when
	err := runProcessor(ctx, t, p, time.Second)

	// then it retried at the backoff pace: at 0, 40 and 80 ms
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.GreaterOrEqual(t, len(api.receives), 2)
	assert.LessOrEqual(t, len(api.receives), 4)
}

func TestProcessor_LeavesMessageWhenHandlerFails(t *testing.T) {
	// given
	api := newFakeAPI()
	q := ensuredQueue(t, api, DefaultOptions())
	api.receiveResponses = [][]types.Message{{sqsMessage(1, `{"id":"a"}`)}}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Processor[testMessage]{Queue: q, Handle: func(context.Context, testMessage) error {
		cancel()
		return errors.New("firefly unavailable")
	}}

	// when
	_ = runProcessor(ctx, t, p, time.Second)

	// then SQS redelivers it once the visibility timeout lapses
	assert.Empty(t, api.deleted)
}

func TestProcessor_RecoversFromHandlerPanicAndKeepsProcessing(t *testing.T) {
	// given the first message panics its handler
	api := newFakeAPI()
	q := ensuredQueue(t, api, DefaultOptions())
	api.receiveResponses = [][]types.Message{
		{sqsMessage(1, `{"id":"boom"}`)},
		{sqsMessage(2, `{"id":"b"}`)},
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Processor[testMessage]{Queue: q, Handle: func(_ context.Context, m testMessage) error {
		if m.ID == "boom" {
			panic("nil map")
		}
		cancel()
		return nil
	}}

	// when
	err := runProcessor(ctx, t, p, time.Second)

	// then the panicking message stays queued and the next one is handled
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, []string{"receipt-2"}, deletedReceipts(api))
}

func TestProcessor_LeavesUndecodableMessageForRedelivery(t *testing.T) {
	// given a body that is not JSON
	api := newFakeAPI()
	q := ensuredQueue(t, api, DefaultOptions())
	api.receiveResponses = [][]types.Message{{sqsMessage(1, `not json`)}}
	ctx, cancel := context.WithCancel(context.Background())
	api.onReceive = func(call int) {
		if call == 2 {
			cancel()
		}
	}
	handled := 0
	p := &Processor[testMessage]{Queue: q, Handle: func(context.Context, testMessage) error {
		handled++
		return nil
	}}

	// when
	_ = runProcessor(ctx, t, p, time.Second)

	// then it reaches the dead-letter queue after MaxReceiveCount receives
	assert.Zero(t, handled)
	assert.Empty(t, api.deleted)
}

func TestProcessor_StopsPromptlyWhenCancelledDuringBackoff(t *testing.T) {
	// given a receive failure followed by a long backoff
	api := newFakeAPI()
	q := ensuredQueue(t, api, DefaultOptions())
	api.receiveErr = errors.New("connection reset")
	ctx, cancel := context.WithCancel(context.Background())
	api.onReceive = func(int) { cancel() }
	p := &Processor[testMessage]{
		Queue:   q,
		Backoff: time.Hour,
		Handle:  func(context.Context, testMessage) error { return nil },
	}

	// when
	err := runProcessor(ctx, t, p, time.Second)

	// then
	assert.ErrorIs(t, err, context.Canceled)
}
