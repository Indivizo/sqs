package queue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// DefaultBackoff is how long Run waits after a failed receive when
// Processor.Backoff is unset.
const DefaultBackoff = 5 * time.Second

// Processor receives messages from a queue, decodes each JSON body into a
// T and hands it to Handle.
//
// A message is deleted only after Handle succeeds. When decoding fails, or
// Handle fails or panics, the message is left on the queue: SQS redelivers
// it after the visibility timeout, and after MaxReceiveCount receives moves
// it to the dead-letter queue.
type Processor[T any] struct {
	Queue  *Queue
	Handle func(ctx context.Context, msg T) error
	// Backoff is the pause after a failed receive. Zero means DefaultBackoff.
	Backoff time.Duration
}

// Run processes messages until ctx is done and then returns ctx.Err().
// Several processors may run on the same queue concurrently.
func (p *Processor[T]) Run(ctx context.Context) error {
	backoff := p.Backoff
	if backoff == 0 {
		backoff = DefaultBackoff
	}
	queueLog := log.WithField("queue_name", p.Queue.Name)
	queueLog.Info("processing queue started")

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		msg, err := p.Queue.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Without a pause, an SQS outage becomes a tight loop of
			// failing calls.
			queueLog.WithError(err).Warn("receiving message failed")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			continue
		}
		if msg == nil {
			continue
		}

		p.process(ctx, queueLog.WithField("message_id", msg.ID), msg)
	}
}

func (p *Processor[T]) process(ctx context.Context, msgLog *log.Entry, msg *Message) {
	// A fresh value per message: decoding into a reused one would leave
	// fields a message omits holding the previous message's values.
	var body T
	if err := json.Unmarshal([]byte(msg.Body), &body); err != nil {
		msgLog.WithError(err).Warn("decoding message failed; leaving it for redelivery")
		return
	}

	if err := p.handle(ctx, body); err != nil {
		msgLog.WithError(err).Warn("handling message failed; leaving it for redelivery")
		return
	}

	if err := p.Queue.Delete(ctx, msg.ReceiptHandle); err != nil {
		msgLog.WithError(err).Warn("deleting handled message failed; it will be redelivered")
	}
}

// handle turns a panic in Handle into an error, so one bad message cannot
// stop the loop.
func (p *Processor[T]) handle(ctx context.Context, body T) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("handler panicked: %v", r)
		}
	}()
	return p.Handle(ctx, body)
}
