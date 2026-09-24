# A helper package for Amazon SQS

Services create their own queues at startup. `Ensure` creates a queue together
with a dead-letter queue (`<name>-deadMessages`) and links them with a redrive
policy, or binds to both when they already exist. `Processor` receives, decodes
and acknowledges messages.

Built on [aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2). Requires Go 1.24+.

```
go get github.com/Indivizo/sqs@v0.2.0
```

## Setup

Credentials come from the AWS SDK's default chain: `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`, a shared config file, or an instance role. The region
defaults to `eu-central-1`.

The credentials need `sqs:CreateQueue`, `sqs:GetQueueAttributes`,
`sqs:SendMessage`, `sqs:ReceiveMessage` and `sqs:DeleteMessage` on both queues.

## Create a queue

```go
import queue "github.com/Indivizo/sqs"

client, err := queue.NewClient(ctx, "") // "" means eu-central-1
if err != nil {
	return err
}
q, err := queue.Ensure(ctx, client, "my-service-jobs", queue.DefaultOptions())
if err != nil {
	return err // e.g. missing permissions; a service usually stops here
}
```

`Ensure` is safe to call on every start. SQS refuses `CreateQueue` for an
existing queue whose attributes differ from the requested ones, so keep the
`Options` a queue was created with.

The dead-letter queue's name must fit SQS's 80-character limit, which leaves 67
characters for the queue's own name. A longer name returns
`ErrQueueNameTooLong` before anything is sent to AWS.

## Send a message

```go
id, err := q.Send(ctx, "raw body")
id, err = q.SendJSON(ctx, JobMessage{JobID: "42"})
```

## Process messages

```go
p := &queue.Processor[JobMessage]{
	Queue: q,
	Handle: func(ctx context.Context, msg JobMessage) error {
		return process(ctx, msg)
	},
}
go func() {
	_ = p.Run(ctx) // returns ctx.Err() once ctx is cancelled
}()
```

Each message body is decoded from JSON into a new `JobMessage`.

| Outcome | Message |
|---|---|
| `Handle` returns `nil` | deleted |
| `Handle` returns an error | left on the queue |
| `Handle` panics | recovered and logged; left on the queue |
| the body is not valid JSON for `T` | left on the queue |
| receiving fails | the processor waits `Backoff` (default 5s) and retries |

A message left on the queue is redelivered once its visibility timeout (600s)
lapses. After `MaxReceiveCount` (5) receives, SQS moves it to the dead-letter
queue, which keeps it for 14 days.

To drive the loop yourself instead, use `q.Receive`, which makes one long poll
and returns a nil message when nothing is queued, then `q.Delete`.

## Testing

`Ensure` takes the `API` interface, which `*sqs.Client` satisfies. A
hand-written fake of `API` lets a service test its queue code without AWS.

## Migrating from v0.1.0

v0.2.0 is a breaking change on the same module path. Services stay on v0.1.0
until they upgrade.

| v0.1.0 | v0.2.0 |
|---|---|
| `queue.New(name)` | `queue.Ensure(ctx, client, name, queue.DefaultOptions())` |
| `(&queue.Queue{Name: name}).Init()` | `queue.Ensure(...)` |
| `q.SendMessage(v)` | `q.SendJSON(ctx, v)` |
| `q.ReceiveMessage()` | `q.Receive(ctx)` |
| `q.DeleteMessage(msg)` | `q.Delete(ctx, msg.ReceiptHandle)` |
| `queue.Processor{Queue: q, HandleMessageBody: fn}` and `go p.Process(&body)` | `queue.Processor[T]{Queue: q, Handle: fn}` and `go p.Run(ctx)` |
| `func(p Processor, b *interface{}) error` with a type assertion | `func(ctx context.Context, msg T) error` |

- **Keep `DefaultOptions()`** for queues created by v0.1.0. Its values are
  v0.1.0's hardcoded ones, and any other value makes `CreateQueue` fail against
  the existing queue.
- Queue and dead-letter queue names are unchanged.
- v0.1.0 decoded every message into one shared value, so fields a message
  omitted kept the previous message's values. Each message is now decoded into a
  new value. Check handlers that relied on the old behaviour.
- A failed receive now waits before retrying, instead of retrying at once.
- The module requires Go 1.24 and aws-sdk-go-v2. v0.1.0 used aws-sdk-go v1.
