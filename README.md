# A helper package for Amazon SQS

## How to use

### Setup
You need to set up `AWS_SECRET_ACCESS_KEY` and `AWS_ACCESS_KEY_ID` environment variables.

### Create a new queue
```
func main() {
        // ...
  
	if _, err := NewQueue(); err != nil {
		// Do something with the error...
	}
  
        // ...
}

func NewQueue() (sqs.Processor, error) {
	pq, err := queue.New("your-queue-name"))
	if err != nil {
		return queue.Processor{}, err
	}

	processor := sqs.Processor{
		Queue:             pq,
		HandleMessageBody: handleMessageBody,
	}

	message := new(yourQueueMessage)
	go processor.Process(message)

	return processor, nil
}
```

### Add a new message to the queue
```
func addMessageToQueue() error {
        queue := &sqs.Queue{
		Name: "your-queue-name",
	}
	if err := queue.Init(); err != nil {
		return err
	}

	message := yourQueueMessage{
		Foo:        "baz",
	}
	if _, err := queue.SendMessage(message); err != nil {
		return err
	}
  
        return nil
}
```

### Handle messages
```
func handleMessageBody(processor sqs.Processor, b *interface{}) (err error) {
	message := (*b).(*yourQueueMessage)

	// Do somethong with the message...

	return
}
```
