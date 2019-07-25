package queue

import (
	"encoding/json"
	"strings"

	"github.com/aws/aws-sdk-go/service/sqs"
	log "github.com/sirupsen/logrus"
)

// UnmarshalMessageBody will return a MessageBody struct from the given sqs.Message.
func UnmarshalMessageBody(message *sqs.Message, v interface{}) (err error) {
	reader := strings.NewReader(*message.Body)
	err = json.NewDecoder(reader).Decode(v)
	if err != nil {
		log.WithFields(log.Fields{
			//"queueName":         GetQueueName(),
			"messagID":          *message.MessageId,
			"messageBodyString": *message.Body,
			"error":             err,
		}).Error("Unmarshal messageBody")
	}

	return
}

// Processor represents a method that handles incoming sqs messages.
type Processor struct {
	Queue             *Queue
	HandleMessageBody func(Processor, *interface{}) error
}

// Process handles incoming sqs messages.
// The body parameter is not typed, so we can decode the incoming message in a structure that is passed via this parameter.
// On passing nil, the Json marshaller will marshall it as map[string]interface{}.
//
// Since we are passing the containing structure, this method is not threadsafe.
// On the other hand multiple Processors can process the same sqs queues parallel without any problem.
func (processor *Processor) Process(body interface{}) {
	queueDetails := log.Fields{
		"queueName": processor.Queue.Name,
		"queueURL":  processor.Queue.URL,
	}

	log.WithFields(queueDetails).Info("Processing queue started")
	for {
		log.WithFields(queueDetails).Info("Polling queue")

		message, err := processor.Queue.ReceiveMessage()
		if err != nil || message == nil {
			continue
		}
		err = UnmarshalMessageBody(message, &body)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
				"body":  body,
			}).Warning("Error unmarshalling message")

			continue
		}
		if err = processor.HandleMessageBody(*processor, &body); err != nil {
			log.WithFields(log.Fields{
				"error":     err,
				"message":   message,
				"queueName": processor.Queue.Name,
				"queueURL":  processor.Queue.URL,
			}).Warning("Error processing message")
			continue
		}
		if _, err := processor.Queue.DeleteMessage(message); err != nil {
			log.WithFields(log.Fields{
				"message":   message,
				"queueName": processor.Queue.Name,
				"queueURL":  processor.Queue.URL,
			}).Warning("Error deleting queue message")
		}
	}
}
