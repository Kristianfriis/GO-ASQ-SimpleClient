package goasqsimpleclient

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/Azure/azure-storage-queue-go/azqueue"
)

type Queue struct {
	QeueUrl azqueue.QueueURL
	CTX     context.Context
}

func Setup(accountName string, accountKey string, queueName string) Queue {
	_url, err := url.Parse(fmt.Sprintf("https://%s.queue.core.windows.net/%s", accountName, queueName))
	if err != nil {
		log.Fatal("Error parsing url: ", err)
	}

	credential, err := azqueue.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal("Error creating credentials: ", err)
	}

	p := azqueue.NewPipeline(credential, azqueue.PipelineOptions{})

	queueUrl := azqueue.NewQueueURL(*_url, p)

	ctx := context.TODO()

	return Queue{QeueUrl: queueUrl, CTX: ctx}
}

func (q Queue) MessagesToProcess() int {
	props, err := q.QeueUrl.GetProperties(q.CTX)

	if err != nil {
		log.Fatal(err)
	}

	m := props.ApproximateMessagesCount()

	return int(m)
}

func (q Queue) GetMessage() (messageBytes []byte, err error) {
	msgUrl := q.QeueUrl.NewMessagesURL()

	dequeueResp, err := msgUrl.Dequeue(q.CTX, 1, 10*time.Second)

	if err != nil {
		log.Fatal("Error dequeueing message: ", err)
	}
	msgToDecode := ""

	for i := int32(0); i < dequeueResp.NumMessages(); i++ {
		msg := dequeueResp.Message(i)

		msgToDecode = msg.Text

		// msgIdUrl := msgUrl.NewMessageIDURL(msg.ID)
		// _, err = msgIdUrl.Delete(q.CTX, msg.PopReceipt)
		// if err != nil {
		// 	log.Fatal("Error deleting message: ", err)
		// }
	}
	if msgToDecode != "" {
		decodedMsg, decodeErr := base64.StdEncoding.DecodeString(msgToDecode)
		if decodeErr != nil {
			return nil, decodeErr
		} else {
			return decodedMsg, nil
		}
	}

	return nil, errors.New("could not get message from queue")
}

func (q Queue) PushMessage(source interface{}) {
	msgUrl := q.QeueUrl.NewMessagesURL()

	e, _ := json.Marshal(source)

	newMessageContent := base64.StdEncoding.EncodeToString([]byte(e))
	_, err := msgUrl.Enqueue(q.CTX, newMessageContent, 0, 0)
	if err != nil {
		log.Fatal("Error adding message to queue: ", err)
	}

	log.Print("Added message to the queue")
}
