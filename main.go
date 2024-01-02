package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	gohttpclient "github.com/bozd4g/go-http-client"
	"github.com/jszwec/csvutil"
)

type Message struct {
	ID    int    `csv:"id"`
	State string `csv:"state"`
}

type OSState struct {
	State string `json:"state"`
}

type RejectStateBody struct {
	Action string              `json:"action"`
	Data   RejectStateBodyData `json:"data"`
}

type ConfirmStateBody struct {
	Action string               `json:"action"`
	Data   ConfirmStateBodyData `json:"data"`
}

type ConfirmStateBodyData struct {
	State             string    `json:"state"`
	DeliveryTimeID    int       `json:"deliveryTimeId"`
	UserID            int       `json:"userId"`
	ReceptionSystemID int       `json:"receptionSystemId"`
	ResponseDate      time.Time `json:"responseDate"`
}

type RejectStateBodyData struct {
	State           string `json:"state"`
	RejectMessageID int    `json:"rejectMessageId"`
	RejectNotes     string `json:"rejectNotes"`
}

const (
	PendingState  = "PENDING"
	ConfirmState  = "CONFIRMED"
	RejectedState = "REJECTED"
)

func main() {
	args := os.Args

	messages := make([]Message, 0)
	err := readCsvFile(fmt.Sprintf("${file_path}/order_%s.csv", args[1]), &messages)
	if err != nil {
		log.Fatalf("error: %v", err)
		return
	}

	ctx := context.Background()
	client := gohttpclient.New("http://localhost:11035/")

	for _, msg := range messages {
		err := processMessage(ctx, client, msg)
		if err != nil {
			log.Printf(err.Error())
			return
		}
	}
}

func readCsvFile(filePath string, v interface{}) error {
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Unable to read input file "+filePath, err)
	}

	csvReader := csv.NewReader(f)

	dec, err := csvutil.NewDecoder(csvReader)
	if err != nil {
		return err
	}

	return dec.Decode(v)
}

func processMessage(ctx context.Context, client *gohttpclient.Client, msg Message) error {
	getResponse, err := client.Get(ctx, fmt.Sprintf("v1/orders/%v/state", msg.ID))
	if err != nil {
		log.Printf("error: %v", err)
		return err
	}

	var orderState OSState
	err = getResponse.Unmarshal(&orderState)
	if err != nil {
		log.Printf("error: %v", err)
		return err
	}

	today, _ := time.Parse(time.RFC3339, "2024-01-02T17:20:00Z")

	var bodyStr []byte
	if orderState.State == PendingState {
		if msg.State == ConfirmState {
			body := ConfirmStateBody{
				Action: "responded",
				Data: ConfirmStateBodyData{
					State:             ConfirmState,
					DeliveryTimeID:    2,
					UserID:            51903773,
					ReceptionSystemID: 65,
					ResponseDate:      today,
				},
			}
			bodyStr, _ = json.Marshal(body)
			log.Printf("[original_state:%s][state:%s][order_id:%v]", orderState.State, body.Data.State, msg.ID)
		} else if msg.State == RejectedState {
			body := RejectStateBody{
				Action: "responded",
				Data: RejectStateBodyData{
					State:           RejectedState,
					RejectMessageID: 12,
					RejectNotes:     fmt.Sprintf("Order %v has been rejected due to failures in its processing", msg.ID),
				},
			}
			bodyStr, _ = json.Marshal(body)
			log.Printf("[original_state:%s][state:%s][order_id:%v]", msg.State, body.Data.State, msg.ID)
		} else {
			return errors.New(fmt.Sprintf("no confirm ni rejected: %s", msg.State))
		}

		putResponse, err := client.Put(ctx, fmt.Sprintf("v1/orders/%v/state", msg.ID),
			gohttpclient.WithBody(bodyStr),
			gohttpclient.WithHeader("Authorization", "X"),
			gohttpclient.WithHeader("Origin", "postman"))
		if err != nil {
			log.Printf("error: %v", err)
			return err
		}

		if !putResponse.Ok() {
			responseBody := string(putResponse.Body()[:])
			log.Printf("error: %v", responseBody)
			return err
		}
	}

	return nil
}
