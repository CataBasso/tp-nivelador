package protocol

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/bet"
)

const (
	MsgBet     byte = 1
	MsgDone    byte = 2
	MsgAck     byte = 3
	MsgWinners byte = 4
)

const coma = ","
const newLine = "\n"

type WinnerRecord struct {
	FirstName string
	LastName  string
	Document  string
	Birthdate string
	Number    string
}

func EncodeBet(b bet.Bet) []byte {
	fields := strings.Join([]string{
		strconv.Itoa(b.AgencyId),
		b.FirstName,
		b.LastName,
		strconv.Itoa(b.Document),
		b.Birthdate,
		strconv.Itoa(b.Number),
	}, coma)

	message := make([]byte, 1+len(fields))
	message[0] = MsgBet
	copy(message[1:], fields)
	return message
}

func EncodeDone() []byte {
	return []byte{MsgDone}
}

func DecodeMessageType(raw []byte) (byte, error) {
	if len(raw) < 1 {
		return 0, fmt.Errorf("empty message")
	}
	return raw[0], nil
}

func DecodeWinners(raw []byte) ([]WinnerRecord, error) {
	if len(raw) < 1 {
		return nil, fmt.Errorf("empty message")
	}

	payload := string(raw[1:])
	if payload == "" {
		return []WinnerRecord{}, nil
	}

	records := strings.Split(payload, newLine)
	winners := make([]WinnerRecord, 0, len(records))
	for _, record := range records {
		fields := strings.Split(record, coma)
		if len(fields) != 5 {
			return nil, fmt.Errorf("malformed winner record: %q", record)
		}
		winners = append(winners, WinnerRecord{
			FirstName: fields[0],
			LastName:  fields[1],
			Document:  fields[2],
			Birthdate: fields[3],
			Number:    fields[4],
		})
	}
	return winners, nil
}