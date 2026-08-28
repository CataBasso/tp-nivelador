package protocol

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/bet"
)

const (
	MsgBet        byte = 1
	MsgDone       byte = 2
	MsgAck        byte = 3
	MsgWinners    byte = 4
	MsgBatchError byte = 5
)

const (
	comma   = ","
	newLine = "\n"
)

type WinnerRecord struct {
	FirstName string
	LastName  string
	Document  string
	Birthdate string
	Number    string
}

func encodeBetRecord(bet bet.Bet) string {
	return strings.Join([]string{
		strconv.Itoa(bet.AgencyId),
		bet.FirstName,
		bet.LastName,
		strconv.Itoa(bet.Document),
		bet.Birthdate,
		strconv.Itoa(bet.Number),
	}, comma)
}

func EncodeBets(bets []bet.Bet) []byte {
	records := make([]string, len(bets))
	for i, bet := range bets {
		records[i] = encodeBetRecord(bet)
	}
	payload := strings.Join(records, newLine)

	message := make([]byte, 1+len(payload))
	message[0] = MsgBet
	copy(message[1:], payload)
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

// returns the batch error message in a string mode, used to debug batch errors.
func DecodeBatchError(raw []byte) (string, error) {
	if len(raw) < 1 {
		return "", fmt.Errorf("empty message")
	}
	return string(raw[1:]), nil
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
		fields := strings.Split(record, comma)
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