package protocol

import (
	"bytes"
	"fmt"
	"bufio"
)

const (
	MsgBet        byte = 1
	MsgDone       byte = 2
	MsgAck        byte = 3
	MsgWinners    byte = 4
	MsgBatchError byte = 5
)

const (
	comma = ","
	newLine = "\n"
)

type WinnerRecord struct {
	FirstName string
	LastName  string
	Document  string
	Birthdate string
	Number    string
}

func isDigit(b []byte) bool {
	if len(b) == 0 {
		return false
	}

	for _, c := range b {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func EncodeBet(buf *bytes.Buffer, agencyId string, line []byte) error {
	first := bytes.IndexByte(line, comma[0])
	if first == -1 {
		return fmt.Errorf("malformed line: expected 5 fields")
	}
	second := bytes.IndexByte(line[first+1:], comma[0])
	if second == -1 {
		return fmt.Errorf("malformed line: expected 5 fields")
	}
	second += first + 1
	third := bytes.IndexByte(line[second+1:], comma[0])
	if third == -1 {
		return fmt.Errorf("malformed line: expected 5 fields")
	}
	third += second + 1
	fourth := bytes.IndexByte(line[third+1:], comma[0])
	if fourth == -1 {
		return fmt.Errorf("malformed line: expected 5 fields")
	}
	fourth += third + 1

	document := line[second+1 : third]
	number := line[fourth+1:]

	if bytes.IndexByte(number, ',') != -1 {
		return fmt.Errorf("malformed line: expected 5 fields")
	}
	if !isDigit(document) {
		return fmt.Errorf("invalid document: %q", document)
	}
	if !isDigit(number) {
		return fmt.Errorf("invalid number: %q", number)
	}

	buf.WriteString(agencyId)
	buf.WriteByte(',')
	buf.Write(line)
	return nil
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

	if raw[0] != MsgBatchError {
    	return "", fmt.Errorf("unexpected message type: %d", raw[0])
	}

	return string(raw[1:]), nil
}

// writes each winner record directly to the output writer. 
// It returns the number of winners written and an error if any.
func DecodeWinners(winners *bufio.Writer, raw []byte) (int, error) {
	if len(raw) < 1 {
		return 0, fmt.Errorf("empty message")
	}

	if raw[0] != MsgWinners {
    	return 0, fmt.Errorf("unexpected message type: %d", raw[0])
	}

	payload := raw[1:]

	count := 0
	for len(payload) > 0 {
		line := []byte{}
		idx := bytes.IndexByte(payload, newLine[0])
		if idx == -1 {
			line = payload
			payload = nil
		} else {
			line = payload[:idx]
			payload = payload[idx+1:]
		}

		if bytes.Count(line, []byte(comma)) != 4 {
			return count, fmt.Errorf("malformed winner record: %q", line)
		}

		if _, err := winners.Write(line); err != nil {
			return count, err
		}
		if err := winners.WriteByte(newLine[0]); err != nil {
			return count, err
		}
		count++
	}

	return count, nil
}