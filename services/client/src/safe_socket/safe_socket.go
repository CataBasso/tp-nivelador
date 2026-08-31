package safe_socket

import (
	"encoding/binary"
	"fmt"
	"io"
)

const headerSize = 4

func SendAll(socket io.Writer, bytes []byte) error {
	totalSent := 0

	for totalSent < len(bytes) {
		sent, err := socket.Write(bytes[totalSent:])

		if sent > 0 {
			totalSent += sent
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	buff := make([]byte, size)
	totalReceived := 0

	for totalReceived < size {
		received, err := socket.Read(buff[totalReceived:])

		if received > 0 {
			totalReceived += received
		}

		if err != nil {
			if err == io.EOF {
				if totalReceived == size {
					break
				}
				return nil, fmt.Errorf("socket connection closed: expected=%d received=%d", size, totalReceived)
			}
			return nil, err
		}

		if received == 0 {
			return nil, fmt.Errorf("socket connection closed: expected=%d received=%d", size, totalReceived)
		}
	}

	return buff, nil
}

func SendMessage(socket io.Writer, message []byte) error {
	header := [headerSize]byte{}
	binary.BigEndian.PutUint32(header[:], uint32(len(message)))

	if err := SendAll(socket, header[:]); err != nil {
		return err
	}
	return SendAll(socket, message)
}

func RecvMessage(socket io.Reader) ([]byte, error) {
	messageLengthBytes, err := RecvAll(socket, headerSize)
	if err != nil {
		return nil, err
	}

	if len(messageLengthBytes) != headerSize {
		return nil, fmt.Errorf("incomplete message length")
	}

	messageLength := binary.BigEndian.Uint32(messageLengthBytes)

	message, err := RecvAll(socket, int(messageLength))
	if err != nil {
		return nil, err
	}

	if len(message) != int(messageLength) {
		return nil, fmt.Errorf("incomplete message")
	}

	return message, nil
}
