package client

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/bet"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_RETRY_DELAY = 200 * time.Millisecond
const CONNECTION_WAIT_TIMEOUT = 20 * time.Second
const DIAL_TIMEOUT = 2 * time.Second

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
    const action = "connect-to-server"

    deadline := time.Now().Add(CONNECTION_WAIT_TIMEOUT)
    address := net.JoinHostPort(host, port)
    attempt := 0
    var lastErr error

    logger.Info(action, logger.InProgress)

    for {
        conn, err := net.DialTimeout("tcp", address, DIAL_TIMEOUT)
        if err == nil {
            logger.Info(action, logger.Success, "attempt", attempt)
            return conn, nil
        }

        lastErr = err
        logger.Warn(action, logger.Fail, "attempt", attempt, "err", err)
        attempt++

        if time.Now().After(deadline) {
            return nil, fmt.Errorf("could not connect to %s after %d attempts: %w", address, attempt, lastErr)
        }

        time.Sleep(CONNECTION_RETRY_DELAY)
	}
}

func parseBetLine(line string, agencyId int) (bet.Bet, error) {
	line = strings.TrimSpace(line)
	fields := strings.Split(line, ",")
	if len(fields) != 5 {
		return bet.Bet{}, fmt.Errorf("malformed line: expected 5 fields, got %d", len(fields))
	}

	document, err := strconv.Atoi(fields[2])
	if err != nil {
		return bet.Bet{}, fmt.Errorf("invalid document: %w", err)
	}

	number, err := strconv.Atoi(fields[4])
	if err != nil {
		return bet.Bet{}, fmt.Errorf("invalid number: %w", err)
	}

	return bet.Bet{
		AgencyId:  agencyId,
		FirstName: fields[0],
		LastName:  fields[1],
		Document:  document,
		Birthdate: fields[3],
		Number:    number,
	}, nil
}

func (client *Client) Run() error {
	defer client.conn.Close()

	agencyId, err := strconv.Atoi(client.config.AgencyId)
	if err != nil {
		return fmt.Errorf("invalid AGENCY_ID %q: %w", client.config.AgencyId, err)
	}

	inputFile, err := os.Open(client.config.InputFile)
	if err != nil {
		return err
	}
	defer inputFile.Close()

	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	scanner := bufio.NewScanner(inputFile)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	betsAmount := 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		betArgs := []any{"agency-id", client.config.AgencyId, "bet-id", betsAmount}

		parsedBet, err := parseBetLine(line, agencyId)
		if err != nil {
			logger.Error("parse-bet", logger.Fail, betArgs...)
			return err
		}

		if err := safe_socket.SendMessage(client.conn, protocol.EncodeBet(parsedBet)); err != nil {
			logger.Error("send-bet", logger.Fail, betArgs...)
			return err
		}

		ackRaw, err := safe_socket.RecvMessage(client.conn)
		if err != nil {
			logger.Error("recv-ack", logger.Fail, betArgs...)
			return err
		}

		ackType, err := protocol.DecodeMessageType(ackRaw)
		if err != nil || ackType != protocol.MsgAck {
			logger.Error("recv-ack", logger.Fail, betArgs...)
			return fmt.Errorf("unexpected response to bet: type=%d err=%v", ackType, err)
		}

		betsAmount++
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	if err := safe_socket.SendMessage(client.conn, protocol.EncodeDone()); err != nil {
		logger.Error("send-done", logger.Fail, "agency-id", client.config.AgencyId)
		return err
	}

	winnersRaw, err := safe_socket.RecvMessage(client.conn)
	if err != nil {
		logger.Error("recv-winners", logger.Fail, "agency-id", client.config.AgencyId)
		return err
	}

	winners, err := protocol.DecodeWinners(winnersRaw)
	if err != nil {
		return err
	}

	for _, w := range winners {
		row := strings.Join([]string{w.FirstName, w.LastName, w.Document, w.Birthdate, w.Number}, ",")
		if _, err := fmt.Fprintf(writer, "%s\n", row); err != nil {
			return err
		}
	}

	logger.Info(
		"send-bets",
		logger.Success,
		"agency-id", client.config.AgencyId,
		"bets-amount", betsAmount,
		"winners-amount", len(winners),
	)

	return nil
}