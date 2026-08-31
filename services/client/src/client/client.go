package client

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"bytes"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/bet"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const (
	connectionRetryDelay = 200 * time.Millisecond
	connectionWaitTimeout = 20 * time.Second
	dialTimeout = 2 * time.Second
	shutdownForceCloseDelay = 3 * time.Second

	newLine = '\n'
)

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
	BatchSize  int
}

type Client struct {
	conn   net.Conn
	config ClientConfig
	sendBuf bytes.Buffer
}

func NewClient(ctx context.Context, config ClientConfig) (*Client, error) {
	conn, err := connectToServer(ctx, config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(ctx context.Context, host, port string) (net.Conn, error) {
	const action = "connect-to-server"

	deadline := time.Now().Add(connectionWaitTimeout)
	address := net.JoinHostPort(host, port)
	attempt := 0
	var lastErr error

	logger.Info(action, logger.InProgress)

	for {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("connection aborted: %w", ctx.Err())
		}

		conn, err := net.DialTimeout("tcp", address, dialTimeout)
		if err == nil {
			logger.Info(action, logger.Success, "attempt", attempt)
			return conn, nil
		}

		lastErr = err
		logger.Warn(action, logger.Fail, "attempt", attempt, "err", err)
		attempt++

		if time.Now().After(deadline) {
			return nil, fmt.Errorf(
				"could not connect to %s after %d attempts: %w",
				address,
				attempt-1,
				lastErr,
			)
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("connection aborted: %w", ctx.Err())
		case <-time.After(connectionRetryDelay):
		}
	}
}

func parseBetLine(line string, agencyId int) (bet.Bet, error) {
	firstName, rest, ok := strings.Cut(line, ",")
	if !ok {
		return bet.Bet{}, fmt.Errorf("malformed line")
	}
	lastName, rest, ok := strings.Cut(rest, ",")
	if !ok {
		return bet.Bet{}, fmt.Errorf("malformed line")
	}
	documentStr, rest, ok := strings.Cut(rest, ",")
	if !ok {
		return bet.Bet{}, fmt.Errorf("malformed line")
	}
	birthdate, numberStr, ok := strings.Cut(rest, ",")
	if !ok {
		return bet.Bet{}, fmt.Errorf("malformed line")
	}

	document, err := strconv.Atoi(documentStr)
	if err != nil {
		return bet.Bet{}, fmt.Errorf("invalid document: %w", err)
	}

	number, err := strconv.Atoi(numberStr)
	if err != nil {
		return bet.Bet{}, fmt.Errorf("invalid number: %w", err)
	}

	return bet.Bet{
		AgencyId:  agencyId,
		FirstName: firstName,
		LastName:  lastName,
		Document:  document,
		Birthdate: birthdate,
		Number:    number,
	}, nil
}

// Sends a batch of bets to the server and handles the response.
// It returns an error if the server rejects the batch or if there is a communication error.
func (client *Client) sendBatch(batchArgs []any) error {
	if err := safe_socket.SendMessage(client.conn, client.sendBuf.Bytes()); err != nil {
		logger.Error("send-batch", logger.Fail, batchArgs...)
		return err
	}

	responseRaw, err := safe_socket.RecvMessage(client.conn)
	if err != nil {
		logger.Error("recv-batch-response", logger.Fail, batchArgs...)
		return err
	}

	responseType, err := protocol.DecodeMessageType(responseRaw)
	if err != nil {
		return err
	}

	switch responseType {
	case protocol.MsgAck:
		return nil
	case protocol.MsgBatchError:
		errMsg, _ := protocol.DecodeBatchError(responseRaw)
		logger.Error("recv-batch-response", logger.Fail, append(batchArgs, "server-err", errMsg)...)
		return fmt.Errorf("server rejected batch: %s", errMsg)
	default:
		return fmt.Errorf("unexpected response type to batch: %d", responseType)
	}
}

// Listens for context cancellation and gracefully shuts down the client.
func (client *Client) handleShutdown(ctx context.Context, shutdownDone <-chan struct{}) {
	select {
	case <-ctx.Done():
		logger.Info("shutdown", logger.InProgress)

		select {
		case <-shutdownDone:
			return

		case <-time.After(shutdownForceCloseDelay):
			logger.Error(
				"shutdown",
				logger.Fail,
				"reason", "force-close",
			)
			client.conn.Close()
		}

	case <-shutdownDone:
		return
	}
}

func (client *Client) resetSendBuffer() {
	client.sendBuf.Reset()
	client.sendBuf.WriteByte(protocol.MsgBet)
}

// Processes bets from the input file, sending them in batches to the server.
// It returns: 
// 		- the total number of bets processed, 
// 		- the number of batches sent, 
// 		- a boolean indicating if the process was interrupted by a shutdown signal, 
// 		- an error if any occurred
func (client *Client) processBets(ctx context.Context, scanner *bufio.Scanner) (int, int, bool, error) {
	betsAmount := 0
	batchesAmount := 0
	lineCount := 0
	shuttingDown := false

	client.resetSendBuffer()

	flushBatch := func() error {
		if lineCount == 0 {
			return nil
		}
		batchArgs := []any{
			"agency-id", client.config.AgencyId,
			"batch-id", batchesAmount,
			"batch-size", lineCount,
		}
		if err := client.sendBatch(batchArgs); err != nil {
			return err
		}
		betsAmount += lineCount
		batchesAmount++
		lineCount = 0
		client.resetSendBuffer()
		return nil
	}

scanLoop:
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			shuttingDown = true
			break scanLoop
		default:
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		if lineCount > 0 {
			client.sendBuf.WriteByte(newLine)
		}
		if err := protocol.EncodeBet(&client.sendBuf, client.config.AgencyId, line); err != nil {
			logger.Error("parse-bet", logger.Fail,
				"agency-id", client.config.AgencyId,
				"bet-id", betsAmount+lineCount)
			return 0, 0, false, err
		}
		lineCount++

		if lineCount == client.config.BatchSize {
			if err := flushBatch(); err != nil {
				return 0, 0, false, err
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, 0, false, err
	}
	if shuttingDown {
		return betsAmount, batchesAmount, true, nil
	}
	if err := flushBatch(); err != nil {
		return 0, 0, false, err
	}
	if err := safe_socket.SendMessage(client.conn, protocol.EncodeDone()); err != nil {
		logger.Error("send-done", logger.Fail, "agency-id", client.config.AgencyId)
		return 0, 0, false, err
	}

	return betsAmount, batchesAmount, false, nil
}

func (client *Client) Run(ctx context.Context) error {
	defer client.conn.Close()

	if _, err := strconv.Atoi(client.config.AgencyId); err != nil {
		return fmt.Errorf("invalid AGENCY_ID %q: %w", client.config.AgencyId, err)
	}

	if client.config.BatchSize <= 0 {
		return fmt.Errorf(
			"invalid BATCH_SIZE %d: must be greater than 0",
			client.config.BatchSize,
		)
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

	shutdownDone := make(chan struct{})
	defer close(shutdownDone)

	go client.handleShutdown(ctx, shutdownDone)

	betsAmount, batchesAmount, shuttingDown, err :=
		client.processBets(ctx, scanner)

	if err != nil {
		return err
	}

	if shuttingDown {
		logger.Info(
			"send-bets",
			logger.Success,
			"agency-id", client.config.AgencyId,
			"bets-amount", betsAmount,
			"batches-amount", batchesAmount,
			"reason", "shutdown",
		)
		return nil
	}

	winnersRaw, err := safe_socket.RecvMessage(client.conn)
	if err != nil {
		logger.Error("recv-winners", logger.Fail, "agency-id", client.config.AgencyId)
		return err
	}

	winnersAmount, err := protocol.DecodeWinners(writer, winnersRaw)
	if err != nil {
		return err
	}

	logger.Info(
		"send-bets",
		logger.Success,
		"agency-id", client.config.AgencyId,
		"bets-amount", betsAmount,
		"batches-amount", batchesAmount,
		"winners-amount", winnersAmount,
	)

	return nil
}
