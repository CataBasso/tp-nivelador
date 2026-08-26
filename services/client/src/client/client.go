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

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/bet"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const (
	connectionRetryDelay    = 200 * time.Millisecond
	connectionWaitTimeout   = 20 * time.Second
	dialTimeout             = 2 * time.Second
	shutdownForceCloseDelay = 3 * time.Second
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
	fields := strings.Split(line, ",")
	if len(fields) != 5 {
		return bet.Bet{}, fmt.Errorf(
			"malformed line: expected 5 fields, got %d",
			len(fields),
		)
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

func (client *Client) sendBatch(batch []bet.Bet, batchArgs []any) error {
	if err := safe_socket.SendMessage(
		client.conn,
		protocol.EncodeBets(batch),
	); err != nil {
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
		logger.Error(
			"recv-batch-response",
			logger.Fail,
			append(batchArgs, "server-err", errMsg)...,
		)
		return fmt.Errorf("server rejected batch: %s", errMsg)

	default:
		return fmt.Errorf(
			"unexpected response type to batch: %d",
			responseType,
		)
	}
}

func (client *Client) handleShutdown(
	ctx context.Context,
	shutdownDone <-chan struct{},
) {
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

func (client *Client) processBets(
	ctx context.Context,
	scanner *bufio.Scanner,
	agencyId int,
) (int, int, bool, error) {
	betsAmount := 0
	batchesAmount := 0
	batch := make([]bet.Bet, 0, client.config.BatchSize)
	shuttingDown := false

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		batchArgs := []any{
			"agency-id", client.config.AgencyId,
			"batch-id", batchesAmount,
			"batch-size", len(batch),
		}

		if err := client.sendBatch(batch, batchArgs); err != nil {
			return err
		}

		betsAmount += len(batch)
		batchesAmount++
		batch = batch[:0]

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

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parsedBet, err := parseBetLine(line, agencyId)
		if err != nil {
			logger.Error(
				"parse-bet",
				logger.Fail,
				"agency-id", client.config.AgencyId,
				"bet-id", betsAmount+len(batch),
			)
			return 0, 0, false, err
		}

		batch = append(batch, parsedBet)

		if len(batch) == client.config.BatchSize {
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

	if err := safe_socket.SendMessage(
		client.conn,
		protocol.EncodeDone(),
	); err != nil {
		logger.Error(
			"send-done",
			logger.Fail,
			"agency-id", client.config.AgencyId,
		)
		return 0, 0, false, err
	}

	return betsAmount, batchesAmount, false, nil
}

func (client *Client) Run(ctx context.Context) error {
	defer client.conn.Close()

	agencyId, err := strconv.Atoi(client.config.AgencyId)
	if err != nil {
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
		client.processBets(ctx, scanner, agencyId)

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
		logger.Error(
			"recv-winners",
			logger.Fail,
			"agency-id", client.config.AgencyId,
		)
		return err
	}

	winners, err := protocol.DecodeWinners(winnersRaw)
	if err != nil {
		return err
	}

	for _, w := range winners {
		row := strings.Join(
			[]string{
				w.FirstName,
				w.LastName,
				w.Document,
				w.Birthdate,
				w.Number,
			},
			",",
		)

		if _, err := fmt.Fprintf(writer, "%s\n", row); err != nil {
			return err
		}
	}

	logger.Info(
		"send-bets",
		logger.Success,
		"agency-id", client.config.AgencyId,
		"bets-amount", betsAmount,
		"batches-amount", batchesAmount,
		"winners-amount", len(winners),
	)

	return nil
}
