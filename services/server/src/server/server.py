import signal
import socket
import threading
import time

import logger
import safe_socket
import protocol
from lottery import Lottery, Bet


class ShutdownRequested(Exception):
    """Señal interna: el servidor está apagándose, abortar sin tratarlo
    como un error real de comunicación."""


class Server:
    SHUTDOWN_GRACE_PERIOD_SECONDS = 5.0

    def __init__(
        self,
        server_host: str,
        server_port: int,
        lottery: Lottery,
        agency_quorum_min: int,
    ) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery = lottery
        self.agency_quorum_min = agency_quorum_min

        self._storage_lock = threading.Lock()
        self._quorum_condition = threading.Condition()

        self._finished_agencies = set()

        self._shutdown_event = threading.Event()
        self._server_socket = None
        self._threads = []
        self._threads_lock = threading.Lock()
        self._client_sockets = set()
        self._client_sockets_lock = threading.Lock()

    def _handle_sigterm(self, signum, frame):
        logger.info("shutdown", logger.LogResult.in_progress)
        self._shutdown_event.set()

        if self._server_socket is not None:
            try:
                self._server_socket.close()
            except OSError:
                pass

        with self._quorum_condition:
            self._quorum_condition.notify_all()

    def _force_close_remaining_clients(self):
        with self._client_sockets_lock:
            sockets_snapshot = list(self._client_sockets)
        for client_socket in sockets_snapshot:
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def _join_threads_with_bounded_timeout(self):
        deadline = time.monotonic() + self.SHUTDOWN_GRACE_PERIOD_SECONDS
        with self._threads_lock:
            threads_snapshot = list(self._threads)

        for thread in threads_snapshot:
            remaining = max(0.0, deadline - time.monotonic())
            thread.join(timeout=remaining)

        still_alive = [t for t in threads_snapshot if t.is_alive()]
        if still_alive:
            logger.error(
                "shutdown", logger.LogResult.fail,
                "still-alive-threads", len(still_alive),
            )
            self._force_close_remaining_clients()
            for thread in still_alive:
                thread.join(timeout=1.0)

    def _await_quorum(self, agency_id: int) -> None:
        with self._quorum_condition:
            self._finished_agencies.add(agency_id)
            quorum_reached = len(self._finished_agencies) >= self.agency_quorum_min

            if quorum_reached:
                self._quorum_condition.notify_all()
            else:
                self._quorum_condition.wait_for(
                    lambda: (
                        len(self._finished_agencies) >= self.agency_quorum_min
                        or self._shutdown_event.is_set()
                    )
                )

            if (
                self._shutdown_event.is_set()
                and len(self._finished_agencies) < self.agency_quorum_min
            ):
                raise ShutdownRequested()

    def _handle_bet_batch(self, client_socket, raw_message):
        try:
            fields_list = protocol.decode_bets(raw_message)
            bets = [
                Bet(
                    f.agency_id, f.first_name, f.last_name,
                    f.document, f.birthdate, f.number,
                )
                for f in fields_list
            ]
            with self._storage_lock:
                self.lottery.store_bets(bets)
        except Exception as e:
            logger.error("handle-batch", logger.LogResult.fail, "err", str(e))
            safe_socket.send_message(
                client_socket, protocol.encode_batch_error(str(e))
            )
            raise

        safe_socket.send_message(client_socket, protocol.encode_ack())
        return bets[0].agency_id, len(bets)

    def _winners_for_agency(self, agency_id) -> list:
        with self._storage_lock:
            all_bets = list(self.lottery.load_bets())
        return [
            bet for bet in all_bets
            if bet.agency_id == agency_id and self.lottery.has_won(bet)
        ]

    def _handle_client(self, client_socket):
        action = "handle-client"
        bets_amount = 0
        agency_id = None
        logger.info(action, logger.LogResult.in_progress)

        while True:
            try:
                message = safe_socket.recv_message(client_socket)
            except EOFError:
                logger.info(
                    action, logger.LogResult.success, "bets-amount", bets_amount
                )
                return
            except Exception:
                logger.error(
                    action, logger.LogResult.fail, "bets-amount", bets_amount
                )
                raise

            msg_type = protocol.decode_message_type(message)

            if msg_type == protocol.MSG_BET:
                batch_agency_id, batch_count = self._handle_bet_batch(
                    client_socket, message
                )
                agency_id = batch_agency_id
                bets_amount += batch_count

            elif msg_type == protocol.MSG_DONE:
                logger.info(
                    action, logger.LogResult.success,
                    "agency-id", agency_id, "bets-amount", bets_amount,
                )
                self._await_quorum(agency_id)
                winners = self._winners_for_agency(agency_id)
                safe_socket.send_message(
                    client_socket, protocol.encode_winners(winners)
                )
                return

            else:
                raise ValueError(f"unexpected message type: {msg_type}")

    def _serve_client(self, client_socket):
        with self._client_sockets_lock:
            self._client_sockets.add(client_socket)

        try:
            with client_socket:
                self._handle_client(client_socket)
        except ShutdownRequested:
            logger.info(
                "handle-client", logger.LogResult.success, "reason", "shutdown"
            )
        except Exception:
            logger.error("handle-client", logger.LogResult.fail)
        finally:
            with self._client_sockets_lock:
                self._client_sockets.discard(client_socket)

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            self._server_socket = server_socket

            while not self._shutdown_event.is_set():
                try:
                    logger.info("accept-connection", logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                    logger.info("accept-connection", logger.LogResult.success)
                except OSError:
                    break
                except Exception:
                    continue

                thread = threading.Thread(
                    target=self._serve_client,
                    args=(client_socket,),
                    daemon=True,
                )
                with self._threads_lock:
                    self._threads.append(thread)
                thread.start()

        self._join_threads_with_bounded_timeout()
        logger.info("shutdown", logger.LogResult.success)