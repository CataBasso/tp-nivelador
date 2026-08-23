import socket
import threading

import logger
import safe_socket
import protocol
from lottery import Lottery, Bet


class Server:
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

    def _await_quorum(self, agency_id: int) -> None:
        with self._quorum_condition:
            self._finished_agencies.add(agency_id)
            if len(self._finished_agencies) >= self.agency_quorum_min:
                self._quorum_condition.notify_all()
            else:
                self._quorum_condition.wait_for(
                    lambda: len(self._finished_agencies) >= self.agency_quorum_min
                )

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

                logger.info(
                    "await-quorum", logger.LogResult.in_progress,
                    "agency-id", agency_id,
                )
                self._await_quorum(agency_id)
                logger.info(
                    "await-quorum", logger.LogResult.success,
                    "agency-id", agency_id,
                )

                winners = self._winners_for_agency(agency_id)
                safe_socket.send_message(
                    client_socket, protocol.encode_winners(winners)
                )
                return

            else:
                raise ValueError(f"unexpected message type: {msg_type}")

    def _serve_client(self, client_socket):
        try:
            with client_socket:
                self._handle_client(client_socket)
        except Exception:
            logger.error("handle-client", logger.LogResult.fail)

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info("accept-connection", logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                    logger.info("accept-connection", logger.LogResult.success)
                except Exception:
                    continue

                thread = threading.Thread(
                    target=self._serve_client,
                    args=(client_socket,),
                    daemon=True,
                )
                thread.start()