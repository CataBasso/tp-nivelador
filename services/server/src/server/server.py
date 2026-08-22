import socket
import logger
import safe_socket
import protocol
from lottery import Lottery, Bet


class Server:
    def __init__(self, server_host: str, server_port: int, lottery: Lottery) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery = lottery

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
                fields = protocol.decode_bet(message)
                agency_id = fields.agency_id
                bet = Bet(
                    fields.agency_id,
                    fields.first_name,
                    fields.last_name,
                    fields.document,
                    fields.birthdate,
                    fields.number,
                )
                self.lottery.store_bets([bet])
                bets_amount += 1
                safe_socket.send_message(client_socket, protocol.encode_ack())

            elif msg_type == protocol.MSG_DONE:
                logger.info(
                    action,
                    logger.LogResult.success,
                    "agency-id", agency_id,
                    "bets-amount", bets_amount,
                )
                winners = self._winners_for_agency(agency_id)
                safe_socket.send_message(
                    client_socket, protocol.encode_winners(winners)
                )
                return

            else:
                raise ValueError(f"unexpected message type: {msg_type}")

    def _winners_for_agency(self, agency_id) -> list:
        return [
            bet
            for bet in self.lottery.load_bets()
            if bet.agency_id == agency_id and self.lottery.has_won(bet)
        ]

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info("accept-connection", logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                    logger.info("accept-connection", logger.LogResult.success)

                    with client_socket:
                        self._handle_client(client_socket)
                except Exception:
                    continue