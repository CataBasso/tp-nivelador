import socket
import logger
import safe_socket

class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        logger.info(action, logger.LogResult.in_progress)

        while True:
            try:
                client_message = safe_socket.recv_message(client_socket)
            except EOFError:
                logger.info(
                    action,
                    logger.LogResult.success,
                    "messages-amount",
                    message_amount,
                )
                return
            except Exception:
                logger.error(
                    action, logger.LogResult.fail, "messages-amount", message_amount
                )
                raise

            message_amount += 1
            safe_socket.send_message(client_socket, client_message)

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
