import signal
import socket
import threading

import logger

from .client_handler import ClientHandler

from .close_server import (
    ShutdownRequested,
    handle_sigterm,
    join_threads_with_bounded_timeout,
)

class Server:
    def __init__(
        self,
        server_host: str,
        server_port: int,
        lottery,
        agency_quorum_min: int,
    ) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery = lottery
        self.agency_quorum_min = agency_quorum_min

        self.shutdown_event = threading.Event()
        self.server_socket = None
        self.threads = []
        self.threads_lock = threading.Lock()
        self.client_sockets = set()
        self.client_sockets_lock = threading.Lock()
        self.storage_lock = threading.Lock()
        self.quorum_condition = threading.Condition()
        self.finished_agencies = set()

    def serve_client(self, client_socket):
        with self.client_sockets_lock:
            self.client_sockets.add(client_socket)

        try:
            with client_socket:
                handler = ClientHandler(
                    client_socket,
                    self.lottery,
                    self.storage_lock,
                    self.quorum_condition,
                    self.finished_agencies,
                    self.agency_quorum_min,
                    self.shutdown_event,
                )
                handler.handle()

        except ShutdownRequested:
            logger.info("handle-client", logger.LogResult.success, "reason", "shutdown")
        except Exception:
            logger.error("handle-client", logger.LogResult.fail)
        finally:
            with self.client_sockets_lock:
                self.client_sockets.discard(client_socket)

    def run(self):
        signal.signal(signal.SIGTERM, lambda signum, frame: handle_sigterm(self))

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            self.server_socket = server_socket

            while not self.shutdown_event.is_set():
                try:
                    logger.info("accept-connection", logger.LogResult.in_progress)

                    client_socket, _ = server_socket.accept()

                    logger.info("accept-connection", logger.LogResult.success)

                except OSError:
                    break
                except Exception:
                    continue

                thread = threading.Thread(
                    target=self.serve_client,
                    args=(client_socket,),
                    daemon=True,
                )

                with self.threads_lock:
                    self.threads.append(thread)

                thread.start()

        join_threads_with_bounded_timeout(self)

        logger.info("shutdown", logger.LogResult.success)
