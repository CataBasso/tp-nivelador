import socket
import time

import logger


SHUTDOWN_GRACE_PERIOD_SECONDS = 5.0


class ShutdownRequested(Exception):
    """El servidor está apagándose."""


def handle_sigterm(server):
    logger.info("shutdown", logger.LogResult.in_progress)

    server.shutdown_event.set()

    if server.server_socket is not None:
        try:
            server.server_socket.close()
        except OSError:
            pass

    with server.quorum_condition:
        server.quorum_condition.notify_all()


def force_close_remaining_clients(server):
    with server.client_sockets_lock:
        sockets_snapshot = list(server.client_sockets)

    for client_socket in sockets_snapshot:
        try:
            client_socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass


def join_threads_with_bounded_timeout(server):
    deadline = (
        time.monotonic()
        + SHUTDOWN_GRACE_PERIOD_SECONDS
    )

    with server.threads_lock:
        threads_snapshot = list(server.threads)

    for thread in threads_snapshot:
        remaining = max(
            0.0,
            deadline - time.monotonic(),
        )
        thread.join(timeout=remaining)

    still_alive = [
        thread
        for thread in threads_snapshot
        if thread.is_alive()
    ]

    if still_alive:
        logger.error(
            "shutdown",
            logger.LogResult.fail,
            "still-alive-threads",
            len(still_alive),
        )

        force_close_remaining_clients(server)

        for thread in still_alive:
            thread.join(timeout=1.0)