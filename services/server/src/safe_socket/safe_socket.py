import socket
import struct 

def recv_all(socket: socket.socket, size: int) -> bytes:
    data = bytearray()

    while len(data) < size:
        try:
            chunk = socket.recv(max(size - len(data), 2))
        except InterruptedError:
            continue
        except OSError as e:
            raise ConnectionError(f"recv failed: {e}") from e

        if not chunk:
            if len(data) == 0:
                raise EOFError("socket closed by peer")
            raise ConnectionError(
                f"socket connection closed: expected={size} received={len(data)}"
            )

        data.extend(chunk)

    return bytes(data)

def send_all(socket: socket.socket, bytes_data: bytes) -> int:
    total_sent = 0

    while total_sent < len(bytes_data):
        try:
            sent = socket.send(bytes_data[total_sent:])
        except InterruptedError:
            continue
        except OSError as e:
            raise ConnectionError(f"send failed: {e}") from e

        total_sent += sent

    return total_sent

def recv_message(socket: socket.socket):
    message_len_bytes = recv_all(socket, 4)

    if len(message_len_bytes) != 4:
        raise ConnectionError("Incomplete message length")

    message_len = struct.unpack("!I", message_len_bytes)[0]

    message = recv_all(socket, message_len)

    if len(message) != message_len:
        raise ConnectionError("Incomplete message")

    return message

def send_message(socket: socket.socket, message: bytes):
    packet = struct.pack("!I", len(message)) + message
    send_all(socket, packet)
