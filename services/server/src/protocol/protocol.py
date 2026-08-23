MSG_BET = 1
MSG_DONE = 2
MSG_ACK = 3
MSG_WINNERS = 4
MSG_BATCH_ERROR = 5

COMA = ","
NEW_LINE = "\n"


class BetFields:
    def __init__(self, agency_id, first_name, last_name, document, birthdate, number):
        self.agency_id = agency_id
        self.first_name = first_name
        self.last_name = last_name
        self.document = document
        self.birthdate = birthdate
        self.number = number


def decode_message_type(raw: bytes) -> int:
    if len(raw) < 1:
        raise ValueError("empty message")
    return raw[0]


def _decode_bet_record(record: str) -> BetFields:
    agency_id, first_name, last_name, document, birthdate, number = record.split(
        COMA
    )
    return BetFields(
        int(agency_id), first_name, last_name, int(document), birthdate, int(number)
    )


def decode_bets(raw: bytes) -> list[BetFields]:
    payload = raw[1:].decode("utf-8")
    return [_decode_bet_record(record) for record in payload.split(NEW_LINE)]


def encode_bet(agency_id, first_name, last_name, document, birthdate, number) -> bytes:
    fields = COMA.join(
        [str(agency_id), first_name, last_name, str(document), birthdate, str(number)]
    )
    return bytes([MSG_BET]) + fields.encode("utf-8")


def encode_done() -> bytes:
    return bytes([MSG_DONE])


def encode_ack() -> bytes:
    return bytes([MSG_ACK])


def encode_batch_error(message: str) -> bytes:
    return bytes([MSG_BATCH_ERROR]) + message.encode("utf-8")


def encode_winners(bets) -> bytes:
    records = [
        COMA.join(
            [bet.first_name, bet.last_name, str(bet.document), bet.birthdate, str(bet.number)]
        )
        for bet in bets
    ]
    payload = NEW_LINE.join(records)
    return bytes([MSG_WINNERS]) + payload.encode("utf-8")