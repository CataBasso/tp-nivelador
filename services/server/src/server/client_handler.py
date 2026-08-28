import logger
import protocol
import safe_socket

from lottery import Bet

class ClientHandler:

    def __init__(
        self,
        client_socket,
        lottery,
        storage_lock,
        quorum_condition,
        finished_agencies,
        agency_quorum_min,
        shutdown_event,
    ):
        self.client_socket = client_socket
        self.lottery = lottery
        self.storage_lock = storage_lock
        self.quorum_condition = quorum_condition
        self.finished_agencies = finished_agencies
        self.agency_quorum_min = agency_quorum_min
        self.shutdown_event = shutdown_event

    def handle(self):
        action = "handle-client"
        bets_amount = 0
        agency_id = None

        logger.info(
            action,
            logger.LogResult.in_progress,
        )

        while True:
            try:
                message = safe_socket.recv_message(
                    self.client_socket
                )

            except EOFError:
                logger.info(
                    action,
                    logger.LogResult.success,
                    "bets-amount",
                    bets_amount,
                )
                return

            except Exception:
                logger.error(
                    action,
                    logger.LogResult.fail,
                    "bets-amount",
                    bets_amount,
                )
                raise

            msg_type = protocol.decode_message_type(message)

            if msg_type == protocol.MSG_BET:
                batch_agency_id, batch_count = (
                    self.handle_bet_batch(message)
                )

                agency_id = batch_agency_id
                bets_amount += batch_count

            elif msg_type == protocol.MSG_DONE:
                logger.info(
                    action,
                    logger.LogResult.success,
                    "agency-id",
                    agency_id,
                    "bets-amount",
                    bets_amount,
                )

                self.await_quorum(agency_id)

                winners = self.winners_for_agency(agency_id)

                safe_socket.send_message(
                    self.client_socket,
                    protocol.encode_winners(winners),
                )

                return

            else:
                raise ValueError(
                    f"unexpected message type: {msg_type}"
                )

    def handle_bet_batch(self, raw_message):
        try:
            fields_list = protocol.decode_bets(raw_message)

            bets = [
                Bet(
                    f.agency_id,
                    f.first_name,
                    f.last_name,
                    f.document,
                    f.birthdate,
                    f.number,
                )
                for f in fields_list
            ]

            with self.storage_lock:
                self.lottery.store_bets(bets)

        except Exception as e:
            logger.error(
                "handle-batch",
                logger.LogResult.fail,
                "err",
                str(e),
            )

            safe_socket.send_message(
                self.client_socket,
                protocol.encode_batch_error(str(e)),
            )

            raise

        safe_socket.send_message(
            self.client_socket,
            protocol.encode_ack(),
        )

        return bets[0].agency_id, len(bets)

    def winners_for_agency(self, agency_id):
        with self.storage_lock:
            all_bets = list(self.lottery.load_bets())

        return [
            bet
            for bet in all_bets
            if (
                bet.agency_id == agency_id
                and self.lottery.has_won(bet)
            )
        ]

    def await_quorum(self, agency_id):
        with self.quorum_condition:
            self.finished_agencies.add(agency_id)

            quorum_reached = (
                len(self.finished_agencies)
                >= self.agency_quorum_min
            )

            if quorum_reached:
                self.quorum_condition.notify_all()

            else:
                self.quorum_condition.wait_for(
                    lambda: (
                        len(self.finished_agencies)
                        >= self.agency_quorum_min
                        or self.shutdown_event.is_set()
                    )
                )

            if (
                self.shutdown_event.is_set()
                and len(self.finished_agencies)
                < self.agency_quorum_min
            ):
                raise self.ShutdownRequested()