import json
import logging
from confluent_kafka import Consumer, Producer, KafkaError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CONFIG = {
    'bootstrap.servers': 'localhost:9094',
    'group.id': 'ack-generation-group-python',
    'auto.offset.reset': 'earliest'
}

class AckGenerator:
    def __init__(self, config):
        self.consumer = Consumer(config)
        self.producer = Producer({'bootstrap.servers': config['bootstrap.servers']})

    def build_997_ack(self, tracking_id: str, status: str) -> str:
        """
        Generates an X12 997 payload.
        status: 'A' = Accepted, 'R' = Rejected
        """
        # This is a basic structurally hardcoded 997 wrapper mapping your feedback loops
        return (
            f"ISA*00*          *00*          *ZZ*RECEIVERONLY   *ZZ*SENDERONLY     *260518*1600*U*00401*000000001*0*P*>~\n"
            f"GS*FA*RECEIVERONLY*SENDERONLY*20260518*1600*1*X*004010~\n"
            f"ST*997*0001~\n"
            f"AK1*PO*{tracking_id}~\n"
            f"AK2*850*0001~\n"
            f"AK5*{status}~\n"
            f"SE*6*0001~\n"
            f"GE*1*1~\n"
            f"IEA*1*000000001~"
        )

    def start(self):
        # Subscribe to both success and failure tracks simultaneously
        self.consumer.subscribe(['edi-850-parsed', 'edi-dead-letter-queue'])
        logging.info("997 Acknowledgment Listener Engine Started...")

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logging.error(f"Ack Engine Error: {msg.error()}")
                        break

                topic = msg.topic()
                msg_key = msg.key().decode('utf-8') if msg.key() else "UNKNOWN_PO"
                
                if topic == 'edi-850-parsed':
                    # Generate Positive ACK
                    ack_997 = self.build_997_ack(msg_key, "A")
                    logging.info(f"Generating positive 997 ACK for: {msg_key}")
                else:
                    # Generate Negative (Rejected) ACK from DLQ events
                    ack_997 = self.build_997_ack(msg_key, "R")
                    logging.info(f"Generating negative (Failure) 997 ACK for: {msg_key}")

                # Publish out bound verification code back to trade partner routing topic
                self.producer.produce(
                    topic='edi-outbound-997', 
                    key=msg_key, 
                    value=ack_997
                )
                self.producer.poll(0)

        except KeyboardInterrupt:
            logging.info("Stopping Ack engine safely...")
        finally:
            self.consumer.close()
            self.producer.flush()

if __name__ == "__main__":
    engine = AckGenerator(CONFIG)
    engine.start()