import logging
import time
from confluent_kafka import Producer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka Configuration
PRODUCER_CONFIG = {
    'bootstrap.servers': 'localhost:9094',
  #  'enable.idempotence': True,        # Guarantees no duplicate records on network retry
    'acks': 'all',                     # Ensures all backup brokers acknowledge receipt
    'max.in.flight.requests.per.connection': 5
}

class EdiRawProducer:
    def __init__(self, config):
        self.producer = Producer(config)
        self.topic = 'edi-850-raw'

    def delivery_report(self, err, msg):
        """Callback invoked when a message is successfully delivered or fails."""
        if err is not None:
            logging.error(f"Failed to deliver EDI message: {err}")
        else:
            logging.info(f"Successfully pushed to {msg.topic()} | Partition: {msg.partition()} | Offset: {msg.offset()}")

    def extract_fallback_key(self, edi_text: str) -> str:
        """Quickly peeks inside the payload to pull out the PO number to use as the Kafka Key."""
        segments = edi_text.split('~')
        for segment in segments:
            if segment.strip().startswith('BEG'):
                elements = segment.split('*')
                if len(elements) > 3:
                    return elements[3].strip()  # Return the PO Number
        return "UNKNOWN_BATCH"

    def send_edi_payload(self, raw_edi_string: str):
        """Sends a single raw EDI transaction block to Kafka."""
        try:
            # Use the PO Number as the message key to keep routing consistent per vendor
            msg_key = self.extract_fallback_key(raw_edi_string)
            
            logging.info(f"Publishing raw EDI to Kafka for Tracking Key: {msg_key}...")
            
            # Send payload asynchronously
            self.producer.produce(
                topic=self.topic,
                key=msg_key,
                value=raw_edi_string.encode('utf-8'),
                callback=self.delivery_report
            )
            
            # Trigger internal batch delivery queues
            self.producer.poll(0)
            
        except Exception as e:
            logging.error(f"Error putting payload into broker queue: {str(e)}")

    def close(self):
        """Flushes outstanding memory queues before terminating application process."""
        logging.info("Flushing buffer queue...")
        self.producer.flush()


if __name__ == "__main__":
    # Mocking standard incoming EDI 850 files (X12 Format)
    sample_edi_file_1 = (
        "ISA*00*          *00*          *ZZ*SENDERONLY     *ZZ*RECEIVERONLY   *260524*1030*U*00401*00000021*0*P*>~\n"
        "GS*PO*SENDERONLY*RECEIVERONLY*20260524*1030*1*X*004010~\n"
        "ST*850*0001~\n"
        "BEG*00*SA*PO-99991**20260524~\n"
        "PO1*1*45*EA*29.99**VC*PROD-XYZ~\n"
        "CTT*1~\n"
        "SE*6*0001~\n"
        "GE*1*1~\n"
        "IEA*1*000000201~"
    )

    sample_edi_file_2 = (
        "ISA*00*          *00*          *ZZ*SENDERONLY     *ZZ*RECEIVERONLY   *260524*1031*U*00401*000000204*0*P*>~\n"
        "GS*PO*SENDERONLY*RECEIVERONLY*20260524*1031*1*X*004010~\n"
        "ST*850*0002~\n"
        "BEG*00*SA*PO-99992**20260524~\n"
        "PO1*1*120*EA*5.50**VC*PROD-MNO~\n"
        "CTT*1~\n"
        "SE*6*0001~\n"
        "GE*1*1~\n"
        "IEA*1*000000202~"
    )

    # Initialize and fire messages
    edi_sender = EdiRawProducer(PRODUCER_CONFIG)
    
    edi_sender.send_edi_payload(sample_edi_file_1)
    time.sleep(5) # Simulating spacing between file deliveries
    edi_sender.send_edi_payload(sample_edi_file_2)
    
    # Close down cleanly
    edi_sender.close()