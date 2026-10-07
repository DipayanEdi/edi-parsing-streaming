import json
import logging
from confluent_kafka import Consumer, Producer, KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9094',
    'group.id': 'edi-processing-group-python',
    'auto.offset.reset': 'earliest',
    'enable.idempotence': True  # Ensures production reliability
}

# In-memory deduplication store (In production, replace with a distributed cache like Redis or a persistent key-value store)
DUPLICATE_STORE = {}

class EdiPipelineProcessor:
    def __init__(self, config):
        self.consumer = Consumer(config)
        # Separate config instance for Producer (removes consumer-specific properties)
        producer_config = {'bootstrap.servers': config['bootstrap.servers'], 'enable.idempotence': True}
        self.producer = Producer(producer_config)
        
    def extract_control_number(self, edi_text: str) -> str:
        """Extracts the Interchange Control Number from the ISA segment."""
        segments = edi_text.split('~')
        for segment in segments:
            if segment.strip().startswith('ISA'):
                elements = segment.split('*')
                if len(elements) > 13:
                    return elements[13].strip()
        raise ValueError("Invalid EDI: Missing or malformed ISA header segment.")

    def parse_to_order(self, edi_text: str) -> dict:
        """Parses raw EDI 850 loop segments into a structured dictionary."""
        po_data = {
            "poNumber": None,
            "poDate": None,
            "senderId": None,
            "receiverId": None,
            "items": []
        }
        
        segments = edi_text.split('~')
        for segment in segments:
            elements = segment.strip().split('*')
            if not elements or elements[0] == '':
                continue
                
            segment_id = elements[0]
            
            if segment_id == 'ISA':
                po_data['senderId'] = elements[6].strip()
                po_data['receiverId'] = elements[8].strip()
            elif segment_id == 'BEG':
                po_data['poNumber'] = elements[3].strip()
                po_data['poDate'] = elements[5].strip()
            elif segment_id == 'PO1':
                item = {
                    "lineNo": elements[1].strip(),
                    "quantity": int(elements[2].strip()),
                    "uom": elements[3].strip(),
                    "price": float(elements[4].strip()),
                    "sku": elements[7].strip() if len(elements) > 7 else "UNKNOWN"
                }
                po_data['items'].append(item)
                
        if not po_data['poNumber']:
            raise ValueError("Invalid EDI 850 structure: Missing BEG segment data.")
            
        return po_data

    def delivery_report(self, err, msg):
        """Optional per-message delivery callback for tracking success/failure."""
        if err is not None:
            logging.error(f"Message delivery failed: {err}")
        else:
            logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def start(self):
        self.consumer.subscribe(['edi-850-raw'])
        logging.info("EDI Streaming Pipeline Started. Listening to 'edi-850-raw'...")

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logging.error(f"Consumer Error: {msg.error()}")
                        break

                raw_edi = msg.value().decode('utf-8')
                msg_key = msg.key().decode('utf-8') if msg.key() else None

                try:
                    # 1. Deduplication Check
                    control_num = self.extract_control_number(raw_edi)
                    if control_num in DUPLICATE_STORE:
                        logging.warning(f"Duplicate transmission ignored for Control Number: {control_num}")
                        continue
                    
                    # 2. Parsing & Transformation
                    parsed_po = self.parse_to_order(raw_edi)
                    
                    # Track processed control numbers
                    DUPLICATE_STORE[control_num] = True
                    
                    # 3. Route to Parsed JSON Topic
                    json_payload = json.dumps(parsed_po)
                    self.producer.produce(
                        topic='edi-850-parsed', 
                        key=parsed_po['poNumber'], 
                        value=json_payload, 
                        callback=self.delivery_report
                    )
                    
                except Exception as e:
                    # 4. Error Handling -> Route to DLQ
                    error_payload = {
                        "error": str(e),
                        "raw_payload": raw_edi
                    }
                    logging.error(f"Failed to process message. Routing to DLQ. Error: {str(e)}")
                    self.producer.produce(
                        topic='edi-dead-letter-queue', 
                        key=msg_key, 
                        value=json.dumps(error_payload), 
                        callback=self.delivery_report
                    )
                
                # Flush producer pipeline buffer occasionally
                self.producer.poll(0)

        except KeyboardInterrupt:
            logging.info("Shutting down pipeline processing gracefully...")
        finally:
            self.consumer.close()
            self.producer.flush()

if __name__ == "__main__":
    pipeline = EdiPipelineProcessor(KAFKA_CONFIG)
    pipeline.start()