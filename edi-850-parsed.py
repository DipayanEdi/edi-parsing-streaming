import json
import logging
from confluent_kafka import Consumer, KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka Configuration
CONSUMER_CONFIG = {
    'bootstrap.servers': 'localhost:9094',
    'group.id': 'erp-fulfillment-group',    # Distinct consumer group for ERP business logic
    'auto.offset.reset': 'earliest',         # Read from the start if no offsets are committed
    'enable.auto.commit': False,             # Manual commits for business transaction safety
}

class EdiParsedConsumer:
    def __init__(self, config):
        self.consumer = Consumer(config)
        self.topic = 'edi-850-parsed'

    def update_internal_erp(self, purchase_order_data: dict) -> bool:
        """
        Simulates pushing the clean JSON data into an ERP system API or Database.
        """
        po_number = purchase_order_data.get("poNumber")
        sender = purchase_order_data.get("senderId")
        items_count = len(purchase_order_data.get("items", []))
        
        logging.info(f"[ERP SYNC] Processing PO #{po_number} received from Partner: {sender}")
        
        # Iterating through the clean, typed JSON array natively
        for item in purchase_order_data.get("items", []):
            logging.info(f"   -> Allocating Inventory: SKU {item['sku']} | Qty: {item['quantity']} | Unit Price: ${item['price']}")
        
        # Simulate successful database write
        logging.info(f"[ERP SYNC] Successfully committed PO #{po_number} with {items_count} line items to ERP Ledger.\n")
        return True

    def start(self):
        self.consumer.subscribe([self.topic])
        logging.info(f"ERP Integration Engine listening for structured data on '{self.topic}'...")

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logging.error(f"Kafka Consumer Error: {msg.error()}")
                        break

                # Decode the value (which is clean JSON sent by the pipeline processor)
                raw_json_str = msg.value().decode('utf-8')
                
                try:
                    # Parse the string natively into a Python dictionary
                    po_payload = json.loads(raw_json_str)
                    
                    # Execute business actions
                    success = self.update_internal_erp(po_payload)
                    
                    if success:
                        # Only commit Kafka offsets AFTER your downstream database/ERP confirms saving the data.
                        # This guarantees At-Least-Once delivery and eliminates data-loss risks.
                        self.consumer.commit(asynchronous=False)
                        
                except json.JSONDecodeError:
                    logging.error(f"Received malformed JSON on parsed topic. Value: {raw_json_str}")
                    # In production, you might commit anyway to skip a broken message, or route to an alert.
                    self.consumer.commit(asynchronous=False)
                except Exception as biz_error:
                    logging.error(f"Downstream system rejected transaction. Halting pipeline: {str(biz_error)}")
                    # Do not commit offsets here if your database is down, allowing the engine to retry when recovered.

        except KeyboardInterrupt:
            logging.info("Gracefully stopping ERP Integration Engine...")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    integration_engine = EdiParsedConsumer(CONSUMER_CONFIG)
    integration_engine.start()