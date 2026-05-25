import json
from confluent_kafka import Producer
from signal_forge.exporters.base import BaseExporter

class KafkaExporter(BaseExporter):
    """
    Decoupled SignalForge exporter that streams simulated records to Apache Kafka topics in real-time.
    Implements standard BaseExporter.
    """
    
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.topic = None

    def open(self, record_type: str, topic_name: str):
        """
        Initializes the confluent-kafka Producer client.
        In Kafka's context, the 'topic_name' maps to the target Kafka topic.
        """
        self.topic = topic_name
        
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': f'signal-forge-{record_type}',
            'queue.buffering.max.messages': 100000,
            # Tuning parameters for continuous low-latency streaming
            'linger.ms': 10,
            'acks': 1
        }
        
        try:
            self.producer = Producer(conf)
            print(f"[+] Kafka Producer initialized for topic '{self.topic}' (Brokers: {self.bootstrap_servers})")
        except Exception as e:
            print(f"[!] Error: Failed to initialize Kafka Producer. Detail: {e}")
            raise e

    def write_record(self, record: dict):
        if not self.producer:
            raise IOError("Kafka Exporter not opened. Call open() first.")
            
        try:
            upper_record = {k.upper(): v for k, v in record.items()}
            # Serialize dictionary to JSON string bytes
            payload = json.dumps(upper_record, ensure_ascii=False).encode('utf-8')
            
            # Send message asynchronously
            self.producer.produce(
                self.topic, 
                value=payload, 
                callback=self._delivery_report
            )
            
            # Poll to trigger completed delivery callbacks
            self.producer.poll(0)
        except Exception as e:
            print(f"[!] Error pushing record to Kafka topic '{self.topic}': {e}")

    def close(self):
        if self.producer:
            print(f"[~] Flushing remaining Kafka buffer messages for '{self.topic}'...")
            # Block until all messages in queue are dispatched
            self.producer.flush(timeout=5.0)
            self.producer = None
            self.topic = None

    def _delivery_report(self, err, msg):
        """
        Optional asynchronous callback triggered by message delivery success or failure.
        """
        if err is not None:
            print(f"[!] Kafka Delivery Warning: Message failed: {err}")
