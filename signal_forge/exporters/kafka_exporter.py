import json
from confluent_kafka import Producer
from signal_forge.exporters.base import BaseExporter

# Define Avro schemas as Python dictionaries matching the uppercase event fields.
# These will be JSON-serialized and registered with the Schema Registry.
CDR_SCHEMA_DICT = {
    "type": "record",
    "name": "CDRRecord",
    "namespace": "com.signalforge.cdr",
    "fields": [
        {"name": "RECORD_ID", "type": "string"},
        {"name": "TIMESTAMP", "type": "string"},
        {"name": "CALLING_NUM", "type": "string"},
        {"name": "CALLED_NUM", "type": "string"},
        {"name": "CALL_TYPE", "type": "string"},
        {"name": "DURATION_SEC", "type": "int"},
        {"name": "CALLER_IMSI", "type": "string"},
        {"name": "CALLER_IMEI", "type": "string"},
        {"name": "CALLER_MNO", "type": "string"},
        {"name": "BTS_ID", "type": "string"},
        {"name": "CELL_ID", "type": "int"},
        {"name": "LAC", "type": "int"},
        {"name": "STATUS", "type": "string"}
    ]
}

IPDR_SCHEMA_DICT = {
    "type": "record",
    "name": "IPDRRecord",
    "namespace": "com.signalforge.ipdr",
    "fields": [
        {"name": "RECORD_ID", "type": "string"},
        {"name": "TIMESTAMP", "type": "string"},
        {"name": "SUBSCRIBER_ID", "type": "string"},
        {"name": "USERNAME", "type": "string"},
        {"name": "MSISDN", "type": "string"},
        {"name": "IMSI", "type": "string"},
        {"name": "SOURCE_PRIVATE_IP", "type": "string"},
        {"name": "SOURCE_PRIVATE_PORT", "type": "int"},
        {"name": "SOURCE_PUBLIC_IP", "type": "string"},
        {"name": "SOURCE_PUBLIC_PORT", "type": "int"},
        {"name": "DESTINATION_IP", "type": "string"},
        {"name": "DESTINATION_PORT", "type": "int"},
        {"name": "PROTOCOL", "type": "string"},
        {"name": "BYTES_SENT", "type": "long"},
        {"name": "BYTES_RECEIVED", "type": "long"},
        {"name": "PROVIDER_NAME", "type": "string"},
        {"name": "ACCESS_TYPE", "type": "string"}
    ]
}

class KafkaExporter(BaseExporter):
    """
    Decoupled SignalForge exporter that streams simulated records to Apache Kafka topics in real-time.
    Implements standard BaseExporter.
    """
    
    def __init__(self, bootstrap_servers: str, schema_registry_url: str = None):
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.producer = None
        self.topic = None
        self.record_type = None
        
        # Schema Registry structures
        self.schema_registry_client = None
        self.serializer = None

    def open(self, record_type: str, topic_name: str):
        """
        Initializes the confluent-kafka Producer client.
        In Kafka's context, the 'topic_name' maps to the target Kafka topic.
        """
        self.topic = topic_name
        self.record_type = record_type
        
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

        # Lazily load and configure Schema Registry if an endpoint is provided.
        if self.schema_registry_url:
            try:
                from confluent_kafka.schema_registry import SchemaRegistryClient
                from confluent_kafka.schema_registry.avro import AvroSerializer
                
                sr_conf = {'url': self.schema_registry_url}
                self.schema_registry_client = SchemaRegistryClient(sr_conf)
                
                if record_type.lower() == "cdr":
                    schema_dict = CDR_SCHEMA_DICT
                elif record_type.lower() == "ipdr":
                    schema_dict = IPDR_SCHEMA_DICT
                else:
                    raise ValueError(f"Unsupported record type for Schema Registry: {record_type}")
                
                schema_str = json.dumps(schema_dict)
                
                # Identity lambda is used as to_dict because record fields are already mapped to schema
                self.serializer = AvroSerializer(
                    self.schema_registry_client,
                    schema_str,
                    lambda record, ctx: record
                )
                print(f"[+] Schema Registry active (URL: {self.schema_registry_url}). Registered schema for '{record_type}'")
            except Exception as e:
                print(f"[!] Error: Failed to initialize Schema Registry serializer for topic '{self.topic}'. Detail: {e}")
                raise e

    def write_record(self, record: dict):
        if not self.producer:
            raise IOError("Kafka Exporter not opened. Call open() first.")
            
        try:
            # Guarantee uppercase key conversion
            upper_record = {k.upper(): v for k, v in record.items()}
            
            # 1. Determine Partition Key (e.g. MSISDN/CALLING_NUM, or RECORD_ID)
            partition_key = upper_record.get("CALLING_NUM") or upper_record.get("MSISDN") or upper_record.get("RECORD_ID")
            key_bytes = str(partition_key).encode('utf-8') if partition_key is not None else None
            
            # 2. Construct Metadata Headers
            headers = []
            
            # Record Type
            if "CALL_TYPE" in upper_record:
                headers.append(("record_type", b"CDR"))
            else:
                headers.append(("record_type", b"IPDR"))
                
            # Operator / Provider
            operator = upper_record.get("CALLER_MNO") or upper_record.get("PROVIDER_NAME")
            if operator:
                headers.append(("operator", str(operator).encode('utf-8')))
                
            # Timestamp
            timestamp = upper_record.get("TIMESTAMP")
            if timestamp:
                headers.append(("timestamp", str(timestamp).encode('utf-8')))
                
            # Serialize payload: Avro (if schema registry active) vs JSON
            if self.serializer:
                from confluent_kafka.serialization import MessageField, SerializationContext
                ctx = SerializationContext(self.topic, MessageField.VALUE)
                payload = self.serializer(upper_record, ctx)
                headers.append(("content_format", b"AVRO"))
            else:
                payload = json.dumps(upper_record, ensure_ascii=False).encode('utf-8')
                headers.append(("content_format", b"JSON"))
            
            # Send message asynchronously with Key and Headers!
            self.producer.produce(
                self.topic, 
                value=payload,
                key=key_bytes,
                headers=headers,
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
            self.schema_registry_client = None
            self.serializer = None

    def _delivery_report(self, err, msg):
        """
        Optional asynchronous callback triggered by message delivery success or failure.
        """
        if err is not None:
            print(f"[!] Kafka Delivery Warning: Message failed: {err}")
