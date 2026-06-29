import os
import sys
import time
import argparse
from datetime import datetime, timedelta
from signal_forge.generators.cdr import CDRGenerator
from signal_forge.generators.ipdr import IPDRGenerator
from signal_forge.exporters.csv_exporter import CSVExporter
from signal_forge.exporters.json_exporter import JSONExporter

# Import KafkaExporter safely to prevent failure if confluent-kafka is not installed locally
try:
    from signal_forge.exporters.kafka_exporter import KafkaExporter
except ImportError:
    KafkaExporter = None

def parse_args():
    parser = argparse.ArgumentParser(
        description="SignalForge: Extensible Synthetic Telecom CDR and IPDR Log Generator"
    )
    # Common settings
    parser.add_argument(
        "--users", type=int, default=100, help="Size of the user subscriber pool (default: 100)"
    )
    parser.add_argument(
        "--towers", type=int, default=15, help="Number of BTS towers in the grid (default: 15)"
    )
    parser.add_argument(
        "--records", type=int, default=2000, help="Maximum target record limit (default: 2000)"
    )
    parser.add_argument(
        "--region", type=str, default="Mumbai", choices=["Mumbai", "Delhi_NCR", "Bengaluru"],
        help="Target geographic city boundaries for cell tower positions (default: Mumbai)"
    )
    parser.add_argument(
        "--format", type=str, default="csv", choices=["csv", "json", "kafka"],
        help="Export target format: file-based (csv, json) or stream-based (kafka) (default: csv)"
    )
    parser.add_argument(
        "--output-dir", type=str, default="./output", help="Output folder destination (default: ./output)"
    )
    parser.add_argument(
        "--config", type=str, default="config.yaml", help="Path to configuration yaml (default: config.yaml)"
    )
    
    # Batch settings
    parser.add_argument(
        "--days", type=int, default=1, help="Backfill duration in days for batch simulation (default: 1)"
    )
    
    # Streaming/Daemon settings
    parser.add_argument(
        "--stream", action="store_true", help="Enable infinite continuous streaming daemon mode"
    )
    parser.add_argument(
        "--speed", type=float, default=60.0,
        help="Simulation speed multiplier. e.g. 60.0 means 1 hour simulated in 1 minute of wall time (default: 60.0)"
    )
    parser.add_argument(
        "--kafka-bootstrap", type=str,
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Bootstrap servers for Kafka Exporter (default: KAFKA_BOOTSTRAP_SERVERS env var or localhost:9092)"
    )
    parser.add_argument(
        "--kafka-schema-registry", type=str,
        default=os.environ.get("KAFKA_SCHEMA_REGISTRY_URL"),
        help="Schema Registry URL for Kafka Exporter (default: KAFKA_SCHEMA_REGISTRY_URL env var or None)"
    )
    parser.add_argument(
        "--kafka-cdr-topic", type=str,
        default=os.environ.get("KAFKA_CDR_TOPIC", "cdr-records"),
        help="Kafka topic for CDR events (default: KAFKA_CDR_TOPIC env var or cdr-records)"
    )
    parser.add_argument(
        "--kafka-ipdr-topic", type=str,
        default=os.environ.get("KAFKA_IPDR_TOPIC", "ipdr-records"),
        help="Kafka topic for IPDR events (default: KAFKA_IPDR_TOPIC env var or ipdr-records)"
    )
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Load configuration file for default values and overrides
    import yaml
    config_data = {}
    if os.path.exists(args.config):
        try:
            with open(args.config, 'r') as f:
                config_data = yaml.safe_load(f) or {}
        except Exception as e:
            print(f"[!] Warning: Failed to parse config file '{args.config}': {e}")
            
    schema_registry_url = args.kafka_schema_registry or config_data.get("kafka", {}).get("schema_registry_url")
    if schema_registry_url and str(schema_registry_url).lower().strip() in ("none", "null", "false", ""):
        schema_registry_url = None
    
    print("\n" + "="*60)
    print("📡 SIGNALFORGE SYNTHETIC LOG ENGINE INITIALIZATION")
    print("="*60)
    print(f"[-] Config Path : {args.config}")
    print(f"[-] Region      : {args.region}")
    print(f"[-] Users Pool  : {args.users}")
    print(f"[-] Towers Pool : {args.towers}")
    print(f"[-] Format      : {args.format.upper()}")
    
    if args.stream:
        print(f"[-] Running Mode: CONTINUOUS STREAMING SERVICE")
        print(f"[-] Speed Scale : {args.speed}x (1 simulated minute = {60/args.speed:.2f} real seconds)")
        if args.format == "kafka":
            print(f"[-] Kafka Broker: {args.kafka_bootstrap}")
            print(f"[-] Registry URL: {schema_registry_url if schema_registry_url else 'None (JSON format)'}")
            print(f"[-] CDR Topic   : {args.kafka_cdr_topic}")
            print(f"[-] IPDR Topic  : {args.kafka_ipdr_topic}")
    else:
        print(f"[-] Running Mode: BATCH BACKFILL")
        print(f"[-] Time Range  : {args.days} Days backfill")
        print(f"[-] Output Dir  : {args.output_dir}")
        if args.format == "kafka":
            print(f"[-] Kafka Broker: {args.kafka_bootstrap}")
            print(f"[-] Registry URL: {schema_registry_url if schema_registry_url else 'None (JSON format)'}")
            print(f"[-] CDR Topic   : {args.kafka_cdr_topic}")
            print(f"[-] IPDR Topic  : {args.kafka_ipdr_topic}")
    print("="*60 + "\n")
    
    if not os.path.exists(args.config):
        print(f"[!] Error: Configuration file '{args.config}' not found.")
        return

    # 1. Instantiate modular exporters
    if args.format == "kafka":
        if not KafkaExporter:
            print("[!] Error: 'confluent-kafka' package is required for kafka export but not installed.")
            sys.exit(1)
        cdr_exporter = KafkaExporter(args.kafka_bootstrap, schema_registry_url=schema_registry_url)
        ipdr_exporter = KafkaExporter(args.kafka_bootstrap, schema_registry_url=schema_registry_url)
        cdr_target = args.kafka_cdr_topic
        ipdr_target = args.kafka_ipdr_topic
    elif args.format == "csv":
        cdr_exporter = CSVExporter()
        ipdr_exporter = CSVExporter()
        cdr_target = args.output_dir
        ipdr_target = args.output_dir
    else:
        cdr_exporter = JSONExporter()
        ipdr_exporter = JSONExporter()
        cdr_target = args.output_dir
        ipdr_target = args.output_dir

    # 2. Build coherent relational pools
    print("[+] Building Relational Pools...")
    cdr_gen = CDRGenerator(args.config)
    cdr_gen.initialize_pools(num_users=args.users, num_towers=args.towers, region_name=args.region)
    
    ipdr_gen = IPDRGenerator(args.config)
    ipdr_gen.bts_pool = cdr_gen.bts_pool
    ipdr_gen.user_pool = cdr_gen.user_pool

    # Open exporters
    cdr_exporter.open("cdr", cdr_target)
    ipdr_exporter.open("ipdr", ipdr_target)

    # 3. Execution Pipeline
    try:
        if args.stream:
            # === CONTINUOUS STREAMING SERVICE MODE ===
            # Simulate real-time streaming, advancing step_minutes every sleep cycle
            step_minutes = 15
            tick = datetime.now()
            
            # Compute pacing (ticks duration / speed)
            sleep_duration = (step_minutes * 60.0) / args.speed
            
            print(f"[+] Starting stream. Pacing: sleeping {sleep_duration:.2f} seconds between simulated ticks...")
            print("[!] Press Ctrl+C to terminate the service.")
            print("-"*60)
            
            stream_count = 0
            while True:
                # Format a console telemetry status line
                print(f"[~] Simulating Tick: {tick.strftime('%Y-%m-%d %H:%M:%S')} (Speed: {args.speed}x)")
                
                # CDR and IPDR are generated simultaneously inside the same timestamp tick!
                cdr_records_sent = 0
                for record in cdr_gen.generate_records(tick, tick, step_minutes=step_minutes):
                    cdr_exporter.write_record(record)
                    cdr_records_sent += 1
                
                ipdr_records_sent = 0
                for record in ipdr_gen.generate_records(tick, tick, step_minutes=step_minutes):
                    ipdr_exporter.write_record(record)
                    ipdr_records_sent += 1
                
                stream_count += 1
                print(f"    ↳ Streamed: {cdr_records_sent} CDR events | {ipdr_records_sent} IPDR events")
                
                # Advance simulated clock
                tick += timedelta(minutes=step_minutes)
                
                # Maintain real-time clock pacing
                time.sleep(sleep_duration)
                
        else:
            # === BATCH BACKFILL MODE ===
            end_date = datetime.now()
            start_date = end_date - timedelta(days=args.days)
            
            print("[+] Generating CDR batch records...")
            cdr_count = 0
            for record in cdr_gen.generate_records(start_date, end_date):
                cdr_exporter.write_record(record)
                cdr_count += 1
                if cdr_count >= args.records:
                    break
            print(f"[✓] Generated {cdr_count} CDR logs.")
            
            print("[+] Generating IPDR batch records...")
            ipdr_count = 0
            for record in ipdr_gen.generate_records(start_date, end_date):
                ipdr_exporter.write_record(record)
                ipdr_count += 1
                if ipdr_count >= args.records:
                    break
            print(f"[✓] Generated {ipdr_count} IPDR/CGNAT logs.")
            
            print("="*60)
            print("✨ BATCH SYNTHETIC DATA GENERATION SUCCESSFUL")
            print(f"[-] Records saved inside: {os.path.abspath(args.output_dir)}")
            print("="*60 + "\n")

    except KeyboardInterrupt:
        print("\n[!] Stream service interrupted by user. Shutting down gracefully...")
    finally:
        # Guarantee exporters close resource handles safely
        cdr_exporter.close()
        ipdr_exporter.close()

if __name__ == "__main__":
    main()
