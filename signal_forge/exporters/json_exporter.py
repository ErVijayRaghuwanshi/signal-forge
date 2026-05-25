import os
import json
from signal_forge.exporters.base import BaseExporter

class JSONExporter(BaseExporter):
    """
    Exports simulated records to JSON Lines (JSONL) format.
    Each record is written as a self-contained, single-line JSON string.
    """
    
    def __init__(self):
        self.file_handle = None

    def open(self, record_type: str, output_dir: str):
        os.makedirs(output_dir, exist_ok=True)
        filename = f"{record_type}_records.jsonl"
        file_path = os.path.join(output_dir, filename)
        
        self.file_handle = open(file_path, 'w', encoding='utf-8')
        
    def write_record(self, record: dict):
        if not self.file_handle:
            raise IOError("Exporter not opened. Call open() first.")
            
        upper_record = {k.upper(): v for k, v in record.items()}
        json_line = json.dumps(upper_record, ensure_ascii=False)
        self.file_handle.write(json_line + '\n')
        
    def close(self):
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None
