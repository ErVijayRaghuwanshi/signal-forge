import os
import csv
from signal_forge.exporters.base import BaseExporter

class CSVExporter(BaseExporter):
    """
    Exports simulated records to structured standard CSV files.
    """
    
    def __init__(self):
        self.file_handle = None
        self.writer = None
        self.headers_written = False

    def open(self, record_type: str, output_dir: str):
        os.makedirs(output_dir, exist_ok=True)
        filename = f"{record_type}_records.csv"
        file_path = os.path.join(output_dir, filename)
        
        self.file_handle = open(file_path, 'w', newline='', encoding='utf-8')
        self.headers_written = False
        
    def write_record(self, record: dict):
        if not self.file_handle:
            raise IOError("Exporter not opened. Call open() first.")
            
        upper_record = {k.upper(): v for k, v in record.items()}
            
        if not self.headers_written:
            # Dynamically discover headers from the first record dict keys
            fieldnames = list(upper_record.keys())
            self.writer = csv.DictWriter(self.file_handle, fieldnames=fieldnames)
            self.writer.writeheader()
            self.headers_written = True
            
        self.writer.writerow(upper_record)
        
    def close(self):
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None
            self.writer = None
            self.headers_written = False
