from abc import ABC, abstractmethod

class BaseExporter(ABC):
    """
    Abstract Base Class for all SignalForge record exporters.
    Subclass this interface to support new sinks (such as Kafka, database sinks, Parquet files).
    """
    
    @abstractmethod
    def open(self, record_type: str, output_dir: str):
        """
        Initializes the exporter context (e.g., opens file handles or network sockets).
        :param record_type: 'cdr' or 'ipdr'
        :param output_dir: destination path
        """
        pass

    @abstractmethod
    def write_record(self, record: dict):
        """
        Pipes a single simulated record dictionary to the target sink.
        """
        pass

    @abstractmethod
    def close(self):
        """
        Performs cleanups, flushes buffers, and closes handles.
        """
        pass
