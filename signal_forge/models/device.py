from dataclasses import dataclass

@dataclass
class DeviceProfile:
    IMEI: str
    BRAND: str
    MODEL: str
    OS: str
    GENERATION: str  # '3G', '4G', '5G'

# Preset templates for typical devices found on Indian networks
DEVICE_TEMPLATES = [
    {"brand": "Apple", "model": "iPhone 15 Pro", "os": "iOS", "generation": "5G", "weight": 0.10},
    {"brand": "Samsung", "model": "Galaxy S24 Ultra", "os": "Android", "generation": "5G", "weight": 0.10},
    {"brand": "Xiaomi", "model": "Redmi Note 13 Pro", "os": "Android", "generation": "5G", "weight": 0.25},
    {"brand": "OnePlus", "model": "OnePlus 12", "os": "Android", "generation": "5G", "weight": 0.15},
    {"brand": "Realme", "model": "Realme C67", "os": "Android", "generation": "4G", "weight": 0.20},
    {"brand": "Jio", "model": "JioPhone Prima", "os": "KaiOS", "generation": "4G", "weight": 0.15},
    {"brand": "Nokia", "model": "Nokia 105", "os": "Series 30+", "generation": "3G", "weight": 0.05},
]
