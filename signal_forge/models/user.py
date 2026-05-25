from dataclasses import dataclass
from enum import Enum
from signal_forge.models.device import DeviceProfile
from signal_forge.models.bts import BTSProfile

class UserPersona(Enum):
    STANDARD = {
        "call_rate": 0.15,      # probability of call per hourly tick
        "sms_rate": 0.25,       # probability of sms per hourly tick
        "data_rate": 0.40,      # probability of starting web session per tick
        "active_hours": range(8, 23), # 8 AM to 11 PM
        "data_vol_range": (10, 200)   # MB per session
    }
    HEAVY_CALLER = {
        "call_rate": 0.50,
        "sms_rate": 0.40,
        "data_rate": 0.15,
        "active_hours": range(9, 21), # Business hours
        "data_vol_range": (1, 30)
    }
    DATA_CONSUMER = {
        "call_rate": 0.05,
        "sms_rate": 0.10,
        "data_rate": 0.75,
        "active_hours": range(7, 24),
        "data_vol_range": (100, 2048)  # Up to 2GB per session
    }
    NIGHT_OWL = {
        "call_rate": 0.08,
        "sms_rate": 0.30,
        "data_rate": 0.60,
        "active_hours": list(range(21, 24)) + list(range(0, 5)), # 9 PM to 5 AM
        "data_vol_range": (50, 1500)
    }

@dataclass
class UserProfile:
    SUBSCRIBER_ID: str
    NAME: str
    EMAIL: str
    MSISDN: str
    IMSI: str
    MNO: str                  # Jio, Airtel, Vodafone_Idea, BSNL
    BROADBAND_ISP: str        # ACT_Fibernet, Hathway, Tata_Play_Fiber, or None
    DEVICE: DeviceProfile
    HOME_BTS: BTSProfile
    PERSONA: UserPersona
    PRIVATE_IP: str           # Pre-assigned CGNAT IP
