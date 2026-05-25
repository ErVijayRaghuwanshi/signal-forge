from dataclasses import dataclass

@dataclass
class BTSProfile:
    BTS_ID: str
    CELL_ID: int
    LAC: int
    MNO: str          # Jio, Airtel, Vodafone_Idea, BSNL
    LATITUDE: float
    LONGITUDE: float
    GENERATION: str   # '3G', '4G', '5G'
    CAPACITY: int     # Max concurrent connections supported by the tower
