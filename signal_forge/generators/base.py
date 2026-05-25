import yaml
import random
from datetime import datetime, timedelta
from faker import Faker
from signal_forge.providers.telecom import TelecomProvider
from signal_forge.models.device import DeviceProfile, DEVICE_TEMPLATES
from signal_forge.models.bts import BTSProfile
from signal_forge.models.user import UserProfile, UserPersona

class BaseGenerator:
    """
    Base generator managing configuration parsing, Faker bootstrap, 
    and relational pool setups for users, BTS towers, and devices.
    """
    
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        
        # Initialize Faker with telecom provider
        self.fake = Faker()
        self.fake.add_provider(TelecomProvider)
        
        # Pools
        self.bts_pool = []
        self.user_pool = []
        
    def _load_config(self, config_path: str) -> dict:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
            
    def initialize_pools(self, num_users: int, num_towers: int, region_name: str = "Mumbai"):
        """
        Builds the physical network grid and the user database relational profiles.
        """
        region = self.config["simulation"]["geo_regions"].get(region_name)
        if not region:
            region = self.config["simulation"]["geo_regions"]["Mumbai"]
            
        mno_dist = self.config["simulation"]["mno_distribution"]
        isp_dist = self.config["simulation"]["isp_distribution"]
        
        # 1. Generate BTS (Cell Tower) Pool
        operators = list(mno_dist.keys())
        op_weights = list(mno_dist.values())
        
        lac_counter = 1000
        for i in range(num_towers):
            bts_id = f"BTS-{region_name[:3].upper()}-{i+1:03d}"
            cell_id = random.randint(10000, 99999)
            lac = lac_counter + (i // 5) # Group towers in LACs
            
            # Select operator based on market share weights
            mno = random.choices(operators, weights=op_weights, k=1)[0]
            
            # Random coordinates in the city boundary
            lat = random.uniform(region["lat_min"], region["lat_max"])
            lon = random.uniform(region["lon_min"], region["lon_max"])
            
            gen = random.choice(["4G", "5G"]) if mno in ["Jio", "Airtel"] else random.choice(["3G", "4G"])
            capacity = 1000 if gen == "5G" else (500 if gen == "4G" else 200)
            
            bts = BTSProfile(
                BTS_ID=bts_id,
                CELL_ID=cell_id,
                LAC=lac,
                MNO=mno,
                LATITUDE=lat,
                LONGITUDE=lon,
                GENERATION=gen,
                CAPACITY=capacity
            )
            self.bts_pool.append(bts)
            
        # 2. Generate User Avatars Pool
        isp_names = list(isp_dist.keys())
        isp_weights = list(isp_dist.values())
        
        personas = list(UserPersona)
        persona_weights = [0.45, 0.20, 0.25, 0.10] # Standard, Heavy, Data, Night Owl
        
        # Setup device templates
        devices = DEVICE_TEMPLATES
        dev_weights = [d["weight"] for d in devices]
        
        for i in range(num_users):
            sub_id = f"SUB-{i+1:06d}"
            name = self.fake.name()
            email = self.fake.free_email()
            
            # Select Operator (MNO)
            mno = random.choices(operators, weights=op_weights, k=1)[0]
            msisdn = self.fake.msisdn()
            
            # Assign IMSI according to MCC/MNC
            # India MCC is 404/405. Standard mapping:
            mcc = "405"
            mnc = "840" if mno == "Jio" else ("45" if mno == "Airtel" else ("18" if mno == "Vodafone_Idea" else "07"))
            imsi = self.fake.imsi(mcc, mnc)
            
            # Broadband mapping (60% chance of having home broadband)
            has_broadband = random.random() < 0.60
            broadband = random.choices(isp_names, weights=isp_weights, k=1)[0] if has_broadband else "None"
            
            # Instantiate Device profile
            dev_tpl = random.choices(devices, weights=dev_weights, k=1)[0]
            device = DeviceProfile(
                IMEI=self.fake.imei(),
                BRAND=dev_tpl["brand"],
                MODEL=dev_tpl["model"],
                OS=dev_tpl["os"],
                GENERATION=dev_tpl["generation"]
            )
            
            # Match user home BTS tower belonging to their operator
            op_towers = [b for b in self.bts_pool if b.MNO == mno]
            home_bts = random.choice(op_towers) if op_towers else random.choice(self.bts_pool)
            
            # Assign Persona & Private CGNAT IP
            persona = random.choices(personas, weights=persona_weights, k=1)[0]
            private_ip = self.fake.cgnat_private_ip()
            
            user = UserProfile(
                SUBSCRIBER_ID=sub_id,
                NAME=name,
                EMAIL=email,
                MSISDN=msisdn,
                IMSI=imsi,
                MNO=mno,
                BROADBAND_ISP=broadband,
                DEVICE=device,
                HOME_BTS=home_bts,
                PERSONA=persona,
                PRIVATE_IP=private_ip
            )
            self.user_pool.append(user)
            
    def get_time_series_ticks(self, start_date: datetime, end_date: datetime, step_minutes: int = 15):
        """
        Yields chronological datetime objects between two timestamps.
        """
        current = start_date
        while current <= end_date:
            yield current
            current += timedelta(minutes=step_minutes)
