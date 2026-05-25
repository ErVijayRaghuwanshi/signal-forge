import random
from datetime import datetime, timedelta
from signal_forge.generators.base import BaseGenerator

# Static preset pool of common Indian internet destinations to simulate realistic traffic
DESTINATION_SERVICES = [
    {"name": "YouTube", "domain": "youtube.com", "ips": ["172.217.16.142", "142.250.190.46"], "port": 443, "protocol": "TCP", "weight": 0.30},
    {"name": "Instagram", "domain": "instagram.com", "ips": ["157.240.23.174", "157.240.13.35"], "port": 443, "protocol": "TCP", "weight": 0.20},
    {"name": "WhatsApp", "domain": "whatsapp.net", "ips": ["31.13.88.49", "157.240.16.53"], "port": 443, "protocol": "TCP", "weight": 0.25},
    {"name": "Hotstar", "domain": "hotstar.com", "ips": ["23.212.248.51", "23.212.248.60"], "port": 443, "protocol": "TCP", "weight": 0.10},
    {"name": "NPCI_UPI", "domain": "upi.npci.org.in", "ips": ["103.14.161.45"], "port": 443, "protocol": "TCP", "weight": 0.08},
    {"name": "Google_DNS", "domain": "dns.google", "ips": ["8.8.8.8", "8.8.4.4"], "port": 53, "protocol": "UDP", "weight": 0.07},
]

class IPDRGenerator(BaseGenerator):
    """
    Simulates IP Detail Records (IPDR) and Carrier-Grade NAT (CGNAT) allocation logs.
    """
    
    def generate_records(self, start_date: datetime, end_date: datetime, step_minutes: int = 15):
        """
        Generates chronologically aligned IPDR records using configured ISP/MNO IP allocations.
        """
        for tick in self.get_time_series_ticks(start_date, end_date, step_minutes):
            hour = tick.hour
            shuffled_users = list(self.user_pool)
            random.shuffle(shuffled_users)
            
            for user in shuffled_users:
                persona_val = user.PERSONA.value
                
                # Check active hours
                if hour not in persona_val["active_hours"]:
                    continue
                
                # Scale activity probabilities for the tick duration
                tick_factor = step_minutes / 60.0
                data_prob = persona_val["data_rate"] * tick_factor
                
                if random.random() < data_prob:
                    record = self._simulate_data_session(tick, user)
                    if record:
                        yield record

    def _simulate_data_session(self, tick: datetime, user) -> dict:
        # Determine routing path: Home Broadband or Cellular Data
        # If user has a broadband ISP, they have an 80% chance of using it during off-hours (e.g. evening)
        is_broadband = False
        provider_name = user.MNO  # Default to MNO for cellular data
        
        if user.BROADBAND_ISP != "None":
            # Evening or morning hours at home: route over broadband
            if tick.hour >= 18 or tick.hour <= 9:
                is_broadband = True
                provider_name = user.BROADBAND_ISP
                
        # Draw public IP corresponding to the selected provider's allocated subnets
        cidr_pool = self.config["public_ip_pools"].get(provider_name, [])
        source_public_ip = self.fake.public_isp_ip(cidr_pool)
        
        # CGNAT Port Translation logic
        # Ephemeral private ports (usually 49152 - 65535)
        source_private_port = random.randint(49152, 65535)
        # Translated public ports (usually 1024 - 49151)
        source_public_port = random.randint(1024, 49151)
        
        # Select target service destination
        services = DESTINATION_SERVICES
        weights = [s["weight"] for s in services]
        service = random.choices(services, weights=weights, k=1)[0]
        
        destination_ip = random.choice(service["ips"])
        destination_port = service["port"]
        protocol = service["protocol"]
        
        # Calculate session parameters
        random_offset_sec = random.randint(0, (15 * 60) - 1)
        session_time = tick + timedelta(seconds=random_offset_sec)
        
        # Bytes volume generated based on persona data bounds
        vol_range = user.PERSONA.value["data_vol_range"]
        # Convert MB to bytes
        total_mb = random.uniform(vol_range[0], vol_range[1])
        
        # Large streams (e.g., YouTube/Hotstar) have high download ratios (e.g., 95% down, 5% up)
        # DNS has balanced small transactions
        if service["name"] in ["YouTube", "Hotstar"]:
            ratio = random.uniform(0.92, 0.97)
        elif service["name"] == "NPCI_UPI":
            ratio = random.uniform(0.40, 0.60)
        else:
            ratio = random.uniform(0.80, 0.90)
            
        bytes_received = int(total_mb * 1024 * 1024 * ratio)
        bytes_sent = int(total_mb * 1024 * 1024 * (1 - ratio))
        
        return {
            "record_id": f"IPD-{user.SUBSCRIBER_ID[:3]}-{random.randint(100000, 999999)}",
            "timestamp": session_time.isoformat(),
            "subscriber_id": user.SUBSCRIBER_ID,
            "username": user.EMAIL.split('@')[0],
            "msisdn": user.MSISDN,
            "imsi": user.IMSI if not is_broadband else "N/A (BROADBAND)",
            "source_private_ip": user.PRIVATE_IP if not is_broadband else f"192.168.1.{random.randint(2, 254)}",
            "source_private_port": source_private_port,
            "source_public_ip": source_public_ip,
            "source_public_port": source_public_port,
            "destination_ip": destination_ip,
            "destination_port": destination_port,
            "protocol": protocol,
            "bytes_sent": bytes_sent,
            "bytes_received": bytes_received,
            "provider_name": provider_name.replace("_", " "),
            "access_type": "BROADBAND" if is_broadband else "CELLULAR"
        }
