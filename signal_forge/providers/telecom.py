import ipaddress
import random
from faker.providers import BaseProvider

class TelecomProvider(BaseProvider):
    """
    Custom Faker provider for generating realistic, mathematically valid 
    telecom-related attributes.
    """

    def msisdn(self) -> str:
        """
        Generates a realistic Indian mobile number (+91 followed by 10 digits 
        starting with 9, 8, 7, or 6).
        """
        prefix = random.choice(['9', '8', '7', '6'])
        digits = "".join(random.choices("0123456789", k=9))
        return f"+91{prefix}{digits}"

    def imsi(self, mcc: str, mnc: str) -> str:
        """
        Generates a 15-digit International Mobile Subscriber Identity (IMSI)
        based on the specified Mobile Country Code (MCC) and Mobile Network Code (MNC).
        Format: MCC (3 digits) + MNC (2-3 digits) + MSIN (9-10 digits).
        """
        mcc_mnc = f"{mcc}{mnc}"
        remaining_len = 15 - len(mcc_mnc)
        msin = "".join(random.choices("0123456789", k=remaining_len))
        return f"{mcc_mnc}{msin}"

    def imei(self) -> str:
        """
        Generates a mathematically valid 15-digit International Mobile Equipment 
        Identity (IMEI) using the Luhn algorithm.
        """
        # Type Allocation Code (TAC) - first 8 digits. 
        # Example TAC ranges for common devices
        tac_prefixes = ["86820304", "35902809", "35165711", "35368409", "86438204"]
        tac = random.choice(tac_prefixes)
        
        # Serial number - next 6 digits
        serial = "".join(random.choices("0123456789", k=6))
        
        imei_14 = f"{tac}{serial}"
        
        # Calculate Luhn checksum digit
        checksum = self._calculate_luhn_checksum(imei_14)
        return f"{imei_14}{checksum}"

    def cgnat_private_ip(self) -> str:
        """
        Generates a private IPv4 address from the RFC 6598 Carrier-Grade NAT (CGNAT) 
        dedicated space: 100.64.0.0/10.
        """
        # 100.64.0.0/10 maps to 100.64.0.0 - 100.127.255.255
        octet_2 = random.randint(64, 127)
        octet_3 = random.randint(0, 255)
        octet_4 = random.randint(1, 254) # Avoid .0 and .255 for network/broadcast aesthetics
        return f"100.{octet_2}.{octet_3}.{octet_4}"

    def public_isp_ip(self, cidr_list: list) -> str:
        """
        Generates a public IPv4 address drawn from a list of CIDR subnets.
        Utilizes standard library ipaddress module to ensure mathematical validity.
        """
        if not cidr_list:
            # Fallback to random non-private IP
            return f"{random.randint(1, 223)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
        
        # Select random CIDR from the available list
        cidr = random.choice(cidr_list)
        try:
            network = ipaddress.IPv4Network(cidr)
            net_int = int(network.network_address)
            num_addresses = network.num_addresses
            
            # Avoid network/broadcast addresses
            if num_addresses <= 2:
                offset = random.randint(0, num_addresses - 1)
            else:
                offset = random.randint(1, num_addresses - 2)
                
            return str(ipaddress.IPv4Address(net_int + offset))
        except Exception:
            # Fallback in case of invalid CIDR format
            return f"{random.randint(1, 223)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"

    def _calculate_luhn_checksum(self, imei_14: str) -> int:
        """
        Calculates the Luhn algorithm checksum digit for a 14-digit sequence.
        """
        digits = [int(c) for c in imei_14]
        
        # Double the value of every second digit from the right
        # Since IMEI_14 is 14 digits, we double indices 1, 3, 5, 7, 9, 11, 13 (0-indexed odd positions)
        for i in range(1, 14, 2):
            doubled = digits[i] * 2
            if doubled > 9:
                doubled = doubled - 9
            digits[i] = doubled
            
        total = sum(digits)
        checksum = (10 - (total % 10)) % 10
        return checksum
