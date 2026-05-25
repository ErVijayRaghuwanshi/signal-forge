import random
import math
from datetime import datetime, timedelta
from signal_forge.generators.base import BaseGenerator

class CDRGenerator(BaseGenerator):
    """
    Simulates Call Detail Records (CDRs) for voice calls and SMS logs.
    """
    
    def generate_records(self, start_date: datetime, end_date: datetime, step_minutes: int = 15):
        """
        Runs the simulation tick-by-tick and yields generated CDR logs in chronological order.
        """
        for tick in self.get_time_series_ticks(start_date, end_date, step_minutes):
            hour = tick.hour
            
            # Simple BTS load tracker for the current tick
            bts_usage = {bts.BTS_ID: 0 for bts in self.bts_pool}
            
            # Shuffling users ensures no deterministic execution order
            shuffled_users = list(self.user_pool)
            random.shuffle(shuffled_users)
            
            for user in shuffled_users:
                persona_val = user.PERSONA.value
                
                # 1. Check if user is active during this hour
                if hour not in persona_val["active_hours"]:
                    continue
                
                # Scale activity probabilities for the tick duration
                tick_factor = step_minutes / 60.0
                call_prob = persona_val["call_rate"] * tick_factor
                sms_prob = persona_val["sms_rate"] * tick_factor
                
                # Check for call event
                if random.random() < call_prob:
                    record = self._simulate_event(tick, user, "VOICE", bts_usage)
                    if record:
                        yield record
                        
                # Check for SMS event
                elif random.random() < sms_prob:
                    record = self._simulate_event(tick, user, "SMS", bts_usage)
                    if record:
                        yield record

    def _simulate_event(self, tick: datetime, user, event_type: str, bts_usage: dict) -> dict:
        # Determine the BTS tower routing the event
        # 90% chance user is at home, 10% chance roaming/at nearby tower
        mno_towers = [b for b in self.bts_pool if b.MNO == user.MNO]
        if not mno_towers:
            mno_towers = self.bts_pool
            
        if random.random() < 0.90:
            bts = user.HOME_BTS
        else:
            bts = random.choice(mno_towers)
            
        # Check cell site capacity
        if bts_usage[bts.BTS_ID] >= bts.CAPACITY:
            status = "CONGESTION_FAILURE"
        else:
            bts_usage[bts.BTS_ID] += 1
            status = "SUCCESS"
            
        # Pick recipient (callee)
        # 80% chance it's another simulated user in the pool, 20% external call
        if random.random() < 0.80:
            recipient = random.choice(self.user_pool)
            while recipient.SUBSCRIBER_ID == user.SUBSCRIBER_ID:
                recipient = random.choice(self.user_pool)
            called_num = recipient.MSISDN
        else:
            # Generate external mock phone number
            called_num = self.fake.msisdn()
            
        # Event details
        random_offset_sec = random.randint(0, (15 * 60) - 1)
        event_time = tick + timedelta(seconds=random_offset_sec)
        
        duration = 0
        if event_type == "VOICE":
            if status == "SUCCESS":
                # Lognormal distribution represents phone call lengths beautifully
                # e.g., mean of exp(5.2) ~ 180 seconds, variance 0.8
                duration = int(random.lognormvariate(5.2, 0.8))
                duration = max(3, min(duration, 3600)) # Clamped between 3s and 1 hour
                
                # Check call response status
                resp = random.random()
                if resp < 0.05:
                    status = "BUSY"
                    duration = 0
                elif resp < 0.10:
                    status = "NO_ANSWER"
                    duration = 0
            else:
                duration = 0
                
        return {
            "record_id": f"CDR-{user.SUBSCRIBER_ID[:3]}-{random.randint(100000, 999999)}",
            "timestamp": event_time.isoformat(),
            "calling_num": user.MSISDN,
            "called_num": called_num,
            "call_type": event_type,
            "duration_sec": duration,
            "caller_imsi": user.IMSI,
            "caller_imei": user.DEVICE.IMEI,
            "caller_mno": user.MNO,
            "bts_id": bts.BTS_ID,
            "cell_id": bts.CELL_ID,
            "lac": bts.LAC,
            "status": status
        }
