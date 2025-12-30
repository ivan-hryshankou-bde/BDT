from dataclasses import dataclass
from datetime import datetime

@dataclass
class Alert:
    """Class holding parameters for alerting message"""
    rule_name: str
    message: str
    timestamp: datetime