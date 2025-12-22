from dataclasses import dataclass
from datetime import datetime

@dataclass
class Alert:
    rule_name: str
    message: str
    timestamp: datetime