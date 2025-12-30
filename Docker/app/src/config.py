from dataclasses import dataclass

@dataclass
class Config:
    """Class holding configuration parameters for executing the module"""
    CHUNK_SIZE: int
    DATE_COLUMN: str
    ERROR_COLUMN: str
    ERROR_LEVEL: str
    
    THRESHOLD: int
    TIME: str
    TIME_ATTR: str
    ATTRIBUTE: str

    COLUMNS: list[str]

config = Config(
    CHUNK_SIZE = 500000,
    DATE_COLUMN = "date",
    ERROR_COLUMN = "severity",
    ERROR_LEVEL = "Error",

    THRESHOLD = 10,
    TIME = "1min",
    TIME_ATTR = "1h",
    ATTRIBUTE = "bundle_id",

    COLUMNS = [
        "error_code",
        "error_message",
        "severity",
        "log_location",
        "mode",
        "model",
        "graphics",
        "session_id",
        "sdkv",
        "test_mode",
        "flow_id",
        "flow_type",
        "sdk_date",
        "publisher_id",
        "game_id",
        "bundle_id",
        "appv",
        "language",
        "os",
        "adv_id",
        "gdpr",
        "ccpa",
        "country_code",
        "date"
    ]
)