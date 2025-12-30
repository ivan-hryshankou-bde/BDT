import pandas as pd
import logging

from .rules import ErrorsRule
from .config import Config
from .alert import Alert

# Logging configuration that writes alerts and info to report.log and to stdout
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("reports/report.log", mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class LogProcessor:
    """
    Class for processing data logs in chunks, 
    normalizing date columns, applying alerting rules,
    and emitting alerts with logging
    """

    def __init__(self, file_path: str, rules: list[ErrorsRule], config: Config):
        """Initialize log processor"""

        self.file_path = file_path
        self.rules = rules
        self.chunk_size = config.CHUNK_SIZE
        self.date_col = config.DATE_COLUMN
        self.cols = config.COLUMNS

    def process(self):
        """
        Main processing loop that:
        - reads file in chunks
        - normalizes date column
        - applies rules per chunk
        - performs final rule flush with empty DataFrame
        """

        logging.info(f"Starting processing of {self.file_path}")

        try:
            with pd.read_csv(
                self.file_path,
                header=0,
                names=self.cols,
                chunksize=self.chunk_size
            ) as reader:
                
                # Process each chunk independently
                for chunk in reader:
                    chunk = self.__process_date(chunk)
                    self.__process_rule(chunk)
                
                # Final call to flush internal buffer
                self.__process_rule(pd.DataFrame())
                    
        except IOError as file_err:
            logging.error(f"File error: {file_err}") 
        except Exception as e:
            logging.error(f"Unexpected error: {e}")

    def __process_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize date column depending on configured date type:
        - unix timestamp column ("date")
        - SDK formatted string column ("sdk_date")
        """

        # Unix timestamp
        if self.date_col == "date":
            df[self.date_col] = pd.to_datetime(df[self.date_col], unit="s", errors="coerce")

            return df
        
        # SDK date with custom AM/PM suffixes
        elif self.date_col == "sdk_date":
            AM_PM_MAP = {
                "PG": "AM",
                "SA": "AM",
                "CH": "PM",
                "PTG": "PM"
            }

            # Replace custom suffixes with standard AM/PM
            pattern = r"\s(PG|SA|CH|PTG)$"

            df[self.date_col] = (
                df[self.date_col]
                .str.replace(
                    pattern,
                    lambda m: " " + AM_PM_MAP[m.group(1)],
                    regex=True
                )
            )

            # Mask rows that explicitly contain AM/PM
            mask_ampm = df[self.date_col].str.contains(r"\bAM|PM\b", na=False)

            # Detect date format with AM/PM
            df.loc[mask_ampm, self.date_col] = pd.to_datetime(
                df.loc[mask_ampm, self.date_col],
                format="%m/%d/%Y %I:%M:%S %p",
                errors="coerce"
            )

            # Detect 24-hour date format
            df.loc[~mask_ampm, self.date_col] = pd.to_datetime(
                df.loc[~mask_ampm, self.date_col],
                format="%m/%d/%Y %H:%M:%S",
                errors="coerce"
            )

            # Final merging to ensure datetime dtype
            df[self.date_col] = pd.to_datetime(df[self.date_col], errors="coerce")
            
            return df
        
        # Unknown date column return unchanged
        else:
            return df

    def __process_rule(self, df: pd.DataFrame):
        """Apply all configured rules to a DataFrame chunk and emit generated alerts"""

        for rule in self.rules:
            alerts = rule.check(df)
            for alert in alerts:
                self.__send_alert(alert)

    def __send_alert(self, alert: Alert):
        """Emit alert with logging"""

        logging.warning(f"ALERT >>> [{alert.rule_name}] - {alert.message}")
