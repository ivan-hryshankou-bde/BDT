import pandas as pd
import logging

from .rules import ErrorsRule
from .config import Config
from .alert import Alert

# Configure of logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("report.log", mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class LogProcessor:
    def __init__(self, file_path: str, rules: list[ErrorsRule], config: Config):
        self.file_path = file_path
        self.rules = rules
        self.chunk_size = config.CHUNK_SIZE
        self.date_col = config.DATE_COLUMN
        self.cols = config.COLUMNS

    def process(self):
        logging.info(f"Starting processing of {self.file_path}")

        try:
            with pd.read_csv(
                self.file_path,
                header=0,
                names=self.cols,
                chunksize=self.chunk_size
            ) as reader:
                
                for chunk in reader:
                    chunk = self.__process_date(chunk)
                    self.__process_rule(chunk)

                self.__process_rule(pd.DataFrame())
                    
        except IOError as file_err:
            logging.error(f"File error: {file_err}") 
        except Exception as e:
            logging.error(f"Unexpected error: {e}")

    def __process_date(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.date_col == "date":
            df[self.date_col] = pd.to_datetime(df[self.date_col], unit="s", errors="coerce")

            return df
        
        elif self.date_col == "sdk_date":
            AM_PM_MAP = {
                "PG": "AM",
                "SA": "AM",
                "CH": "PM",
                "PTG": "PM"
            }

            pattern = r"\s(PG|SA|CH|PTG)$"

            df[self.date_col] = (
                df[self.date_col]
                .str.replace(
                    pattern,
                    lambda m: " " + AM_PM_MAP[m.group(1)],
                    regex=True
                )
            )

            mask_ampm = df[self.date_col].str.contains(r"\bAM|PM\b", na=False)

            df.loc[mask_ampm, self.date_col] = pd.to_datetime(
                df.loc[mask_ampm, self.date_col],
                format="%m/%d/%Y %I:%M:%S %p",
                errors="coerce"
            )

            df.loc[~mask_ampm, self.date_col] = pd.to_datetime(
                df.loc[~mask_ampm, self.date_col],
                format="%m/%d/%Y %H:%M:%S",
                errors="coerce"
            )

            df[self.date_col] = pd.to_datetime(df[self.date_col], errors="coerce")
            
            return df
        
        else:
            return df

    def __process_rule(self, df: pd.DataFrame):
        for rule in self.rules:
            alerts = rule.check(df)
            for alert in alerts:
                self.__send_alert(alert)

    def __send_alert(self, alert: Alert):
        logging.warning(f"ALERT >>> [{alert.rule_name}] - {alert.message}")
