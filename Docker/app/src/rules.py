from abc import ABC, abstractmethod
import pandas as pd
from .alert import Alert
from .config import Config

class ErrorsRule(ABC):
    name: str

    def __init__(self, config: Config):
        self.error_col = config.ERROR_COLUMN
        self.error_val = config.ERROR_LEVEL
        self.date_col = config.DATE_COLUMN

        self.threshold = config.THRESHOLD
        self.buffer = pd.Series()

    def check(self, df: pd.DataFrame) -> list[Alert]:
        if df.empty and self.buffer.empty:
            return []
        elif df.empty and not self.buffer.empty:
            counts = self.buffer
            self.buffer = pd.Series()
        else:
            error_df = df[df[self.error_col] == self.error_val]
            counts = self._group(error_df)

        finalized = self._merge_buffer(counts)

        alerts = []
        spikes = finalized[finalized > self.threshold]

        for key, count in spikes.items():
            alerts.append(self._build_alert(key, count))

        return alerts

    @abstractmethod
    def _group(self, error_df: pd.DataFrame) -> pd.Series:
        pass

    @abstractmethod
    def _merge_buffer(self, counts: pd.Series) -> pd.Series:
        pass

    @abstractmethod
    def _build_alert(self, key, count: int) -> Alert:
        pass

class ErrorsPerTimeRule(ErrorsRule):
    name = "errors_per_time"

    def __init__(self, config: Config):
        super().__init__(config)
        self.time = config.TIME

    def _group(self, error_df):
        return error_df.groupby(
            pd.Grouper(key=self.date_col, freq=self.time)
        ).size()

    def _merge_buffer(self, counts):
        if not self.buffer.empty:
            buffer_ts = self.buffer.index[0]
            first_ts = counts.index.min()

            if first_ts == buffer_ts:
                counts = counts.add(self.buffer, fill_value=0)
            else:
                counts = pd.concat([self.buffer, counts])

        last_ts = counts.index.max()
        self.buffer = counts[counts.index == last_ts]
        counts = counts[counts.index < last_ts]

        return counts

    def _build_alert(self, key: pd.Timestamp, count):
        return Alert(
            rule_name=self.name,
            message=f"Detected {count} fatal errors at {key}",
            timestamp=key
        )

class ErrorsPerTimeAndAttributeRule(ErrorsRule):
    name = "errors_per_time_and_attribute"

    def __init__(self, config: Config):
        super().__init__(config)
        self.time = config.TIME_ATTR
        self.attribute = config.ATTRIBUTE

    def _group(self, error_df):
        return error_df.groupby([
            pd.Grouper(key=self.date_col, freq=self.time),
            self.attribute
        ]).size()

    def _merge_buffer(self, counts):
        if not self.buffer.empty:
            buffer_hour = self.buffer.index.get_level_values(0)[0]
            first_hour = counts.index.get_level_values(0).min()

            if buffer_hour == first_hour:
                counts = counts.add(self.buffer, fill_value=0)
            else:
                counts = pd.concat([self.buffer, counts])

        last_hour = counts.index.get_level_values(0).max()
        self.buffer = counts[
            counts.index.get_level_values(0) == last_hour
        ]

        counts = counts[
            counts.index.get_level_values(0) < last_hour
        ]

        return counts

    def _build_alert(self, key: tuple[pd.Timestamp, object], count):
        ts, attribute = key
        return Alert(
            rule_name=self.name,
            message=f"Attribute '{attribute}' has {count} errors at {ts}",
            timestamp=ts
        )
