from abundantia.utils import setup_logger


class BaseClient:
    def __init__(self, log_level: str = "DEBUG") -> None:
        self.logger = setup_logger(__name__, log_level)
