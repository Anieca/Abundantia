from __future__ import annotations

import logging


def setup_logger(name: str, level: str = "DEBUG", logfile_path: str | None = None) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if logfile_path is not None:
        handlers.append(logging.FileHandler(logfile_path))

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    for handler in handlers:
        handler.setLevel(getattr(logging, level, logging.NOTSET))
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def convert_freq_to_interval(freq: str) -> int:
    freq_code_map: dict[str, int] = {"S": 1, "T": 60, "H": 3600}

    unit_str = freq[-1]
    num_str = freq[:-1]

    num = int(num_str) if len(num_str) > 0 else 1
    unit = freq_code_map[unit_str]

    return num * unit
