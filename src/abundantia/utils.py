from __future__ import annotations

import logging

FREQ_INTERVAL_MAP: dict[str, int] = {"S": 1, "T": 60, "H": 3600}
INTERVAL_FREQ_MAP: dict[int, str] = {v: k for k, v in FREQ_INTERVAL_MAP.items()}


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

    unit_str = freq[-1]
    num_str = freq[:-1]

    num = int(num_str) if len(num_str) > 0 else 1
    unit = FREQ_INTERVAL_MAP[unit_str]

    return num * unit


def convert_interval_to_freq(interval: int) -> str:
    unit_num: int | None = None
    for unit_candidate in sorted(FREQ_INTERVAL_MAP.values(), reverse=True):
        if interval % unit_candidate == 0:
            unit_num = unit_candidate
            break

    if unit_num is None:
        raise ValueError

    num = interval // unit_num
    unit = INTERVAL_FREQ_MAP[unit_num]
    return f"{num}{unit}"
