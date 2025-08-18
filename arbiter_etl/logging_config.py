import logging
from pathlib import Path
from typing import Optional


def configure_logging(level: int = logging.INFO, logfile: Optional[str | Path] = None) -> None:
    """Configure basic logging for the repository entrypoint.

    - level: logging level (e.g., logging.INFO)
    - logfile: path to file to write logs; if None logs go to stdout.
    """
    root_logger = logging.getLogger()
    if root_logger.handlers:
        # already configured
        return

    fmt = '%(asctime)s %(levelname)s %(name)s: %(message)s'
    if logfile:
        logfile = Path(logfile)
        logfile.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()

    handler.setFormatter(logging.Formatter(fmt))
    root_logger.setLevel(level)
    root_logger.addHandler(handler)
