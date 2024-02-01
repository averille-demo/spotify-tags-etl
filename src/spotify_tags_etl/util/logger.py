"""Single line logger module."""

import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from spotify_tags_etl.util.settings import PROJECT_ROOT, REPO_NAME


def get_readable_size(path: Path) -> str:
    """Convert bytes to readable string."""
    block_size = 1000.0
    if path.is_file():
        file_size = float(path.stat().st_size)
        for unit in ["B", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
            if abs(file_size) < block_size:
                return f"({file_size:03.2f} {unit})"
            file_size /= block_size
    return ""


def relative_size(
    path: Path,
) -> str:
    """Logging helper, truncate path relative to project level, add readable file size."""
    relative_path = path.as_posix().replace(PROJECT_ROOT.as_posix(), "")
    return f"{relative_path} {get_readable_size(path)}"


class SingleLineFormatter(logging.Formatter):
    """Helper class to ensure output is formatted to single line in log file.

    https://docs.python.org/3/library/logging.html#logging.LogRecord

    Returns:
        logging.LogRecord: single line formatted string
    """

    def format(self, record: logging.LogRecord) -> str:
        """Custom formatted log message.

        Args:
            record (LogRecord): single log record

        Returns:
            str: message and exception debugging information to single line
        """
        # if no exception, ensure message is single line output
        if record.msg:
            record.msg = " ".join(str(record.msg).split())
        # if exception, ensure single line output with exception details
        if record.exc_info:
            ex_type, ex_value, ex_tb = sys.exc_info()
            ex_msg = []
            ex_msg.append(f"{record.msg} | " if record.msg else "")
            ex_msg.append(f"{ex_type} ")
            ex_msg.append(" ".join(str(ex_value).split()))
            # overwrite prior message with original plus exception information
            record.msg = "".join(ex_msg)
            # reset logger for next exception
            record.exc_info = None
            record.exc_text = None
        return super().format(record)


def init_logger(
    caller: str,
) -> logging.Logger:
    """Generate custom Logger object writes output to both file and standard output.

    Creates parent directories and blank log file (if missing)
    https://docs.python.org/3/library/logging.html#logging-levels

    Args:
        caller (str): __file__ of calling module passed to getLogger()

    Returns:
        logging.Logger: instance based on name and file location
    """
    # create log directory and empty file (if needed)
    log_file = Path(PROJECT_ROOT, "logs", f"{REPO_NAME}.log")
    if not log_file.parent.exists():
        log_file.parent.mkdir(parents=True, exist_ok=True)
    if not log_file.is_file():
        log_file.touch(mode=0o777, exist_ok=True)

    # when passing __file__, set to caller basename
    logger = logging.getLogger(name=Path(caller).name)
    logger.setLevel(level=logging.INFO)

    # update custom log format
    log_format = SingleLineFormatter(
        fmt="{asctime} [{levelname}] {name} | {funcName}() line:{lineno} | {message}",
        datefmt="%Y-%m-%d %H:%M:%S",
        style="{",
        validate=True,
    )

    def namer(name):
        """Move .log file extension to end after YYYY-MM-DD."""
        return name.replace(".log", "") + ".log"

    # save messages to log file
    fh = TimedRotatingFileHandler(filename=log_file, when="midnight", backupCount=5, encoding="utf8")
    fh.setLevel(level=logging.DEBUG)
    fh.setFormatter(fmt=log_format)
    fh.namer = namer
    logger.addHandler(hdlr=fh)

    # display messages to console
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(level=logging.INFO)
    sh.setFormatter(fmt=log_format)
    logger.addHandler(hdlr=sh)
    return logger
