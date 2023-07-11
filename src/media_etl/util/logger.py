"""Single line logger module."""
import logging
import sys
from pathlib import Path

from media_etl.util.settings import PROJECT_ROOT, REPO_NAME


def get_readable_size(path: Path) -> str:
    """Convert bytes to readable string."""
    if path.exists():
        size = float(path.stat().st_size)
    else:
        size = 0.0
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(size) < 1024.0:
            return f"({size:03.2f} {unit}B)"
        size /= 1024.0
    return f"({size:03.2f} YiB)"


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
    log_format = logging.Formatter(
        fmt="{asctime} [{levelname}] {name} | {funcName}() line:{lineno} | {message}",
        datefmt="%Y-%m-%d %H:%M:%S",
        style="{",
        validate=True,
    )

    # save messages to log file
    file_handler = logging.FileHandler(filename=log_file)
    file_handler.setLevel(level=logging.DEBUG)
    file_handler.setFormatter(fmt=log_format)
    logger.addHandler(hdlr=file_handler)

    # display messages to console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level=logging.INFO)
    console_handler.setFormatter(fmt=log_format)
    logger.addHandler(hdlr=console_handler)
    return logger
