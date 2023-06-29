"""Single line logger module."""
import logging
import sys
from pathlib import Path
from types import TracebackType
from typing import Final

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent.parent.parent
REPO_NAME = PROJECT_ROOT.stem


def get_relative_path(
    path: Path,
) -> str:
    """Truncate path to last n-parts of project level."""
    return path.as_posix().replace(PROJECT_ROOT.as_posix(), "")


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
        message = []
        # if no exception, ensure message is single line output
        if record.msg:
            record.msg = " ".join(str(record.msg).split())
        # if exception, ensure single line output with exception details
        if record.exc_info:
            message.append(f"{record.msg} | " if record.msg else "")
            ex_type, ex_value, ex_tb = sys.exc_info()
            if isinstance(ex_type, BaseException):
                message.append(f"{ex_type} ")
            if isinstance(ex_value, BaseException):
                message.append(" ".join(str(ex_value).split()))
            if isinstance(ex_tb, TracebackType):
                message.append(f" ({Path(ex_tb.tb_frame.f_code.co_filename).name} line:{ex_tb.tb_lineno})")
            record.msg = "".join(message)
            # reset logger for next exception
            record.exc_info = None
            record.exc_text = None
        return super().format(record)


def init_logger(
    log_name: str,
) -> logging.Logger:
    """Generate custom Logger object writes output to both file and standard output.

    Creates parent directories and blank log file (if missing)
    https://docs.python.org/3/library/logging.html#logging-levels

    Args:
        log_name (str): __file__ of calling module passed to getLogger()

    Returns:
        logging.Logger: instance based on name and file location
    """
    # create log directory and empty file (if needed)
    log_file = Path(PROJECT_ROOT, "logs", f"{REPO_NAME}.log")
    # create log directory and empty file (if needed)
    if not log_file.parent.exists():
        log_file.parent.mkdir(parents=True, exist_ok=True)
    if not log_file.is_file():
        log_file.touch(mode=0o777, exist_ok=True)

    # when passing __file__, set to caller basename
    logger = logging.getLogger(name=Path(log_name).name)
    logger.setLevel(level=logging.INFO)

    # update custom log format
    log_format = SingleLineFormatter(
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
