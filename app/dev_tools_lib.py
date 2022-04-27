"""
python library of reusable developer tools
"""
__author__ = "averille"
__status__ = "demo"
__license__ = "MIT"
__version__ = "1.5.5"
import os
import sys
import logging
from pathlib import Path
import pprint as pp

CWD_PATH = Path(__file__).resolve().parent
# all modules write to same log:
REPO_NAME = CWD_PATH.parent.name


def show_data(title: str, data: object = None) -> None:
    """Displays all data values in formatted output"""
    print(f"\n{title}")
    pp.pprint(object=data, indent=2, width=120, compact=True, sort_dicts=False)


class SingleLineExceptionFormatter(logging.Formatter):
    """https://docs.python.org/3/library/logging.html#logging.LogRecord"""

    def format(self, record: logging.LogRecord) -> str:
        if record.exc_info:
            single_line = ""
            if record.msg:
                single_line += f"{record.msg} | "
            ex_type, ex_value, ex_tb = sys.exc_info()
            ex_type = f"{ex_type}" if ex_type else ""
            ex_value = " ".join(f"{str(ex_value)}".split()) if ex_value else ""
            src_name = f"{os.path.split(ex_tb.tb_frame.f_code.co_filename)[1]}" if ex_tb else ""
            line_num = f"{ex_tb.tb_lineno}" if ex_tb else ""
            single_line += f"{ex_type} {ex_value} | {src_name}:{line_num}"
            record.msg = single_line
            record.exc_info = None
            record.exc_text = None
        return super().format(record)


def init_logger(
        log_name=REPO_NAME,
        log_file=Path(CWD_PATH, "logs", f"{REPO_NAME}.log"),
) -> logging.Logger:
    """
    generate custom Logger object writes output to both file and standard output
    """
    if not log_file.parent.exists():
        log_file.parent.mkdir(parents=True, exist_ok=True)
    if not log_file.is_file():
        log_file.touch(mode=0o777, exist_ok=True)

    # set name to caller module
    logger = logging.getLogger(name=log_name)
    logger.setLevel(logging.INFO)
    log_format = "{asctime} [{levelname}]\t{name} | {funcName}() line:{lineno} | {message}"
    datefmt = "%Y-%m-%d %H:%M:%S"
    log_fmt = SingleLineExceptionFormatter(fmt=log_format, datefmt=datefmt, style="{",
                                           validate=True)

    # save to file
    file_hdlr = logging.FileHandler(log_file)
    file_hdlr.setLevel(logging.INFO)
    file_hdlr.setFormatter(fmt=log_fmt)
    logger.addHandler(file_hdlr)

    # display log in console
    stdout_hdlr = logging.StreamHandler(sys.stdout)
    stdout_hdlr.setLevel(logging.DEBUG)
    stdout_hdlr.setFormatter(fmt=log_fmt)
    logger.addHandler(stdout_hdlr)
    return logger


def limit_path(
        path: Path,
        level=2,
) -> str:
    """
    returns last n-level parts of path object
    input: drive:/parent/subdir1/subdir2/data/filename.txt
    output: /data/filename.txt
    """
    if level < 1:
        return f"{path}"
    return f"{os.path.sep}{os.path.sep.join(path.parts[-level:])}"
