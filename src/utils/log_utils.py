import logging
import datetime
import json
import traceback

LOGRECORD_DEFAULT_ATTRIBUTES = [
  "name",
  "args",
  "msg",
  "levelname",
  "levelno",
  "pathname",
  "filename",
  "module",
  "exc_info",
  "exc_text",
  "stack_info",
  "lineno",
  "funcName",
  "created",
  "msecs",
  "relativeCreated",
  "thread",
  "threadName",
  "processName",
  "process",
]

class LogfmtFormatter(logging.Formatter):

  def __init__(self, *args, **kwargs):
    super().__init__(args, kwargs)
  
  def format(self, record: logging.LogRecord) -> str:

    isotime = datetime.datetime.fromtimestamp(record.created).isoformat()
    msg = f'time="{isotime}" name="{record.name}" level="{record.levelname}" msg="{record.msg}"'

    # add arguments from 'args'
    # e.g. log.info("something", {"foo": "bar"})
    # args = {"foo": "bar"}
    if (record.args):
      for k, v in record.args.items():
        msg += f" {k}={v}"


    # add arguments from the 'extra' parameter
    # e.g. log.info("something", extra={"foo": "bar"})
    # in this case we will have record.foo = "bar"
    for k in record.__dict__:
      if k not in LOGRECORD_DEFAULT_ATTRIBUTES:
        msg += f" {k}={record.__getattribute__(k)}"

    return msg

class JsonFormatter(logging.Formatter):

  def __init__(self, *args, **kwargs):
    super().__init__(args, kwargs)
  
  def format(self, record: logging.LogRecord) -> str:

    isotime = datetime.datetime.fromtimestamp(record.created).isoformat()
    msg_dict = {
      "time": isotime,
      "name": record.name,
      "level": record.levelname,
      "msg": record.msg, 
    }

    if record.exc_info:
      (exc_type, exc, trace) = record.exc_info
      msg_dict["error"] = {
        "type": str(exc_type),
        "message": str(exc),
        "trace": traceback.format_exc(limit=5),
      }
      # msg_dict["exc_info"] = str(record.exc_info)

    # add arguments from 'args'
    # e.g. log.info("something", {"foo": "bar"})
    # args = {"foo": "bar"}
    if (record.args):
      msg_dict |= record.args


    # add arguments from the 'extra' parameter
    # e.g. log.info("something", extra={"foo": "bar"})
    # in this case we will have record.foo = "bar"
    for k in record.__dict__:
      if k not in LOGRECORD_DEFAULT_ATTRIBUTES:
        msg_dict[k] = record.__getattribute__(k)

    return json.dumps(msg_dict)

def create_console_logger(
    name: str, 
    level: int = logging.INFO, 
    formatter: logging.Formatter = JsonFormatter()
) -> logging.Logger:
  
  # this has to be set to "NOTSET", 
  # otherwise only "warning" and higher priority logs will be printed
  logging.root.setLevel(logging.NOTSET)

  log = logging.getLogger(name)
  console_handler = logging.StreamHandler()
  console_handler.setFormatter(formatter)
  console_handler.setLevel(level)
  log.addHandler(console_handler)
  return log
