import logging
from logging.handlers import TimedRotatingFileHandler
from auth_puller import auth_puller
import os

log_file_name = "bursar-integ.log"
paths = auth_puller("auth.json", "paths")
log_path = os.path.join(paths["local_path"], paths["logs"], log_file_name)

logging.basicConfig(
    handlers=[
        TimedRotatingFileHandler(log_path, when="midnight"),
        logging.StreamHandler(),
    ],
    level=logging.INFO,
    format="%(asctime)s -- %(levelname)s -- %(filename)s/%(funcName)s -- %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

logging.getLogger("paramiko").setLevel(logging.WARNING)

log = logging.getLogger(__name__)
