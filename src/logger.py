import logging
from .config import LOG_LEVEL, LIB_LOG_LEVEL

def get_logger():
    level = getattr(logging, LOG_LEVEL, logging.INFO)
    lib_level = getattr(logging, LIB_LOG_LEVEL, logging.WARNING)
    logging.basicConfig(
        level=level,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        force=True
    )
    logging.getLogger("urllib3").setLevel(lib_level)
    logging.getLogger("kubernetes").setLevel(lib_level)
    return logging.getLogger("openshift_watcher")
