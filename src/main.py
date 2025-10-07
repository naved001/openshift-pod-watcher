from .config import DB_PATH
from .db import PodDatabase
from .kube import get_corev1_api
from .watcher import watch_loop
from .logger import get_logger


def main():
    logger = get_logger()
    api = get_corev1_api()
    pod_database = PodDatabase(str(DB_PATH))
    import ipdb; ipdb.set_trace()

    logger.info("Starting application!")
    try:
        watch_loop(api, pod_database, logger)
    except KeyboardInterrupt:
        logger.info("NOOOOOO Don't kill me please.")
        pod_database.close()

if __name__ == "__main__":
    main()
