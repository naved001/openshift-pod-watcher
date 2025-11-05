from kubernetes import client, config
from .logger import get_logger

def get_corev1_api():
    logger = get_logger()
    try:
        config.load_incluster_config()
        logger.info("Loaded in-cluster kubernetes config")
    except config.ConfigException:
        logger.info("In-cluster config not found")
        try:
            config.load_kube_config()
            logger.info("Loaded config from kube context")
        except config.ConfigException as e:
            logger.error(f"Failed to load any kubernetes configuration: {e}")
            raise
    return client.CoreV1Api()
