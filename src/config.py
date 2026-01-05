import os
DB_PATH = os.environ.get("DB_PATH", "pods.db")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LIB_LOG_LEVEL = os.environ.get("LIB_LOG_LEVEL", "WARNING").upper()
STREAM_TIMEOUT = int(os.environ.get("STREAM_TIMEOUT", 240))  # in seconds
IGNORED_NAMESPACES = {
    "openshift",
    "kube-system",
    "kube-public",
    "kube-node-lease",
    "default",
    "istio-system",
    "openshift-marketplace",
    "nvidia-gpu-operator",
    "open-cluster-management-agent-addon",
    "open-cluster-management-agent",
}
IGNORED_NAMESPACE_PREFIXES = ("openshift-", "kube-")
TERMINAL_PHASES = {"Succeeded", "Failed"}
STARTING_PHASES = {"Pending", "Running"}
