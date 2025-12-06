import os
DB_PATH = os.environ.get("DB_PATH", "pods.db")
STREAM_TIMEOUT = int(os.environ.get("STREAM_TIMEOUT", 120))
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
