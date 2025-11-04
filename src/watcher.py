from kubernetes import watch
from datetime import timezone
from .config import (
    STREAM_TIMEOUT,
    TERMINAL_PHASES,
    IGNORED_NAMESPACES,
    IGNORED_NAMESPACE_PREFIXES,
)
from .utils import parse_openshift_resource, now_utc_iso


def effective_pod_requests(pod):
    """
    This function calculates the effective pod requests that the kubernetes scheduler
    uses when scheduling a pod[1]

    It first finds the biggest resources requested by an init container, and then sum of
    all the main containers resources. Finally, the max of either of those is what's
    allocated on a node.

    [1] https://kubernetes.io/docs/concepts/workloads/pods/init-containers/#resource-sharing-within-containers
    """
    if not pod.spec:
        return 0, 0, 0

    init_container_cpu = 0
    init_container_memory = 0
    init_container_gpu = 0
    cpu = memory = gpu = 0

    if pod.spec.init_containers:
        for init_container in pod.spec.init_containers:
            resources = init_container.resources.requests or {}
            init_container_cpu = max(init_container_cpu, parse_openshift_resource(resources.get('cpu', 0)))
            init_container_memory = max(init_container_memory, parse_openshift_resource(resources.get('memory', 0)))
            init_container_gpu = max(init_container_gpu, parse_openshift_resource(resources.get('nvidia.com/gpu', 0)))

    if pod.spec.containers:
        for container in pod.spec.containers:
            res = container.resources.requests or {}
            cpu += parse_openshift_resource(res.get("cpu", "0"))
            memory += parse_openshift_resource(res.get("memory", "0"))
            gpu += int(res.get("nvidia.com/gpu", 0))

    return max(cpu, init_container_cpu), max(memory, init_container_memory), max(gpu, init_container_gpu)


def get_pod_finished_time(pod):
    """Return finished_at timestamp if available."""
    if pod.status and pod.status.container_statuses:
        for cs in pod.status.container_statuses:
            term = getattr(cs.state, "terminated", None)
            if term and term.finished_at:
                return term.finished_at.astimezone(timezone.utc).isoformat()
    return None


def backfill_terminated_pods(api, pod_database, logger):
    """Find pods that have finished running but the pod object
    still exists in the cluster.
    This may be useful in the scenario where a pod starts and finishes
    while our watcher script was down.
    """


def mark_deleted_running_pods(api, pod_database, logger):
    """
    To address the scenario where the script starts tracking a pod, and then
    the script dies for some reason and the pod finishes and the object is deleted.

    Look into the pods that are still running i.e. pods with NULL end_time in our database.
    If they are no longer found in the cluster, mark the end_time to be now to avoid overcharging.
    Set guess to True for these pods, so we can see how often we had to guess end times.

    Maybe mark the end_time for pods every minute they are running, this way we never overcharge.
    This will require running this function in another thread and maybe thread safe way of handling
    access to DB.
    """


def watch_loop(api, pod_database, logger):
    """Main event watcher loop."""

    seen_running = pod_database.get_running_pods()
    logger.info(f"Loading running pods from database. Number of running_pods: {len(seen_running)}")
    w = watch.Watch()
    logger.info("Starting pod event stream...")

    while True:
        for event in w.stream(
            api.list_pod_for_all_namespaces, timeout_seconds=STREAM_TIMEOUT
        ):
            pod = event["object"]  # type: ignore
            ns = pod.metadata.namespace  # type: ignore
            name = pod.metadata.name  # type: ignore
            uid = pod.metadata.uid  # type: ignore
            phase = getattr(pod.status, "phase", None)  # type: ignore
            node = getattr(pod.spec, "node_name", None)  # type: ignore
            etype = event["type"]  # type: ignore

            if ns in IGNORED_NAMESPACES or ns.startswith(IGNORED_NAMESPACE_PREFIXES):
                continue

            cpu, mem, gpu = effective_pod_requests(pod)

            start = getattr(pod.status, "start_time", None)  # type: ignore
            start_iso = start.astimezone(timezone.utc).isoformat() if start else None

            # Start billing when running
            if (
                # When the script starts, all pods that are already running show up as "ADDED" all at once
                etype in ("ADDED", "MODIFIED")
                and phase == "Running"
                and uid not in seen_running
            ):
                pod_database.insert_new_pod(
                    uid, ns, name, node, cpu, mem, gpu, start_iso
                )
                seen_running.add(uid)
                logger.info(f"Started billing pod {ns}/{name}")

            # Stop billing when pod finishes
            elif (
                etype == "MODIFIED" and uid in seen_running and phase in TERMINAL_PHASES
            ):
                end_time = get_pod_finished_time(pod)
                guessed = False
                if not end_time:
                    end_time = now_utc_iso()
                    guessed = True

                pod_database.update_pod_end_time(
                    uid, end_time, guessed
                )
                seen_running.discard(uid)
                logger.info(f"Recorded end_time for {ns}/{name}")

            # If pod is deleted while still running
            elif etype == "DELETED" and uid in seen_running:
                end_time = now_utc_iso()
                guessed = False
                pod_database.update_pod_end_time(
                    uid, end_time, guessed
                )
                seen_running.discard(uid)
                logger.info(f"Deleted pod {ns}/{name}")
