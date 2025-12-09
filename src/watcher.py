from kubernetes import watch
from kubernetes.client.exceptions import ApiException
from datetime import timezone
from .config import (
    STREAM_TIMEOUT,
    TERMINAL_PHASES,
    STARTING_PHASES,
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
    if not (pod.status and pod.status.phase in TERMINAL_PHASES):
        return None

    all_statuses = (pod.status.container_statuses or []) + \
                    (pod.status.init_container_statuses or [])

    if not all_statuses:
        return None

    all_finish_times = []
    for cs in all_statuses:
        term = getattr(cs.state, "terminated", None)
        if term and term.finished_at:
            all_finish_times.append(term.finished_at.astimezone(timezone.utc))

    if all_finish_times:
        return max(all_finish_times).isoformat()

    return None


def startup_reconciliation(api, pod_database, logger):
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
    logger.info("Start reconciliation")
    # get all pods but not from cache. This is slow, but more reliable.
    all_pods_list = api.list_pod_for_all_namespaces(watch=False, resource_version="")
    initial_resource_version = all_pods_list.metadata.resource_version
    logger.info(f"Resource version at startup: {initial_resource_version}")
    cluster_pods_map = {pod.metadata.uid: pod for pod in all_pods_list.items}
    cluster_pods_uids = set(cluster_pods_map.keys())
    seen_running = pod_database.get_running_pods()
    finished_pods = pod_database.get_finished_pods()
    zombie_pod_uids = seen_running - cluster_pods_uids
    guessed_end_time = now_utc_iso()

    if zombie_pod_uids:
        logger.warning(f"Found {len(zombie_pod_uids)} zombie pods - deleted when the watcher was down")
        end_time = guessed_end_time
        guessed = True
        for uid in zombie_pod_uids:
            pod_metadata = pod_database.get_pod_metadata(uid)
            if pod_metadata is None:
                logger.error(f"Could not find metadata for zombie pod uid {uid}, skipping")
                continue
            if pod_metadata['end_time'] is not None:
                logger.error(f"Pod {uid} {pod_metadata['namespace']}/{pod_metadata['name']} already has end_time set, it should not be a zombie, skipping")
                seen_running.discard(uid)
                finished_pods.add(uid)
                continue
            logger.info(f"Setting end time for zombie pod {uid} {pod_metadata['namespace']}/{pod_metadata['name']} started at {pod_metadata['start_time']}")
            pod_database.update_pod_end_time(uid, end_time, guessed)
            seen_running.discard(uid)
            finished_pods.add(uid)
    else:
        logger.info("No zombie pods found!")

    logger.info("Starting backfill checks")
    for uid, pod in cluster_pods_map.items():
        ns = pod.metadata.namespace  # type: ignore
        if ns in IGNORED_NAMESPACES or ns.startswith(IGNORED_NAMESPACE_PREFIXES):
            continue
        name = pod.metadata.name  # type: ignore
        uid = pod.metadata.uid  # type: ignore
        phase = getattr(pod.status, "phase", None)  # type: ignore
        node = getattr(pod.spec, "node_name", None)  # type: ignore
        phase = getattr(pod.status, "phase", None)
        cpu, mem, gpu = effective_pod_requests(pod)
        start = getattr(pod.status, "start_time", None)  # type: ignore
        start_iso = start.astimezone(timezone.utc).isoformat() if start else None

        if phase in TERMINAL_PHASES and uid not in finished_pods:
            if uid in seen_running:
                # This is a stale pod, it was added to the database when the script was
                # running but it finished when the the watcher was down. So we just update it's
                # endtime
                logger.info(f"Updating stale pod {ns}/{name} that finished while watcher was down.")
                end_time = get_pod_finished_time(pod)
                guessed = False
                if not end_time:
                    end_time = guessed_end_time
                    guessed = True
                pod_database.update_pod_end_time(
                    uid, end_time, guessed
                )
                seen_running.discard(uid)
            else:
                # True backfill, the pod started and finished when the watcher was down
                if start_iso is None:
                    logger.error(f"Pod {ns}/{name} has no start time, cannot backfill")
                    continue

                pod_database.insert_new_pod(
                    uid, ns, name, node, cpu, mem, gpu, start_iso
                )
                guessed = False
                end_time = get_pod_finished_time(pod)
                if not end_time:
                    guessed = True
                    end_time = guessed_end_time
                    # TODO:  we should probably set it to a a minute or so after start, to avoid overcharging
                pod_database.update_pod_end_time(
                    uid, end_time, guessed
                ) # TODO: This should be a single insert statement
                logger.info(f"Backfilled pod {ns}/{name} end_time={end_time} guessed={guessed}")
            finished_pods.add(uid)
    return seen_running, finished_pods, initial_resource_version


def watch_loop(api, pod_database, logger):
    """Main event watcher loop."""
    seen_running, finished_pods, resource_version = startup_reconciliation(api, pod_database, logger)
    logger.info("Starting watch loop")

    while True:
        w = watch.Watch()
        try:
            for event in w.stream(
                api.list_pod_for_all_namespaces,
                timeout_seconds=STREAM_TIMEOUT,
                resource_version=resource_version
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

                # Start billing when running. Instead of relying only no the pahse, we check
                # by seeing if a node is assigned and start_time has been set. That is because
                # when init contaners are running the pod status is actually Pending but it is
                # holding the effective resources
                if (
                    etype in ("ADDED", "MODIFIED")
                    and start is not None
                    and node is not None
                    and uid not in seen_running
                    and phase in STARTING_PHASES
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
                    finished_pods.add(uid)
                    logger.info(f"Recorded end_time for {ns}/{name} guessed={guessed} end_time={end_time}")

                # If pod is deleted while still running
                elif etype == "DELETED" and uid in seen_running:
                    end_time = now_utc_iso()
                    guessed = True
                    pod_database.update_pod_end_time(
                        uid, end_time, guessed
                    )
                    seen_running.discard(uid)
                    finished_pods.add(uid)
                    logger.info(f"Deleted pod {ns}/{name}")
                else:
                    logger.debug(f"Ignored event {etype} for pod {ns}/{name} in phase {phase}")
        except ApiException as e:
            logger.error(f"Kubernetes API exception: {e}")
            if e.status in (410, 504):
                logger.info("Re-establishing watch due to resource version issue.")
                resource_version = ""
                continue
            raise
        except Exception as e:
            logger.exception(f"Unexpected error in watch loop: {e}")
            resource_version = ""
            continue
