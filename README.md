# openshift-pod-watcher

This tool tracks pods and the resources requested by these pods.

This information is writen to a database and may be consumed for
generating a billing report.

## How to run

```
pip install -r requirements.txt
python -m src.main
```

To deploy it in-cluster look at the overlays in `k8s/` directory.

## Effective Resouce Request

We actually track pods and not containers because that's what we care about
for billing. When a pod runs with multiple containers (init or main), we
calculate the effective resource request just like how the scheduler would.

From the kubernetes documentation[1]

> The Pod's effective request/limit for a resource is the higher of:

>    * the sum of all app containers request/limit for a resource

>    * the effective init request/limit for a resource

[1] https://kubernetes.io/docs/concepts/workloads/pods/init-containers/#resource-sharing-within-containers

