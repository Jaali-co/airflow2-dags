"""SparkK8sOperator — soumission SparkApplication sur Kubernetes."""

from __future__ import annotations

import hashlib
import re
import time
from typing import Any

from kubernetes import client, config

from vigie_spark_operator.compat import BaseOperator
from vigie_spark_operator.labels import airflow_labels, merge_labels, sanitize_label_value
from vigie_spark_operator.models import normalize_emptydir_list, normalize_pvc_list
from vigie_spark_operator.retry import k8s_call_with_retry
from vigie_spark_operator.shutdown import handle_job_timeout, resource_summary

_NOISY_LOG_FRAGMENTS = (
    "BlockManagerInfo:",
    "MemoryStore:",
    "MapOutputTrackerMaster",
    "broadcast_",
    "ShutdownHookManager:",
    "MetricsSystemImpl:",
)


class SparkK8sOperator(BaseOperator):
    """
    Soumet un SparkApplication via le Spark Operator K8s.

    Par défaut injecte ``dag_id`` / ``task_id`` / ``run_id`` sur metadata, driver
    et executor. Option ``managed_by=\"vigie\"`` pour le collecteur capacité Vigie.
    ``dry_run=True`` : construit + logue le manifeste sans créer la CR.
    """

    template_fields = (
        "base_name",
        "namespace",
        "image",
        "main_application_file",
        "application_type",
        "main_class",
        "arguments",
        "spark_version",
        "driver_memory",
        "executor_memory",
        "spark_conf",
        "labels",
        "env",
        "jars",
        "pvc_configs",
        "emptydir_configs",
    )

    def __init__(
        self,
        name: str,
        namespace: str,
        image: str,
        main_application_file: str,
        application_type: str = "Python",
        main_class: str | None = None,
        arguments: list | None = None,
        spark_version: str = "3.1.1",
        driver_cores: int = 1,
        driver_cores_request: str = "20m",
        driver_memory: str = "512m",
        executor_cores: int = 1,
        executor_cores_request: str = "400m",
        executor_memory: str = "512m",
        executor_instances: int = 2,
        service_account: str = "spark",
        # Security context — défauts historiques ; surchargeables.
        run_as_user: int = 0,
        fs_group: int = 0,
        allow_privilege_escalation: bool = False,
        # Overrides optionnels par rôle (None → valeur partagée ci-dessus)
        driver_run_as_user: int | None = None,
        driver_fs_group: int | None = None,
        driver_allow_privilege_escalation: bool | None = None,
        executor_run_as_user: int | None = None,
        executor_fs_group: int | None = None,
        executor_allow_privilege_escalation: bool | None = None,
        spark_conf: dict | None = None,
        labels: dict | None = None,
        env: dict | None = None,
        timeout_pod_discovery: int = 120,
        timeout_pod_running: int = 300,
        timeout_job: int = 3600,
        timeout_cleanup: int = 60,
        timeout: int | None = None,
        poll_interval: int = 10,
        api_error_retries: int = 5,
        in_cluster: bool = True,
        jars: list | None = None,
        pvc_configs: list | None = None,
        emptydir_configs: list | None = None,
        fail_on_unschedulable: bool = True,
        dry_run: bool = False,
        inject_airflow_labels: bool = True,
        managed_by: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.base_name = name
        self.name = name
        self.namespace = namespace
        self.image = image
        self.main_application_file = main_application_file
        self.application_type = application_type
        self.main_class = main_class
        self.arguments = arguments or []
        self.spark_version = spark_version
        self.driver_cores = driver_cores
        self.driver_cores_request = driver_cores_request
        self.driver_memory = driver_memory
        self.executor_cores = executor_cores
        self.executor_memory = executor_memory
        self.executor_cores_request = executor_cores_request
        self.executor_instances = executor_instances
        self.service_account = service_account
        self.run_as_user = run_as_user
        self.fs_group = fs_group
        self.allow_privilege_escalation = allow_privilege_escalation
        self.driver_run_as_user = driver_run_as_user
        self.driver_fs_group = driver_fs_group
        self.driver_allow_privilege_escalation = driver_allow_privilege_escalation
        self.executor_run_as_user = executor_run_as_user
        self.executor_fs_group = executor_fs_group
        self.executor_allow_privilege_escalation = executor_allow_privilege_escalation
        self.spark_conf = spark_conf or {}
        self.labels = labels or {}
        self.env = env or {}
        self.poll_interval = poll_interval
        self.api_error_retries = api_error_retries
        self.in_cluster = in_cluster
        self.jars = jars or []
        self.pvc_configs = normalize_pvc_list(pvc_configs)
        self.emptydir_configs = normalize_emptydir_list(emptydir_configs)
        self.fail_on_unschedulable = fail_on_unschedulable
        self.dry_run = dry_run
        self.inject_airflow_labels = inject_airflow_labels
        self.managed_by = managed_by

        if timeout is not None:
            self.timeout_pod_discovery = 120
            self.timeout_pod_running = 300
            self.timeout_job = timeout
            self.timeout_cleanup = 60
        else:
            self.timeout_pod_discovery = timeout_pod_discovery
            self.timeout_pod_running = timeout_pod_running
            self.timeout_job = timeout_job
            self.timeout_cleanup = timeout_cleanup

        self._custom_api = None
        self._core_api = None
        self._force_killed = False

    def _sanitize_k8s_name(self, value: str) -> str:
        value = value.lower()
        value = re.sub(r"[^a-z0-9-]", "-", value)
        value = re.sub(r"-+", "-", value)
        return value.strip("-")

    def _build_unique_name(self, context) -> str:
        max_len = 56
        ti = context.get("ti")
        try_number = getattr(ti, "try_number", 1) if ti is not None else 1
        raw = (
            f"{self.base_name}-"
            f"{context['dag'].dag_id}-"
            f"{context['task'].task_id}-"
            f"{context.get('ts_nodash', 'run')}-"
            f"try{try_number}"
        )
        sanitized = self._sanitize_k8s_name(raw)
        if len(sanitized) <= max_len:
            return sanitized
        hash_suffix = hashlib.sha1(sanitized.encode()).hexdigest()[:8]
        return f"{sanitized[: max_len - 9]}-{hash_suffix}"

    def _build_volume_mounts(self) -> tuple[list, list]:
        driver_mounts, executor_mounts = [], []
        for pvc in self.pvc_configs:
            mount = {"name": pvc.get("name", pvc["claim_name"]), "mountPath": pvc["mount_path"]}
            driver_mounts.append(mount)
            executor_mounts.append(mount)
        for ed in self.emptydir_configs:
            mount = {"name": ed["name"], "mountPath": ed["mount_path"]}
            driver_mounts.append(mount)
            executor_mounts.append(mount)
        return driver_mounts, executor_mounts

    def _build_volumes(self) -> list:
        volumes = []
        for pvc in self.pvc_configs:
            volumes.append(
                {
                    "name": pvc.get("name", pvc["claim_name"]),
                    "persistentVolumeClaim": {"claimName": pvc["claim_name"]},
                }
            )
        for ed in self.emptydir_configs:
            spec: dict[str, Any] = {}
            if ed.get("size_limit"):
                spec["sizeLimit"] = ed["size_limit"]
            volumes.append({"name": ed["name"], "emptyDir": spec})
        return volumes

    def _security_for(self, role: str) -> tuple[dict[str, Any], dict[str, Any]]:
        """Retourne (podSecurityContext, securityContext) pour driver|executor."""
        if role == "driver":
            run_as = self.driver_run_as_user if self.driver_run_as_user is not None else self.run_as_user
            fs_group = self.driver_fs_group if self.driver_fs_group is not None else self.fs_group
            allow_priv = (
                self.driver_allow_privilege_escalation
                if self.driver_allow_privilege_escalation is not None
                else self.allow_privilege_escalation
            )
        else:
            run_as = self.executor_run_as_user if self.executor_run_as_user is not None else self.run_as_user
            fs_group = self.executor_fs_group if self.executor_fs_group is not None else self.fs_group
            allow_priv = (
                self.executor_allow_privilege_escalation
                if self.executor_allow_privilege_escalation is not None
                else self.allow_privilege_escalation
            )
        return (
            {"fsGroup": int(fs_group)},
            {"runAsUser": int(run_as), "allowPrivilegeEscalation": bool(allow_priv)},
        )

    def _load_kube(self) -> None:
        try:
            if self.in_cluster:
                config.load_incluster_config()
            else:
                config.load_kube_config()
        except Exception as exc:
            raise RuntimeError(
                f"Impossible de charger la config Kubernetes : {exc}. "
                "Vérifier in_cluster=True ou kubeconfig accessible."
            ) from exc
        self._custom_api = client.CustomObjectsApi()
        self._core_api = client.CoreV1Api()

    def _build_spark_app_spec(self, context) -> dict[str, Any]:
        self.name = self._build_unique_name(context)
        af_labels = (
            airflow_labels(context, managed_by=self.managed_by)
            if self.inject_airflow_labels
            else ({"managed-by": sanitize_label_value(self.managed_by)} if self.managed_by else {})
        )
        meta_labels = merge_labels(self.labels, {"project": self.base_name}, af_labels)
        pod_labels = merge_labels({"project": self.base_name}, af_labels)

        env_list = [{"name": k, "value": str(v)} for k, v in self.env.items()]
        driver_mounts, executor_mounts = self._build_volume_mounts()
        volumes_list = self._build_volumes()
        driver_pod_sc, driver_sc = self._security_for("driver")
        executor_pod_sc, executor_sc = self._security_for("executor")

        spec: dict[str, Any] = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {
                "name": self.name,
                "namespace": self.namespace,
                "labels": meta_labels,
            },
            "spec": {
                "type": self.application_type,
                "mode": "cluster",
                "image": self.image,
                "mainApplicationFile": self.main_application_file,
                "arguments": self.arguments,
                "sparkVersion": self.spark_version,
                "restartPolicy": {"type": "Never"},
                "driver": {
                    "volumeMounts": driver_mounts,
                    "cores": self.driver_cores,
                    "memory": self.driver_memory,
                    "coreRequest": self.driver_cores_request,
                    "podSecurityContext": driver_pod_sc,
                    "securityContext": driver_sc,
                    "serviceAccount": self.service_account,
                    "env": env_list,
                    "labels": pod_labels,
                },
                "executor": {
                    "volumeMounts": executor_mounts,
                    "cores": self.executor_cores,
                    "memory": self.executor_memory,
                    "coreRequest": self.executor_cores_request,
                    "podSecurityContext": executor_pod_sc,
                    "securityContext": executor_sc,
                    "instances": self.executor_instances,
                    "env": env_list,
                    "labels": pod_labels,
                },
                "sparkConf": self.spark_conf,
            },
        }

        if volumes_list:
            spec["spec"]["volumes"] = volumes_list
        if self.jars:
            spec["spec"]["deps"] = {"jars": self.jars}
        if self.application_type.lower() in ("scala", "java"):
            if not self.main_class:
                raise ValueError("`main_class` requis pour Scala ou Java")
            spec["spec"]["mainClass"] = self.main_class

        return spec

    def _delete_spark_app(self) -> None:
        try:
            self._custom_api.delete_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=self.namespace,
                plural="sparkapplications",
                name=self.name,
                body=client.V1DeleteOptions(),
            )
            self.log.info("SparkApplication supprimée.")
        except client.exceptions.ApiException as exc:
            if exc.status != 404:
                self.log.warning("Erreur suppression SparkApplication : %s", exc)
        except Exception as exc:
            self.log.warning("Erreur suppression SparkApplication : %s", exc)

    def _force_kill_pods(self) -> None:
        if self._force_killed or self._core_api is None:
            return
        self._force_killed = True
        try:
            pods = self._core_api.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=f"spark-app-name={self.name}",
            )
            for pod in pods.items:
                self.log.warning("Force delete pod %s", pod.metadata.name)
                try:
                    self._core_api.delete_namespaced_pod(
                        name=pod.metadata.name,
                        namespace=self.namespace,
                        body=client.V1DeleteOptions(grace_period_seconds=0),
                    )
                except client.exceptions.ApiException:
                    pass
        except Exception as exc:
            self.log.warning("Force-kill pods échoué : %s", exc)

    def _wait_app_deleted(self) -> None:
        deadline = time.time() + self.timeout_cleanup
        while True:
            try:
                self._custom_api.get_namespaced_custom_object(
                    group="sparkoperator.k8s.io",
                    version="v1beta2",
                    namespace=self.namespace,
                    plural="sparkapplications",
                    name=self.name,
                )
            except client.exceptions.ApiException as exc:
                if exc.status == 404:
                    return
                raise
            if time.time() > deadline:
                raise TimeoutError("Timeout suppression ancienne SparkApplication")
            time.sleep(self.poll_interval)

    def _create_spark_app(self, spec: dict[str, Any]) -> None:
        def _create():
            return self._custom_api.create_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=self.namespace,
                plural="sparkapplications",
                body=spec,
            )

        try:
            k8s_call_with_retry(_create, log=self.log)
        except client.exceptions.ApiException as exc:
            if exc.status == 409:
                self.log.warning("'%s' existe déjà — suppression et re-soumission...", self.name)
                self._delete_spark_app()
                self._wait_app_deleted()
                k8s_call_with_retry(_create, log=self.log)
            elif exc.status == 403:
                raise RuntimeError(
                    f"RBAC insuffisant pour créer une SparkApplication "
                    f"dans '{self.namespace}' (403) : {exc.reason}"
                ) from exc
            elif exc.status in (429, 507):
                raise RuntimeError(
                    f"Quota ou ressources insuffisantes dans '{self.namespace}' "
                    f"({exc.status}) : {exc.reason}"
                ) from exc
            else:
                raise

    def execute(self, context):
        if self.application_type.lower() in ("scala", "java") and not self.main_class:
            raise ValueError("`main_class` requis pour Scala ou Java")

        spec = self._build_spark_app_spec(context)
        summary = resource_summary(spec)
        self.log.info(
            "SparkApplication %s — labels driver=%s",
            self.name,
            summary["labels"]["driver"],
        )

        if self.dry_run:
            self.log.info("dry_run=True — pas de soumission K8s")
            self.log.info("dry_run resource summary: %s", summary)
            return summary

        self._load_kube()
        self.log.info("Soumission SparkApplication '%s'...", self.name)
        self._create_spark_app(spec)

        state = "UNKNOWN"
        try:
            state = self._run_job()
        finally:
            self._delete_spark_app()

        if state == "FAILED":
            raise Exception(f"Le job Spark '{self.name}' a échoué")
        if state in ("DELETED", "UNKNOWN"):
            raise Exception(
                f"Le job Spark '{self.name}' s'est terminé dans un état indéterminé ({state}). "
                "Vérifier les logs du driver."
            )
        return {"state": state, "name": self.name, "labels": summary["labels"]}

    def _get_pod_events(self, pod_name: str) -> list:
        try:
            events = self._core_api.list_namespaced_event(
                namespace=self.namespace,
                field_selector=f"involvedObject.name={pod_name}",
            )
            return events.items
        except client.exceptions.ApiException:
            return []

    def _check_unschedulable(self, pod_name: str) -> None:
        for ev in self._get_pod_events(pod_name):
            if ev.reason == "FailedScheduling":
                msg = f"Pod driver non schedulable : {ev.message}"
                self.log.error(msg)
                if self.fail_on_unschedulable:
                    raise Exception(msg)

    def _check_image_pull_error(self, pod_name: str) -> None:
        for ev in self._get_pod_events(pod_name):
            if ev.reason in ("Failed", "BackOff") and getattr(ev.involvedObject, "field_path", None) == "spec.containers{spark-kubernetes-driver}":
                if any(kw in (ev.message or "") for kw in ("ImagePullBackOff", "ErrImagePull", "pull")):
                    raise Exception(f"Impossible de puller l'image '{self.image}' : {ev.message}")

    def _run_job(self) -> str:
        self.log.info("Recherche du pod driver...")
        driver_pod_name = None
        deadline = time.time() + self.timeout_pod_discovery
        started = time.time()

        while not driver_pod_name:
            pods = self._core_api.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=f"spark-role=driver,spark-app-name={self.name}",
            )
            active = [p for p in pods.items if p.status.phase not in ("Succeeded", "Failed", "Unknown")]
            if active:
                active.sort(key=lambda p: p.metadata.creation_timestamp, reverse=True)
                driver_pod_name = active[0].metadata.name
                self.log.info("Pod driver : %s (phase=%s)", driver_pod_name, active[0].status.phase)
            else:
                if time.time() > deadline:
                    raise TimeoutError(
                        f"Pod driver introuvable après {self.timeout_pod_discovery}s — "
                        "vérifier Spark Operator et ressources."
                    )
                time.sleep(self.poll_interval)

        self.log.info("Attente pod driver Running...")
        deadline = time.time() + self.timeout_pod_running

        while True:
            pod = self._core_api.read_namespaced_pod(driver_pod_name, self.namespace)
            phase = pod.status.phase
            container_statuses = pod.status.container_statuses or []

            if phase == "Pending":
                self._check_unschedulable(driver_pod_name)
                self._check_image_pull_error(driver_pod_name)
            elif phase in ("Failed", "Unknown"):
                reason = ""
                for cs in container_statuses:
                    if cs.name == "spark-kubernetes-driver" and cs.state.terminated:
                        reason = f" (raison: {cs.state.terminated.reason}, exit: {cs.state.terminated.exit_code})"
                raise Exception(f"Pod driver '{driver_pod_name}' en phase {phase}{reason} avant démarrage")

            for cs in container_statuses:
                if cs.name == "spark-kubernetes-driver":
                    if cs.state.waiting and cs.state.waiting.reason in ("CrashLoopBackOff", "Error", "OOMKilled"):
                        raise Exception(
                            f"Pod driver en {cs.state.waiting.reason} — "
                            "vérifier logs et ressources mémoire/CPU"
                        )

            driver_cs = next((cs for cs in container_statuses if cs.name == "spark-kubernetes-driver"), None)
            if phase == "Running" and driver_cs and driver_cs.state.running:
                self.log.info("Pod driver Running : %s", driver_pod_name)
                break

            if time.time() > deadline:
                self._check_unschedulable(driver_pod_name)
                raise TimeoutError(
                    f"Pod driver pas Running après {self.timeout_pod_running}s (phase: {phase})"
                )
            time.sleep(self.poll_interval)

        self.log.info("--- Début logs driver Spark ---")
        try:
            logs = self._core_api.read_namespaced_pod_log(
                driver_pod_name,
                self.namespace,
                follow=True,
                _preload_content=False,
                container="spark-kubernetes-driver",
            )
            for line in logs.stream():
                decoded = line.decode("utf-8").rstrip()
                if any(fragment in decoded for fragment in _NOISY_LOG_FRAGMENTS):
                    self.log.debug(decoded)
                else:
                    self.log.info(decoded)
        except Exception as exc:
            self.log.warning("Interruption lecture logs driver : %s", exc)
        finally:
            self.log.info("--- Fin logs driver Spark ---")

        self.log.info("Attente fin SparkApplication...")
        deadline = time.time() + self.timeout_job
        consecutive_errors = 0

        while True:
            elapsed = time.time() - started
            handle_job_timeout(
                elapsed=elapsed,
                timeout=float(self.timeout_job),
                namespace=self.namespace,
                app_name=self.name,
                delete_app=self._delete_spark_app,
                force_kill_pods=self._force_kill_pods,
                log=self.log,
            )
            if elapsed >= self.timeout_job + 30:
                raise TimeoutError(
                    f"SparkApplication '{self.name}' toujours en cours après {self.timeout_job}s "
                    "(force-kill tenté)"
                )

            try:
                app = self._custom_api.get_namespaced_custom_object(
                    group="sparkoperator.k8s.io",
                    version="v1beta2",
                    namespace=self.namespace,
                    plural="sparkapplications",
                    name=self.name,
                )
                consecutive_errors = 0
                state = app.get("status", {}).get("applicationState", {}).get("state")
                if state in ("COMPLETED", "FAILED"):
                    self.log.info("SparkApplication terminée : %s", state)
                    return state
            except client.exceptions.ApiException as exc:
                if exc.status == 404:
                    return self._resolve_state_from_driver_pod(driver_pod_name)
                if exc.status >= 500:
                    consecutive_errors += 1
                    self.log.warning(
                        "Erreur API server (%s) poll (%s/%s)",
                        exc.status,
                        consecutive_errors,
                        self.api_error_retries,
                    )
                    if consecutive_errors > self.api_error_retries:
                        raise RuntimeError(
                            f"API server K8s indisponible après {self.api_error_retries} tentatives"
                        ) from exc
                else:
                    raise

            if time.time() > deadline and elapsed < self.timeout_job + 30:
                # laisser handle_job_timeout gérer delete + grace
                pass
            time.sleep(self.poll_interval)

    def _resolve_state_from_driver_pod(self, driver_pod_name: str) -> str:
        self.log.warning(
            "SparkApplication introuvable (404) — vérification pod driver '%s'...",
            driver_pod_name,
        )
        try:
            pod = self._core_api.read_namespaced_pod(driver_pod_name, self.namespace)
            phase = pod.status.phase
            if phase == "Succeeded":
                return "COMPLETED"
            if phase == "Failed":
                return "FAILED"
            if phase in ("Running", "Pending"):
                time.sleep(30)
                pod = self._core_api.read_namespaced_pod(driver_pod_name, self.namespace)
                phase = pod.status.phase
                if phase == "Succeeded":
                    return "COMPLETED"
                if phase == "Failed":
                    return "FAILED"
                return "DELETED"
            return "UNKNOWN"
        except client.exceptions.ApiException as exc:
            if exc.status != 404:
                self.log.error("Erreur lecture pod driver : %s", exc)
            return "UNKNOWN"
        except Exception as exc:
            self.log.error("Erreur résolution état pod driver : %s", exc)
            return "UNKNOWN"

    def on_kill(self) -> None:
        try:
            self._load_kube()
        except Exception:
            pass
        if self._custom_api is None:
            return
        self.log.warning("Kill reçu — suppression SparkApplication '%s'...", self.name)
        self._delete_spark_app()
        self._force_kill_pods()
