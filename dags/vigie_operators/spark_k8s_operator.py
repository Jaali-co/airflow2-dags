import hashlib
import re
import time

from airflow.models import BaseOperator
from kubernetes import client, config


# Préfixes de logs Spark considérés comme du bruit — réduits à DEBUG
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
    Soumet un SparkApplication à Kubernetes via le Spark Operator.

    Robustesse :
    - Timeouts séparés par phase (discovery, running, job, cleanup)
    - Cleanup garanti via try/finally
    - Nom unique par DAG run/try pour éviter les collisions (S27/S29)
    - Détection ImagePullBackOff / CrashLoopBackOff (S11/S14)
    - Détection Unschedulable via events K8s (S15)
    - Erreurs K8s explicites : RBAC, quota à la soumission (S6/S8)
    - Poll état final : erreurs 5xx intermittentes tolérées (S22)
    - 404 SparkApp → vérification pod driver avec attente race condition 30s (S20)
    - Logs Spark bruités filtrés en DEBUG
    """

    template_fields = (
        "base_name", "namespace", "image", "main_application_file",
        "application_type", "main_class", "arguments", "spark_version",
        "driver_memory", "executor_memory", "spark_conf", "labels",
        "env", "jars", "pvc_configs", "emptydir_configs",
    )

    def __init__(
        self,
        name: str,
        namespace: str,
        image: str,
        main_application_file: str,
        application_type: str = "Python",
        main_class: str = None,
        arguments: list = None,
        spark_version: str = "3.1.1",
        driver_cores: int = 1,
        driver_cores_request: str = "20m",
        driver_memory: str = "512m",
        executor_cores: int = 1,
        executor_cores_request: str = "400m",
        executor_memory: str = "512m",
        executor_instances: int = 2,
        service_account: str = "spark",
        spark_conf: dict = None,
        labels: dict = None,
        env: dict = None,
        # Timeouts séparés par phase
        timeout_pod_discovery: int = 120,
        timeout_pod_running: int = 300,
        timeout_job: int = 3600,
        timeout_cleanup: int = 60,
        # Alias legacy
        timeout: int = None,
        poll_interval: int = 10,
        # Nombre de retries tolérés sur erreurs 5xx lors du poll état final
        api_error_retries: int = 5,
        in_cluster: bool = True,
        jars: list = None,
        pvc_configs: list = None,
        emptydir_configs: list = None,
        fail_on_unschedulable: bool = True,
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
        self.spark_conf = spark_conf or {}
        self.labels = labels or {}
        self.env = env or {}
        self.poll_interval = poll_interval
        self.api_error_retries = api_error_retries
        self.in_cluster = in_cluster
        self.jars = jars or []
        self.pvc_configs = pvc_configs or []
        self.emptydir_configs = emptydir_configs or []
        self.fail_on_unschedulable = fail_on_unschedulable

        # Gestion alias legacy timeout=
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

    def _sanitize_k8s_name(self, value: str) -> str:
        value = value.lower()
        value = re.sub(r"[^a-z0-9-]", "-", value)
        value = re.sub(r"-+", "-", value)
        return value.strip("-")

    def _build_unique_name(self, context) -> str:
        """Génère un nom unique par DAG run + try number."""
        MAX_LEN = 56
        raw = (
            f"{self.base_name}-"
            f"{context['dag'].dag_id}-"
            f"{context['task'].task_id}-"
            f"{context['ts_nodash']}-"
            f"try{context['ti'].try_number}"
        )
        sanitized = self._sanitize_k8s_name(raw)
        if len(sanitized) <= MAX_LEN:
            return sanitized
        hash_suffix = hashlib.sha1(sanitized.encode()).hexdigest()[:8]
        return f"{sanitized[:MAX_LEN - 9]}-{hash_suffix}"

    def _build_volume_mounts(self) -> tuple:
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
            volumes.append({
                "name": pvc.get("name", pvc["claim_name"]),
                "persistentVolumeClaim": {"claimName": pvc["claim_name"]},
            })
        for ed in self.emptydir_configs:
            spec = {}
            if ed.get("size_limit"):
                spec["sizeLimit"] = ed["size_limit"]
            volumes.append({"name": ed["name"], "emptyDir": spec})
        return volumes

    def _delete_spark_app(self):
        """Supprime la SparkApplication sans lever d'exception."""
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
        except client.exceptions.ApiException as e:
            if e.status != 404:
                self.log.warning(f"Erreur suppression SparkApplication : {e}")
        except Exception as e:
            self.log.warning(f"Erreur suppression SparkApplication : {e}")

    def _wait_app_deleted(self):
        """Attend la disparition effective de la SparkApplication."""
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
            except client.exceptions.ApiException as e:
                if e.status == 404:
                    return
                raise
            if time.time() > deadline:
                raise TimeoutError("Timeout suppression ancienne SparkApplication")
            time.sleep(self.poll_interval)

    def _get_pod_events(self, pod_name: str) -> list:
        try:
            events = self._core_api.list_namespaced_event(
                namespace=self.namespace,
                field_selector=f"involvedObject.name={pod_name}",
            )
            return events.items
        except client.exceptions.ApiException:
            return []

    def _check_unschedulable(self, pod_name: str):
        for ev in self._get_pod_events(pod_name):
            if ev.reason == "FailedScheduling":
                msg = f"Pod driver non schedulable : {ev.message}"
                self.log.error(msg)
                if self.fail_on_unschedulable:
                    raise Exception(msg)

    def _check_image_pull_error(self, pod_name: str):
        for ev in self._get_pod_events(pod_name):
            if ev.reason in ("Failed", "BackOff") and ev.involvedObject.field_path == "spec.containers{spark-kubernetes-driver}":
                if any(kw in (ev.message or "") for kw in ("ImagePullBackOff", "ErrImagePull", "pull")):
                    raise Exception(
                        f"Impossible de puller l'image '{self.image}' : {ev.message}"
                    )

    def execute(self, context):
        try:
            if self.in_cluster:
                config.load_incluster_config()
            else:
                config.load_kube_config()
        except Exception as e:
            raise RuntimeError(
                f"Impossible de charger la config Kubernetes : {e}. "
                "Vérifier que le pod tourne bien dans le cluster (in_cluster=True) "
                "ou que kubeconfig est accessible."
            )

        self._custom_api = client.CustomObjectsApi()
        self._core_api = client.CoreV1Api()

        if self.application_type.lower() in ["scala", "java"] and not self.main_class:
            raise ValueError("`main_class` requis pour Scala ou Java")

        self.name = self._build_unique_name(context)
        self.log.info(f"Nom SparkApplication : {self.name}")

        env_list = [{"name": k, "value": str(v)} for k, v in self.env.items()]
        driver_mounts, executor_mounts = self._build_volume_mounts()
        volumes_list = self._build_volumes()

        spec = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {
                "name": self.name,
                "namespace": self.namespace,
                "labels": {**self.labels, "project": self.base_name},
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
                    "podSecurityContext": {"fsGroup": 0},
                    "securityContext": {"runAsUser": 0, "allowPrivilegeEscalation": False},
                    "serviceAccount": self.service_account,
                    "env": env_list,
                    "labels": {"project": self.base_name},
                },
                "executor": {
                    "volumeMounts": executor_mounts,
                    "cores": self.executor_cores,
                    "memory": self.executor_memory,
                    "coreRequest": self.executor_cores_request,
                    "podSecurityContext": {"fsGroup": 0},
                    "securityContext": {"runAsUser": 0, "allowPrivilegeEscalation": False},
                    "instances": self.executor_instances,
                    "env": env_list,
                    "labels": {"project": self.base_name},
                },
                "sparkConf": self.spark_conf,
            },
        }

        if volumes_list:
            spec["spec"]["volumes"] = volumes_list
        if self.jars:
            spec["spec"]["deps"] = {"jars": self.jars}
        if self.application_type.lower() in ["scala", "java"]:
            spec["spec"]["mainClass"] = self.main_class

        self.log.info(f"Soumission SparkApplication '{self.name}'...")
        try:
            self._custom_api.create_namespaced_custom_object(
                group="sparkoperator.k8s.io", version="v1beta2",
                namespace=self.namespace, plural="sparkapplications", body=spec,
            )
        except client.exceptions.ApiException as e:
            if e.status == 409:
                self.log.warning(f"'{self.name}' existe déjà — suppression et re-soumission...")
                self._delete_spark_app()
                self._wait_app_deleted()
                self._custom_api.create_namespaced_custom_object(
                    group="sparkoperator.k8s.io", version="v1beta2",
                    namespace=self.namespace, plural="sparkapplications", body=spec,
                )
            elif e.status == 403:
                raise RuntimeError(
                    f"RBAC insuffisant pour créer une SparkApplication "
                    f"dans le namespace '{self.namespace}' (403) : {e.reason}"
                )
            elif e.status in (429, 507):
                raise RuntimeError(
                    f"Quota ou ressources insuffisantes dans le namespace "
                    f"'{self.namespace}' ({e.status}) : {e.reason}"
                )
            else:
                raise

        state = "UNKNOWN"
        try:
            state = self._run_job()
        finally:
            self._delete_spark_app()

        if state == "FAILED":
            raise Exception(f"Le job Spark '{self.name}' a échoué")
        if state in ("DELETED", "UNKNOWN"):
            raise Exception(
                f"Le job Spark '{self.name}' s'est terminé dans un état indéterminé ({state}) — "
                "SparkApplication supprimée avant la fin ou état pod driver non conclusif. "
                "Vérifier les logs du driver."
            )

    def _run_job(self) -> str:
        self.log.info("Recherche du pod driver...")
        driver_pod_name = None
        deadline = time.time() + self.timeout_pod_discovery

        while not driver_pod_name:
            pods = self._core_api.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=f"spark-role=driver,spark-app-name={self.name}",
            )
            active = [
                p for p in pods.items
                if p.status.phase not in ("Succeeded", "Failed", "Unknown")
            ]
            if active:
                active.sort(key=lambda p: p.metadata.creation_timestamp, reverse=True)
                driver_pod_name = active[0].metadata.name
                self.log.info(
                    f"Pod driver : {driver_pod_name} "
                    f"(phase={active[0].status.phase})"
                )
            else:
                if time.time() > deadline:
                    raise TimeoutError(
                        f"Pod driver introuvable après {self.timeout_pod_discovery}s — "
                        "Spark Operator a peut-être échoué à créer le pod. "
                        "Vérifier les logs du Spark Operator et les ressources du cluster."
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
                        reason = (
                            f" (raison: {cs.state.terminated.reason}, "
                            f"exit: {cs.state.terminated.exit_code})"
                        )
                raise Exception(
                    f"Pod driver '{driver_pod_name}' en phase {phase}{reason} avant démarrage"
                )

            for cs in container_statuses:
                if cs.name == "spark-kubernetes-driver":
                    if cs.state.waiting and cs.state.waiting.reason in (
                        "CrashLoopBackOff", "Error", "OOMKilled"
                    ):
                        raise Exception(
                            f"Pod driver en {cs.state.waiting.reason} — "
                            "vérifier les logs et les ressources mémoire/CPU"
                        )

            driver_cs = next(
                (cs for cs in container_statuses if cs.name == "spark-kubernetes-driver"),
                None,
            )
            if phase == "Running" and driver_cs and driver_cs.state.running:
                self.log.info(f"Pod driver Running : {driver_pod_name}")
                break

            if time.time() > deadline:
                self._check_unschedulable(driver_pod_name)
                raise TimeoutError(
                    f"Pod driver pas Running après {self.timeout_pod_running}s "
                    f"(phase: {phase}) — vérifier les ressources et l'image"
                )
            time.sleep(self.poll_interval)

        self.log.info("--- Début logs driver Spark ---")
        try:
            logs = self._core_api.read_namespaced_pod_log(
                driver_pod_name, self.namespace,
                follow=True, _preload_content=False,
                container="spark-kubernetes-driver",
            )
            for line in logs.stream():
                decoded = line.decode("utf-8").rstrip()
                if any(fragment in decoded for fragment in _NOISY_LOG_FRAGMENTS):
                    self.log.debug(decoded)
                else:
                    self.log.info(decoded)
        except Exception as e:
            self.log.warning(f"Interruption lecture logs driver : {e}")
        finally:
            self.log.info("--- Fin logs driver Spark ---")

        self.log.info("Attente fin SparkApplication...")
        deadline = time.time() + self.timeout_job
        consecutive_errors = 0

        while True:
            try:
                app = self._custom_api.get_namespaced_custom_object(
                    group="sparkoperator.k8s.io", version="v1beta2",
                    namespace=self.namespace, plural="sparkapplications",
                    name=self.name,
                )
                consecutive_errors = 0
                state = app.get("status", {}).get("applicationState", {}).get("state")

                if state not in ("COMPLETED", "FAILED", None):
                    self.log.debug(f"SparkApplication état intermédiaire : {state}")

                if state in ("COMPLETED", "FAILED"):
                    self.log.info(f"SparkApplication terminée : {state}")
                    return state

            except client.exceptions.ApiException as e:
                if e.status == 404:
                    return self._resolve_state_from_driver_pod(driver_pod_name)
                elif e.status >= 500:
                    consecutive_errors += 1
                    self.log.warning(
                        f"Erreur API server ({e.status}) lors du poll "
                        f"({consecutive_errors}/{self.api_error_retries}) — retry..."
                    )
                    if consecutive_errors > self.api_error_retries:
                        raise RuntimeError(
                            f"API server K8s indisponible après {self.api_error_retries} "
                            f"tentatives consécutives ({e.status})"
                        )
                else:
                    raise

            if time.time() > deadline:
                raise TimeoutError(
                    f"SparkApplication '{self.name}' toujours en cours "
                    f"après {self.timeout_job}s"
                )
            time.sleep(self.poll_interval)

    def _resolve_state_from_driver_pod(self, driver_pod_name: str) -> str:
        self.log.warning(
            f"SparkApplication introuvable (404) — "
            f"vérification pod driver '{driver_pod_name}'..."
        )
        try:
            pod = self._core_api.read_namespaced_pod(driver_pod_name, self.namespace)
            phase = pod.status.phase

            if phase == "Succeeded":
                self.log.info("Pod driver Succeeded → COMPLETED")
                return "COMPLETED"
            elif phase == "Failed":
                self.log.error("Pod driver Failed → FAILED")
                return "FAILED"
            elif phase in ("Running", "Pending"):
                self.log.warning(
                    f"Pod driver encore en phase {phase} — "
                    "attente 30s propagation K8s..."
                )
                time.sleep(30)
                try:
                    pod = self._core_api.read_namespaced_pod(driver_pod_name, self.namespace)
                    phase = pod.status.phase
                    self.log.info(f"Pod driver phase après 30s : {phase}")
                    if phase == "Succeeded":
                        return "COMPLETED"
                    elif phase == "Failed":
                        return "FAILED"
                except Exception as e:
                    self.log.warning(f"Erreur re-lecture pod driver : {e}")

                self.log.error(
                    f"Pod driver toujours en phase {phase} après 30s — "
                    "suppression externe probable"
                )
                return "DELETED"
            else:
                self.log.error(f"Phase pod driver inattendue : {phase}")
                return "UNKNOWN"

        except client.exceptions.ApiException as e:
            if e.status == 404:
                self.log.error("Pod driver introuvable — état final indéterminé")
            else:
                self.log.error(f"Erreur lecture pod driver : {e}")
            return "UNKNOWN"
        except Exception as e:
            self.log.error(f"Erreur résolution état pod driver : {e}")
            return "UNKNOWN"

    def on_kill(self):
        try:
            if self.in_cluster:
                config.load_incluster_config()
            else:
                config.load_kube_config()
        except Exception:
            pass

        if self._custom_api is None:
            self._custom_api = client.CustomObjectsApi()

        self.log.warning(f"Kill reçu — suppression SparkApplication '{self.name}'...")
        self._delete_spark_app()

    def on_success(self, context):
        self.log.info(f"SparkApplication '{self.name}' terminée avec succès")
