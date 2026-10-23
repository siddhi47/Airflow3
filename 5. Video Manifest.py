"""
Probe new videos with ffprobe and keep a manifest.json next to them.

Two DAGs, connected by the `raw_videos` asset:

    detect_new_videos   every 2 min: list videos/, compare with manifest.json,
          │             emit an asset event listing the new files
          ▼
    probe_videos        on each asset event: ffprobe every new file in its own
                        pod, then merge the results into manifest.json

Storage is an ObjectStoragePath, so the same code works on the local test
folder and on S3. Switch with two Airflow Variables (Admin > Variables):

    videos_root      file:///opt/airflow/test-bucket   (default, local test)
                     s3://raw-vidoes                    (real bucket)
    videos_conn_id   aws-siddhi                         (only needed for s3://)

Locally, upload by copying into /var/lib/airflow-data/test-bucket/videos/ on
the CPU node.
"""

import json
from datetime import datetime, timezone

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import Asset, ObjectStoragePath, Variable, dag, task
from kubernetes.client import models as k8s

raw_videos = Asset(name="raw_videos")

VIDEO_SUFFIXES = {".mp4", ".mov", ".mkv", ".webm", ".avi", ".m4v", ".mpg", ".mpeg", ".ts"}
# A local file this new may still be being copied. S3 objects only appear once
# the upload completes, so this is just a guard for the local test folder.
SETTLE_SECONDS = 30
FFPROBE_IMAGE = "10.0.0.132:30500/ffprobe:1"


def _root() -> ObjectStoragePath:
    url = Variable.get("videos_root", default="file:///opt/airflow/test-bucket")
    conn_id = Variable.get("videos_conn_id", default=None)
    return ObjectStoragePath(url, conn_id=conn_id)


def _read_manifest(root: ObjectStoragePath) -> dict:
    path = root / "manifest.json"
    if not path.exists():
        return {"videos": {}}
    return json.loads(path.read_text())


def _modified(stat: dict) -> datetime:
    # Local filesystems report "mtime" (epoch seconds); S3 reports "LastModified".
    if "LastModified" in stat:
        return stat["LastModified"]
    return datetime.fromtimestamp(stat["mtime"], tz=timezone.utc)


@dag(
    dag_id="detect_new_videos",
    schedule="*/2 * * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["videos"],
)
def detect_new_videos():
    @task(outlets=[raw_videos])
    def detect(*, outlet_events):
        root = _root()
        known = _read_manifest(root)["videos"]
        now = datetime.now(timezone.utc)

        new = []
        for path in (root / "videos").rglob("*"):
            if not path.is_file() or path.suffix.lower() not in VIDEO_SUFFIXES:
                continue
            key = str(path.relative_to(root))  # e.g. "videos/clip.mp4"
            if key in known:
                continue
            stat = path.stat()
            if (now - _modified(stat)).total_seconds() < SETTLE_SECONDS:
                print(f"{key}: modified <{SETTLE_SECONDS}s ago, next run")
                continue
            new.append({"key": key, "size": stat["size"]})

        if not new:
            # Skipped tasks emit no asset event, so probe_videos doesn't run.
            raise AirflowSkipException("no new videos")

        print(f"{len(new)} new: {[v['key'] for v in new]}")
        outlet_events[raw_videos].extra = {"new": new}

    detect()


@dag(
    dag_id="probe_videos",
    schedule=[raw_videos],
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    # Each run rewrites manifest.json; two at once would lose updates.
    max_active_runs=1,
    tags=["videos"],
)
def probe_videos():
    @task
    def collect(*, triggering_asset_events) -> list[list[str]]:
        """Turn the asset events into one [input, key] pair per file to probe."""
        root = _root()
        known = _read_manifest(root)["videos"]

        # Several detector events can be folded into one run; de-duplicate, and
        # drop anything an earlier run already probed.
        pending = {}
        for event in triggering_asset_events[raw_videos]:
            for video in event.extra.get("new", []):
                if video["key"] not in known:
                    pending[video["key"]] = video
        if not pending:
            raise AirflowSkipException("everything already probed")

        if root.protocol == "file":
            # The ffprobe pod mounts the same host folder at /data.
            return [[f"/data/{key}", key] for key in sorted(pending)]

        # S3: ffprobe reads a presigned URL directly, fetching only the bytes it
        # needs (headers/moov), not the whole video.
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        s3 = S3Hook(aws_conn_id=Variable.get("videos_conn_id", default=None))
        bucket = root.bucket
        return [
            [s3.generate_presigned_url("get_object", params={"Bucket": bucket, "Key": key}, expires_in=3600), key]
            for key in sorted(pending)
        ]

    # One pod per video, from an ffprobe image in the homelab registry. The
    # image's probe script writes its JSON result to /airflow/xcom/return.json,
    # which do_xcom_push hands back to Airflow.
    probe = KubernetesPodOperator.partial(
        task_id="ffprobe",
        name="ffprobe",
        namespace="airflow-tasks",
        image=FFPROBE_IMAGE,
        node_selector={"homelab.local/cpu": "true"},
        volumes=[
            k8s.V1Volume(
                name="test-bucket",
                host_path=k8s.V1HostPathVolumeSource(path="/var/lib/airflow-data/test-bucket"),
            )
        ],
        volume_mounts=[k8s.V1VolumeMount(name="test-bucket", mount_path="/data", read_only=True)],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "64Mi"}, limits={"memory": "256Mi"}
        ),
        do_xcom_push=True,
        get_logs=True,
        on_finish_action="delete_pod",
    ).expand(arguments=collect())

    @task
    def update_manifest(results):
        root = _root()
        manifest = _read_manifest(root)
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")

        for result in results:
            key = result["key"]
            if "error" in result:
                manifest["videos"][key] = {"probed_at": now, "error": result["error"]}
                continue
            fmt = result["probe"].get("format", {})
            streams = result["probe"].get("streams", [])
            video = next((s for s in streams if s.get("codec_type") == "video"), {})
            audio = next((s for s in streams if s.get("codec_type") == "audio"), {})
            manifest["videos"][key] = {
                "probed_at": now,
                "size_bytes": int(fmt.get("size", 0)),
                "duration_s": float(fmt.get("duration", 0)),
                "bit_rate": int(fmt.get("bit_rate", 0)),
                "container": fmt.get("format_name"),
                "video": {
                    "codec": video.get("codec_name"),
                    "width": video.get("width"),
                    "height": video.get("height"),
                    "fps": video.get("avg_frame_rate"),
                    "pix_fmt": video.get("pix_fmt"),
                } if video else None,
                "audio": {
                    "codec": audio.get("codec_name"),
                    "channels": audio.get("channels"),
                    "sample_rate": audio.get("sample_rate"),
                } if audio else None,
            }

        manifest["updated_at"] = now
        manifest["count"] = len(manifest["videos"])
        body = json.dumps(manifest, indent=2, sort_keys=True)
        if root.protocol == "file":
            # Write-then-rename so a reader never sees a half-written file.
            tmp = root / "manifest.json.tmp"
            tmp.write_text(body)
            tmp.rename(root / "manifest.json")
        else:
            (root / "manifest.json").write_text(body)  # S3 PUTs are atomic
        print(f"manifest.json: {manifest['count']} videos ({len(results)} updated)")

    update_manifest(probe.output)


detect_new_videos()
probe_videos()
