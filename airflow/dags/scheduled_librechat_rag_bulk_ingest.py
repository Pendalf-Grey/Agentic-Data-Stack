import hashlib
import json
import mimetypes
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task


# Этот DAG индексирует документы из общей директории в штатный LibreChat RAG API.
# Airflow и rag_api видят один и тот же host volume, поэтому используем /local/embed:
# Airflow передает путь к файлу, а rag_api читает файл локально и кладет чанки в pgvector.

RAG_API_URL = os.getenv("LIBRECHAT_RAG_API_URL", "http://rag_api:8000").rstrip("/")
RAG_BULK_DIR = Path(os.getenv("LIBRECHAT_RAG_BULK_DIR", "/opt/airflow/rag_bulk/incoming"))
RAG_BULK_CRON = os.getenv("AIRFLOW_LIBRECHAT_RAG_BULK_CRON", "*/10 * * * *")
RAG_ENTITY_ID = os.getenv("LIBRECHAT_RAG_BULK_ENTITY_ID", "public").strip() or "public"
LIBRECHAT_MONGO_URI = os.getenv("LIBRECHAT_MONGO_URI", "mongodb://librechat-db:27017/LibreChat")
LIBRECHAT_RAG_BULK_USER_EMAIL = os.getenv("LIBRECHAT_RAG_BULK_USER_EMAIL", "").strip()
DAG_PAUSED = os.getenv("AIRFLOW_DAG_PAUSED", "true").strip().lower() == "true"

MANIFEST_PATH = RAG_BULK_DIR.parent / ".airflow_rag_manifest.json"
SUPPORTED_EXTENSIONS = {
    ".csv",
    ".doc",
    ".docx",
    ".html",
    ".json",
    ".md",
    ".pdf",
    ".ppt",
    ".pptx",
    ".rst",
    ".text",
    ".txt",
    ".xls",
    ".xlsx",
    ".xml",
}


def read_manifest() -> dict:
    if not MANIFEST_PATH.exists():
        return {"files": {}}
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def write_manifest(manifest: dict) -> None:
    MANIFEST_PATH.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_file_id(relative_path: str, content_hash: str) -> str:
    stem = re.sub(r"[^a-zA-Z0-9_-]+", "_", relative_path).strip("_").lower()
    return f"airflow_bulk_{stem}_{content_hash[:16]}"


def json_request(path: str, payload, method: str = "POST") -> dict:
    request = urllib.request.Request(
        f"{RAG_API_URL}{path}",
        data=json.dumps(payload).encode("utf-8"),
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=900) as response:
            data = response.read().decode("utf-8")
            return json.loads(data) if data else {}
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} failed with {error.code}: {detail}") from error


def delete_document(file_id: str) -> None:
    try:
        json_request("/documents", [file_id], method="DELETE")
    except RuntimeError as error:
        if "failed with 404" not in str(error):
            raise


def get_livrechat_users(db):
    query = {"email": LIBRECHAT_RAG_BULK_USER_EMAIL} if LIBRECHAT_RAG_BULK_USER_EMAIL else {}
    users = list(db.users.find(query, {"_id": 1, "email": 1}))
    if not users:
        target = LIBRECHAT_RAG_BULK_USER_EMAIL or "any LibreChat user"
        raise RuntimeError(f"Cannot sync RAG files to LibreChat UI: no user found for {target}")
    return users


def sync_livrechat_file_records(*, relative_path: str, path: Path, file_id: str, content_hash: str, content_type: str) -> list[dict]:
    try:
        from pymongo import MongoClient
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "pymongo is required for LibreChat file-panel sync. "
            "Set AIRFLOW_PIP_ADDITIONAL_REQUIREMENTS=pymongo==4.7.3 and recreate Airflow containers."
        ) from error

    client = MongoClient(LIBRECHAT_MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client.get_default_database()
    now = datetime.utcnow()
    synced = []
    for user in get_livrechat_users(db):
        db.files.update_one(
            {"user": user["_id"], "file_id": file_id},
            {
                "$set": {
                    "bytes": path.stat().st_size,
                    "filename": path.name,
                    "filepath": "vectordb",
                    "embedded": True,
                    "type": content_type,
                    "source": "local",
                    "context": "message_attachment",
                    "metadata": {
                        "bulkRag": True,
                        "relativePath": relative_path,
                        "sha256": content_hash,
                    },
                    "updatedAt": now,
                },
                "$setOnInsert": {
                    "user": user["_id"],
                    "file_id": file_id,
                    "object": "file",
                    "usage": 0,
                    "createdAt": now,
                },
            },
            upsert=True,
        )
        synced.append({"user": str(user["_id"]), "email": user.get("email"), "file_id": file_id})
    return synced


def delete_livrechat_file_records(file_id: str) -> None:
    try:
        from pymongo import MongoClient
    except ModuleNotFoundError:
        return
    client = MongoClient(LIBRECHAT_MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client.get_default_database()
    db.files.delete_many({"file_id": file_id, "metadata.bulkRag": True})


def iter_supported_files() -> list[Path]:
    if not RAG_BULK_DIR.exists():
        return []
    return sorted(
        path
        for path in RAG_BULK_DIR.rglob("*")
        if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS
    )


def iter_unsupported_files() -> list[Path]:
    if not RAG_BULK_DIR.exists():
        return []
    return sorted(
        path
        for path in RAG_BULK_DIR.rglob("*")
        if path.is_file()
        and not path.name.startswith(".")
        and path.suffix.lower() not in SUPPORTED_EXTENSIONS
    )


@dag(
    dag_id="scheduled_librechat_rag_bulk_ingest",
    description="Scan a shared bulk directory and index documents into LibreChat RAG pgvector.",
    schedule=RAG_BULK_CRON,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=DAG_PAUSED,
    tags=["agentic-data-stack", "librechat", "rag", "pgvector", "bulk"],
)
def scheduled_librechat_rag_bulk_ingest():
    @task
    def ingest_bulk_documents() -> dict:
        manifest = read_manifest()
        indexed = []
        skipped = []
        removed = []
        unsupported = []
        failed = []

        current_relative_paths = {
            str(path.relative_to(RAG_BULK_DIR))
            for path in iter_supported_files()
        }
        for relative_path, previous in list(manifest.get("files", {}).items()):
            if relative_path in current_relative_paths:
                continue
            file_id = previous.get("file_id")
            if file_id:
                delete_document(file_id)
                delete_livrechat_file_records(file_id)
            manifest["files"].pop(relative_path, None)
            removed.append({"path": relative_path, "file_id": file_id})

        unsupported = [
            {
                "path": str(path.relative_to(RAG_BULK_DIR)),
                "reason": f"Unsupported extension: {path.suffix or '<none>'}",
            }
            for path in iter_unsupported_files()
        ]

        for path in iter_supported_files():
            relative_path = str(path.relative_to(RAG_BULK_DIR))
            content_hash = file_sha256(path)
            previous = manifest["files"].get(relative_path)
            if previous and previous.get("sha256") == content_hash:
                synced = sync_livrechat_file_records(
                    relative_path=relative_path,
                    path=path,
                    file_id=previous["file_id"],
                    content_hash=content_hash,
                    content_type=previous.get("content_type") or mimetypes.guess_type(path.name)[0] or "text/plain",
                )
                skipped.append({"path": relative_path, "file_id": previous["file_id"], "synced": synced})
                continue

            file_id = safe_file_id(relative_path, content_hash)
            content_type = mimetypes.guess_type(path.name)[0] or "text/plain"
            if previous and previous.get("file_id"):
                delete_document(previous["file_id"])
                delete_livrechat_file_records(previous["file_id"])

            payload = {
                "filepath": relative_path,
                "filename": path.name,
                "file_content_type": content_type,
                "file_id": file_id,
            }
            try:
                entity = urllib.parse.quote(RAG_ENTITY_ID, safe="")
                result = json_request(f"/local/embed?entity_id={entity}", payload)
                manifest["files"][relative_path] = {
                    "file_id": file_id,
                    "sha256": content_hash,
                    "content_type": content_type,
                    "indexed_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                }
                synced = sync_livrechat_file_records(
                    relative_path=relative_path,
                    path=path,
                    file_id=file_id,
                    content_hash=content_hash,
                    content_type=content_type,
                )
                indexed.append({"path": relative_path, "file_id": file_id, "result": result, "synced": synced})
            except Exception as error:
                failed.append({"path": relative_path, "error": str(error)})

        write_manifest(manifest)
        if failed:
            raise RuntimeError(
                json.dumps(
                    {
                        "indexed": indexed,
                        "skipped": skipped,
                        "removed": removed,
                        "unsupported": unsupported,
                        "failed": failed,
                    },
                    ensure_ascii=False,
                )
            )
        return {
            "indexed": indexed,
            "skipped": skipped,
            "removed": removed,
            "unsupported": unsupported,
            "failed": failed,
        }

    ingest_bulk_documents()


scheduled_librechat_rag_bulk_ingest()
