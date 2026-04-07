import json
import importlib.util
import sys
import threading
import time
import uuid
from contextlib import redirect_stdout, redirect_stderr
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

HOST = "localhost"
PORT = 8080
ROOT_DIR = Path(__file__).resolve().parent
IMPORT_JOBS = {}
IMPORT_JOBS_LOCK = threading.Lock()


def load_etl_main():
    package_dir = ROOT_DIR / "etl"
    package_name = "_etl_pkg"

    if package_name not in sys.modules:
        package_spec = importlib.util.spec_from_file_location(
            package_name,
            package_dir / "__init__.py",
            submodule_search_locations=[str(package_dir)],
        )
        package_module = importlib.util.module_from_spec(package_spec)
        sys.modules[package_name] = package_module
        package_spec.loader.exec_module(package_module)

    module_name = f"{package_name}.main"
    if module_name not in sys.modules:
        module_spec = importlib.util.spec_from_file_location(
            module_name,
            package_dir / "main.py",
        )
        module = importlib.util.module_from_spec(module_spec)
        sys.modules[module_name] = module
        module_spec.loader.exec_module(module)

    return sys.modules[module_name].main


def average_seed_features(seed_tracks):
    from similarity_search import NUMERIC_FEATURES

    if not seed_tracks:
        raise ValueError("No seed tracks provided.")

    averages = {}
    for feature in NUMERIC_FEATURES:
        values = [float(track.get(feature, 0) or 0) for track in seed_tracks]
        averages[feature] = sum(values) / len(values)
    return averages


class JobLogWriter:
    def __init__(self, job_id):
        self.job_id = job_id
        self._buffer = ""

    def write(self, chunk):
        if not chunk:
            return 0

        self._buffer += chunk
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            self._append_line(line.rstrip())
        return len(chunk)

    def flush(self):
        if self._buffer.strip():
            self._append_line(self._buffer.rstrip())
        self._buffer = ""

    def _append_line(self, line):
        if not line:
            return
        with IMPORT_JOBS_LOCK:
            job = IMPORT_JOBS.get(self.job_id)
            if not job:
                return
            job["logs"].append(f"[{time.strftime('%I:%M:%S %p')}] {line}")


def start_import_job():
    job_id = uuid.uuid4().hex
    with IMPORT_JOBS_LOCK:
        IMPORT_JOBS[job_id] = {
            "status": "running",
            "logs": [f"[{time.strftime('%I:%M:%S %p')}] Starting import job..."],
            "result": None,
            "error": "",
        }

    def run_job():
        writer = JobLogWriter(job_id)
        try:
            run_etl_main = load_etl_main()
            from clustering import main as run_clustering_main

            with IMPORT_JOBS_LOCK:
                IMPORT_JOBS[job_id]["logs"].append(f"[{time.strftime('%I:%M:%S %p')}] Running etl.main.main()...")

            with redirect_stdout(writer), redirect_stderr(writer):
                result = run_etl_main()
                with IMPORT_JOBS_LOCK:
                    IMPORT_JOBS[job_id]["logs"].append(
                        f"[{time.strftime('%I:%M:%S %p')}] Running clustering.main()..."
                    )
                run_clustering_main()
            writer.flush()

            with IMPORT_JOBS_LOCK:
                IMPORT_JOBS[job_id]["status"] = "completed"
                IMPORT_JOBS[job_id]["result"] = result
                IMPORT_JOBS[job_id]["logs"].append(f"[{time.strftime('%I:%M:%S %p')}] Import completed.")
        except BaseException as exc:
            writer.flush()
            with IMPORT_JOBS_LOCK:
                IMPORT_JOBS[job_id]["status"] = "failed"
                IMPORT_JOBS[job_id]["error"] = str(exc)
                IMPORT_JOBS[job_id]["logs"].append(f"[{time.strftime('%I:%M:%S %p')}] Import failed: {exc}")

    threading.Thread(target=run_job, daemon=True).start()
    return job_id


def get_import_job(job_id):
    with IMPORT_JOBS_LOCK:
        job = IMPORT_JOBS.get(job_id)
        if not job:
            return None
        return {
            "job_id": job_id,
            "status": job["status"],
            "logs": job["logs"][:],
            "result": job["result"],
            "error": job["error"],
        }


class ClusterViewerHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT_DIR), **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/import_music/status":
            query = parse_qs(parsed.query)
            job_id = query.get("job_id", [""])[0]
            job = get_import_job(job_id)
            if job is None:
                data = json.dumps({"ok": False, "error": "Import job not found."}).encode("utf-8")
                self.send_response(HTTPStatus.NOT_FOUND)
            else:
                data = json.dumps({"ok": True, **job}).encode("utf-8")
                self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return

        route_map = {
            "/": "/frontend/landing.html",
            "/home": "/frontend/landing.html",
            "/clustering": "/frontend/index.html",
            "/playlist": "/frontend/playlist.html",
        }
        if parsed.path in route_map:
            self.send_response(HTTPStatus.FOUND)
            self.send_header("Location", route_map[parsed.path])
            self.end_headers()
            return

        super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/import_music":
            try:
                data = json.dumps({
                    "ok": True,
                    "job_id": start_import_job(),
                    "message": "Started current Spotify data import.",
                }).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            except BaseException as exc:
                data = json.dumps({"ok": False, "error": str(exc)}).encode("utf-8")
                self.send_response(HTTPStatus.BAD_REQUEST)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            return

        if parsed.path != "/api/generate_playlist":
            self.send_error(HTTPStatus.NOT_FOUND, "Unknown endpoint")
            return

        try:
            from similarity_search import search_by_features

            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length).decode("utf-8"))

            seed_tracks = payload.get("seed_tracks", [])
            playlist_length = max(5, min(100, int(payload.get("playlist_length", 20))))
            seed_ids = {track.get("track_id") for track in seed_tracks}

            query = average_seed_features(seed_tracks)
            continuation_needed = max(0, playlist_length - len(seed_tracks))
            results = search_by_features(k=max(continuation_needed * 3, continuation_needed), **query)

            continuation = []
            seen_ids = set(seed_ids)
            for item in results:
                if item["track_id"] in seen_ids:
                    continue
                continuation.append(item)
                seen_ids.add(item["track_id"])
                if len(continuation) >= continuation_needed:
                    break

            response = {
                "query": query,
                "playlist": [*seed_tracks, *continuation][:playlist_length],
            }

            data = json.dumps(response).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except BaseException as exc:
            data = json.dumps({"error": str(exc)}).encode("utf-8")
            self.send_response(HTTPStatus.BAD_REQUEST)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)


def run(host=HOST, port=PORT):
    server = ThreadingHTTPServer((host, port), ClusterViewerHandler)
    print(f"Serving landing page at http://{host}:{port}/")
    print(f"Serving clustering view at http://{host}:{port}/clustering")
    print(f"Serving playlist view at http://{host}:{port}/playlist")
    server.serve_forever()


if __name__ == "__main__":
    run()