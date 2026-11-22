"""Pruebas locales del flujo directo (sin red externa):
1) _OrderedPipe + _PipeBody: tramos fuera de orden -> bytes exactos.
2) _sdk_put_pipe_sync contra un servidor HTTP local: el cuerpo llega
   byte a byte identico, con Content-Length correcto, y la RAM del
   proceso crece poco con un archivo de 300 MB (nada de cargarlo).
3) Cancelacion a mitad de camino: el PUT se aborta.
4) Error del productor: el consumidor lo recibe.
"""
import hashlib
import os
import random
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

os.environ.setdefault("TODUS_PHONE", "+5300000000")
os.environ.setdefault("API_ID", "1")
os.environ.setdefault("API_HASH", "x")
os.environ.setdefault("BOT_TOKEN", "x")
os.environ.setdefault("OWNER_ID", "1")

sys.path.insert(0, str(Path(__file__).parent))
import bot  # noqa: E402

CHUNK = 1024 * 1024


def rss_mb():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS"):
                return int(line.split()[1]) / 1024
    return 0


# --- servidor local que hashea lo que recibe por PUT -------------------
RECEIVED = {}


class Handler(BaseHTTPRequestHandler):
    def do_PUT(self):
        h = hashlib.sha256()
        left = int(self.headers.get("Content-Length") or 0)
        total = left
        while left > 0:
            data = self.rfile.read(min(1024 * 256, left))
            if not data:
                break
            h.update(data)
            left -= len(data)
        RECEIVED[self.path] = (total - left, h.hexdigest(),
                               self.headers.get("Content-Length"))
        self.send_response(200)
        self.end_headers()

    def log_message(self, *a):
        pass


server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
port = server.server_address[1]
threading.Thread(target=server.serve_forever, daemon=True).start()
BASE = f"http://127.0.0.1:{port}"


def make_file(path: Path, size: int) -> str:
    h = hashlib.sha256()
    block = os.urandom(1024 * 1024)
    with path.open("wb") as f:
        left = size
        while left > 0:
            part = block[:min(len(block), left)]
            f.write(part)
            h.update(part)
            left -= len(part)
    return h.hexdigest()


def producer_from_file(pipe, path: Path, size: int, workers=4,
                       delay=0.0):
    """Imita a _download_parallel_pipe del bot: varios hilos reclaman
    indices EN ORDEN desde un contador compartido y cada uno espera su
    ventana antes de poner el tramo; un delay aleatorio desordena la
    llegada. (Un solo hilo recorriendo indices mezclados se bloquea:
    el tramo que el consumidor espera queda detras de la ventana.)"""
    counter = {"next": 0}
    lock = threading.Lock()
    fd = os.open(path, os.O_RDONLY)

    def worker():
        while True:
            with lock:
                idx = counter["next"]
                counter["next"] += 1
            offset = idx * CHUNK
            if offset >= size:
                return
            while offset >= pipe.read_pos + pipe.window \
                    and not pipe.aborted:
                time.sleep(0.01)
            if pipe.aborted:
                return
            data = os.pread(fd, min(CHUNK, size - offset), offset)
            if delay:
                time.sleep(delay * random.random())
            pipe.put(offset, data)

    threads = [threading.Thread(target=worker) for _ in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    os.close(fd)
    if not pipe.aborted:
        pipe.finish()


def test_pipe_order(tmp):
    size = 10 * CHUNK + 12345
    src = tmp / "a.bin"
    digest = make_file(src, size)
    pipe = bot._OrderedPipe(size, 4 * CHUNK)
    job = {"current": 0, "cancelled": False}
    t = threading.Thread(target=producer_from_file,
                         args=(pipe, src, size))
    t.start()
    body = bot._PipeBody(pipe, job)
    h = hashlib.sha256()
    while True:
        data = body.read(8192)  # lecturas chicas, como urllib3
        if not data:
            break
        h.update(data)
    t.join()
    assert h.hexdigest() == digest, "orden de bytes incorrecto"
    assert job["current"] == size, job["current"]
    print("1) pipe ordenado + lecturas chicas: OK", flush=True)


def test_sdk_put_ram(tmp):
    import requests
    size = 300 * CHUNK
    src = tmp / "big.bin"
    digest = make_file(src, size)

    class FakeClient:
        def reserve_upload_url(self, size_, ftype, file_name=""):
            return (f"{BASE}/up", "https://down.ejemplo/abc123/archivo")

        session = requests.Session()

    orig = bot.todus_client
    bot.todus_client = FakeClient()
    try:
        pipe = bot._OrderedPipe(size, 32 * CHUNK)
        job = {"current": 0, "cancelled": False, "chat_id": 1}
        t = threading.Thread(target=producer_from_file,
                             args=(pipe, src, size))
        base_rss = rss_mb()
        t.start()
        url = bot._sdk_put_pipe_sync(pipe, size, "big.bin", job)
        t.join()
        peak_rss = rss_mb()
    finally:
        bot.todus_client = orig
    got_size, got_hash, got_cl = RECEIVED["/up"]
    assert url.startswith("https://down.ejemplo/"), url
    assert got_size == size and got_cl == str(size), (got_size, got_cl)
    assert got_hash == digest, "el servidor recibio bytes distintos"
    growth = peak_rss - base_rss
    assert growth < 120, f"la RAM crecio {growth:.0f} MB con 300 MB"
    print(f"2) PUT SDK en flujo (300 MB): OK, RAM +{growth:.0f} MB",
          flush=True)


def test_cancel(tmp):
    size = 40 * CHUNK
    src = tmp / "c.bin"
    make_file(src, size)
    pipe = bot._OrderedPipe(size, 8 * CHUNK)
    job = {"current": 0, "cancelled": False}
    t = threading.Thread(target=producer_from_file,
                         args=(pipe, src, size), kwargs={"delay": 0.05})
    t.start()
    body = bot._PipeBody(pipe, job)
    body.read(8192)
    job["cancelled"] = True
    try:
        for _ in range(100):
            body.read(8192)
        raise AssertionError("no salto UploadCancelled")
    except bot.UploadCancelled:
        pass
    pipe.abort()
    t.join(timeout=10)
    print("3) cancelacion a mitad de camino: OK", flush=True)


def test_producer_error(tmp):
    pipe = bot._OrderedPipe(5 * CHUNK, 4 * CHUNK)
    boom = RuntimeError("boom productor")

    def prod():
        pipe.put(0, b"x" * CHUNK)
        pipe.finish(error=boom)

    t = threading.Thread(target=prod)
    t.start()
    body = bot._PipeBody(pipe, {"current": 0, "cancelled": False})
    got = body.read(CHUNK)
    assert got == b"x" * CHUNK
    try:
        body.read(8192)
        raise AssertionError("no llego el error del productor")
    except RuntimeError as e:
        assert "boom" in str(e)
    t.join()
    print("4) error del productor llega al consumidor: OK", flush=True)


def main():
    tmp = Path.home() / "workspace/.local/tmp/test-direct"
    tmp.mkdir(parents=True, exist_ok=True)
    try:
        test_pipe_order(tmp)
        test_sdk_put_ram(tmp)
        test_cancel(tmp)
        test_producer_error(tmp)
    finally:
        for f in tmp.iterdir():
            f.unlink()
        tmp.rmdir()
    print("TODAS LAS PRUEBAS DEL FLUJO DIRECTO PASARON", flush=True)


if __name__ == "__main__":
    main()
