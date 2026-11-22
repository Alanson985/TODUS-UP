import os, sys, threading
os.environ.setdefault("API_ID", "123456")
os.environ.setdefault("API_HASH", "0" * 32)
os.environ.setdefault("BOT_TOKEN", "123456:dummy-token-for-local-test")
os.environ.setdefault("TODUS_PHONE", "+5300000000")
sys.path.insert(0, ".")

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import bot

PAYLOAD = os.urandom(5 * 1024 * 1024)
received = {}

class H(BaseHTTPRequestHandler):
    def do_PUT(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n)
        received["path"] = self.path
        received["headers"] = dict(self.headers)
        received["body"] = body
        code = 500 if self.path.startswith("/stream/fail") else 200
        self.send_response(code)
        self.send_header("Content-Length", "0")
        self.end_headers()
    def do_HEAD(self):
        # El bot verifica la subida con un HEAD: sin esto responde 501.
        code = 500 if self.path.startswith("/stream/fail") else 200
        self.send_response(code)
        self.send_header("Content-Length", str(len(PAYLOAD)))
        self.end_headers()
    def log_message(self, *a):
        pass

srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
port = srv.server_address[1]
threading.Thread(target=srv.serve_forever, daemon=True).start()
bot.STREAM_BASE_URL = f"http://127.0.0.1:{port}/stream"

p = Path("/tmp/stream_up_test.bin")
p.write_bytes(PAYLOAD)

state = {"cancelled": False, "current": 0, "total": 0, "chat_id": 12345}
url = bot.stream_upload_sync(p, "test.bin", state)
print("URL devuelta:", url)
assert url.startswith(f"http://127.0.0.1:{port}/stream/users/tg_12345/telegram/")
assert url.endswith("_test.bin")
assert received["body"] == PAYLOAD, "los bytes recibidos difieren"
assert received["headers"].get("User-Agent") == "Wget/1.21.3"
assert received["headers"].get("Host") == "s3.todus.cu"
assert int(received["headers"].get("Content-Length")) == len(PAYLOAD)
assert state["current"] == state["total"] == len(PAYLOAD)
print("OK: PUT /stream/ con clave propia, headers de la receta y bytes identicos")

try:
    bot.stream_upload_sync(p, "test.bin", {"cancelled": True, "current": 0, "total": 0, "chat_id": 1})
    print("ERROR: no se cancelo")
except bot.UploadCancelled:
    print("OK: la cancelacion corta la subida /stream/")

# Fallback al SDK cuando /stream/ falla (servidor responde 500 tras reintentos)
bot.STREAM_BASE_URL = f"http://127.0.0.1:{port}/stream/fail"
bot.upload_sync = lambda path, name="", state=None: "https://sdk.example/URL_FIRMADA"
out = bot.upload_any(p, "test.bin", {"cancelled": False, "current": 0, "total": 0, "chat_id": 9})
assert out == "https://sdk.example/URL_FIRMADA"
print("OK: si /stream/ falla, upload_any cae al SDK clasico")
srv.shutdown()
