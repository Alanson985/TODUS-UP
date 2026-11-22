import os, sys, asyncio, threading
os.environ.setdefault("API_ID", "123456")
os.environ.setdefault("API_HASH", "0" * 32)
os.environ.setdefault("BOT_TOKEN", "123456:dummy-token-for-local-test")
os.environ.setdefault("TODUS_PHONE", "+5300000000")
sys.path.insert(0, ".")

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import bot

PAYLOAD = os.urandom(5 * 1024 * 1024)

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/stream/ok.bin":
            self.send_response(200)
            self.send_header("Content-Length", str(len(PAYLOAD)))
            self.end_headers()
            self.wfile.write(PAYLOAD)
        else:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
    def log_message(self, *a):
        pass

srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
port = srv.server_address[1]
threading.Thread(target=srv.serve_forever, daemon=True).start()
BASE = f"http://127.0.0.1:{port}"

# En local no aplican Host/SNI de la receta (servidor plano de prueba)
bot.STREAM_HEADERS = {}
bot.STREAM_EXTENSIONS = {}

async def main():
    dest = Path("/tmp/stream_test_ok.bin")
    state = {"cancelled": False, "current": 0, "total": 0}
    n = await bot.fetch_stream(f"{BASE}/stream/ok.bin", dest, state)
    assert n == len(PAYLOAD) and dest.read_bytes() == PAYLOAD, "contenido distinto"
    assert state["current"] == state["total"] == len(PAYLOAD)
    print("OK: descarga /stream/ completa, bytes identicos, progreso al 100%")

    try:
        await bot.fetch_stream(f"{BASE}/stream/gone.bin", Path("/tmp/stream_test_404.bin"),
                               {"cancelled": False, "current": 0, "total": 0})
        print("ERROR: el 404 no lanzo excepcion")
    except bot.StreamFetchError as e:
        assert str(e) == "expired"
        print("OK: 404 -> StreamFetchError('expired') (mensaje de enlace vencido)")

    try:
        await bot.fetch_stream(f"{BASE}/stream/ok.bin", Path("/tmp/stream_test_cancel.bin"),
                               {"cancelled": True, "current": 0, "total": 0})
        print("ERROR: no se cancelo")
    except bot.UploadCancelled:
        print("OK: la cancelacion corta la descarga /stream/")

    m = bot.STREAM_RE.search("mira esto https://s3.todus.cu/stream/1e129177_-1003735367650_3202__D-C_+817.mp4 gracias")
    assert m and m.group(0).endswith("817.mp4")
    print("OK: STREAM_RE detecta el enlace del colega en un texto")

asyncio.run(main())
srv.shutdown()
