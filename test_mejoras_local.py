import os, sys, asyncio, threading
os.environ.setdefault("API_ID", "123456")
os.environ.setdefault("API_HASH", "0" * 32)
os.environ.setdefault("BOT_TOKEN", "123456:dummy-token-for-local-test")
os.environ.setdefault("TODUS_PHONE", "+5300000000")
sys.path.insert(0, ".")

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import bot

PAYLOAD = os.urandom(2 * 1024 * 1024)
state_server = {"deleted": False}

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/dl":
            self.send_response(200)
            self.send_header("Content-Length", str(len(PAYLOAD)))
            self.send_header("Content-Disposition", 'attachment; filename="report final.pdf"')
            self.end_headers(); self.wfile.write(PAYLOAD)
        elif self.path.startswith("/plain/"):
            self.send_response(200)
            self.send_header("Content-Length", str(len(PAYLOAD)))
            self.end_headers(); self.wfile.write(PAYLOAD)
        elif self.path == "/stream/obj":
            if state_server["deleted"]:
                self.send_response(404); self.send_header("Content-Length", "0"); self.end_headers()
            else:
                self.send_response(200); self.send_header("Content-Length", str(len(PAYLOAD)))
                self.end_headers(); self.wfile.write(PAYLOAD)
        else:
            self.send_response(404); self.send_header("Content-Length", "0"); self.end_headers()
    def do_HEAD(self):
        if self.path == "/stream/obj" and not state_server["deleted"]:
            self.send_response(200); self.send_header("Content-Length", str(len(PAYLOAD))); self.end_headers()
        else:
            self.send_response(404); self.send_header("Content-Length", "0"); self.end_headers()
    def do_DELETE(self):
        if self.path == "/stream/obj":
            state_server["deleted"] = True
            self.send_response(204); self.send_header("Content-Length", "0"); self.end_headers()
        else:
            self.send_response(404); self.send_header("Content-Length", "0"); self.end_headers()
    def log_message(self, *a):
        pass

srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
port = srv.server_address[1]
threading.Thread(target=srv.serve_forever, daemon=True).start()
BASE = f"http://127.0.0.1:{port}"

async def main():
    # 1) Historial: agregar, leer, marcar borrado
    bot.TMP_DIR = Path("/tmp/todus_hist_test"); bot.TMP_DIR.mkdir(parents=True, exist_ok=True)
    hp = bot._history_path()
    if hp.exists(): hp.unlink()
    bot._history_add({"url": "https://x/1", "name": "a.bin", "size": 10, "ts": 1, "route": "stream", "chat_id": 5, "deleted": False})
    bot._history_add({"url": "https://x/2", "name": "b.bin", "size": 20, "ts": 2, "route": "sdk", "chat_id": 5, "deleted": False})
    items = bot._history_load()
    assert len(items) == 2 and items[1]["name"] == "b.bin"
    bot._history_mark_deleted("https://x/1")
    items = bot._history_load()
    assert items[0]["deleted"] is True and items[1]["deleted"] is False
    print("OK: historial guarda, lee y marca borrados")

    # 2) Sonda de vida + borrado /stream/
    assert await bot._head_status(f"{BASE}/stream/obj") == 200
    assert await bot._head_status(f"{BASE}/nope") == 404
    assert await bot._delete_stream_object(f"{BASE}/stream/obj") is True
    assert await bot._head_status(f"{BASE}/stream/obj") == 404
    print("OK: _head_status y _delete_stream_object (DELETE + verificacion)")

    # 3) fetch_url: nombre desde Content-Disposition y desde la URL
    st = {"cancelled": False, "current": 0, "total": 0}
    dest = Path("/tmp/fetch_test.bin")
    size, name = await bot.fetch_url(f"{BASE}/dl", dest, st)
    assert size == len(PAYLOAD) and name == "report final.pdf" and dest.read_bytes() == PAYLOAD
    size2, name2 = await bot.fetch_url(f"{BASE}/plain/movie.mkv", dest, {"cancelled": False, "current": 0, "total": 0})
    assert name2 == "movie.mkv"
    print("OK: fetch_url baja y resuelve el nombre (header y URL)")

    # 4) fetch_url: 404 y limite de tamano
    try:
        await bot.fetch_url(f"{BASE}/nope", dest, {"cancelled": False, "current": 0, "total": 0})
        print("ERROR: 404 sin excepcion")
    except bot.StreamFetchError as e:
        assert "404" in str(e); print("OK: fetch_url 404 -> StreamFetchError")
    old = bot.MAX_FILE_BYTES
    bot.MAX_FILE_BYTES = 1024
    try:
        await bot.fetch_url(f"{BASE}/dl", dest, {"cancelled": False, "current": 0, "total": 0})
        print("ERROR: too_big sin excepcion")
    except bot.StreamFetchError as e:
        assert str(e) == "too_big"; print("OK: fetch_url respeta el limite (too_big)")
    finally:
        bot.MAX_FILE_BYTES = old

    # 5) upload_any: sin fallback si el archivo supera el limite de RAM del SDK
    p = Path("/tmp/fallback_test.bin"); p.write_bytes(PAYLOAD)
    bot.STREAM_BASE_URL = f"{BASE}/falla"  # el servidor dara 404/500 -> stream falla
    bot.upload_sync = lambda path, name="", state=None: "https://sdk.example/OK"
    bot.SDK_FALLBACK_MAX_BYTES = 10
    try:
        bot.upload_any(p, "x.bin", {"cancelled": False, "current": 0, "total": 0, "chat_id": 1})
        print("ERROR: debio bloquear el fallback")
    except RuntimeError as e:
        assert "too large" in str(e); print("OK: fallback SDK bloqueado para archivos grandes")
    bot.SDK_FALLBACK_MAX_BYTES = 500 * 1024 * 1024
    out = bot.upload_any(p, "x.bin", {"cancelled": False, "current": 0, "total": 0, "chat_id": 1})
    assert out == "https://sdk.example/OK"
    print("OK: con tamano permitido, upload_any cae al SDK")

asyncio.run(main())
srv.shutdown()
print("TODAS LAS PRUEBAS DE MEJORAS OK")
