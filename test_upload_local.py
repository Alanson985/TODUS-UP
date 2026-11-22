import os, sys
os.environ.setdefault("API_ID", "123456")
os.environ.setdefault("API_HASH", "0" * 32)
os.environ.setdefault("BOT_TOKEN", "123456:dummy-token-for-local-test")
os.environ.setdefault("TODUS_PHONE", "+5300000000")
sys.path.insert(0, ".")

from pathlib import Path
import bot

class FakeTokenError(Exception):
    pass

class FakeToDus:
    """Simula el SDK de toDus: reporta progreso por chunks y devuelve una URL."""
    def __init__(self):
        self.calls = 0
        self.logins = 0
    def login_with_phone_only(self):
        self.logins += 1
    def upload_file(self, data, file_name="", timeout=0, progress_callback=None):
        self.calls += 1
        if self.calls == 1:
            raise FakeTokenError("sesion vencida simulada")
        total = len(data)
        step = max(1, total // 10)
        cur = 0
        while cur < total:
            cur = min(total, cur + step)
            if progress_callback:
                progress_callback(cur, total)
        return "https://todus.cu/file/SIMULADO123"

fake = FakeToDus()
bot.todus_client = fake

# Archivo de prueba de 5 MB
p = Path("/tmp/todus_uploads_test.bin")
p.write_bytes(os.urandom(5 * 1024 * 1024))

state = {"current": 0, "total": 0, "cancelled": False}
url = bot.upload_sync(p, "prueba.bin", state)
print("URL devuelta:", url)
print("llamadas al SDK:", fake.calls, "(1 fallo por token + 1 OK)")
print("re-logins:", fake.logins)
print("progreso final:", state["current"], "/", state["total"])
assert url == "https://todus.cu/file/SIMULADO123"
assert state["current"] == state["total"] == p.stat().st_size
assert fake.calls == 2 and fake.logins == 1
print("OK: subida completa con reintento por token vencido")

# Cancelacion a mitad de camino
class FakeToDus2(FakeToDus):
    def upload_file(self, data, file_name="", timeout=0, progress_callback=None):
        if progress_callback:
            progress_callback(100, len(data))
        return "https://todus.cu/file/NO_DEBERIA"

bot.todus_client = FakeToDus2()
state2 = {"current": 0, "total": 0, "cancelled": True}
try:
    bot.upload_sync(p, "prueba.bin", state2)
    print("ERROR: no se cancelo")
except bot.UploadCancelled:
    print("OK: la cancelacion corta la subida (UploadCancelled)")
