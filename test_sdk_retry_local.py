"""Pruebas locales de los reintentos ante errores de red pasajeros en
la subida SDK (via clasica upload_sync y flujo directo _upload_direct):
1) Clasificacion _es_error_transitorio por nombre de tipo.
2) upload_sync: timeout, timeout, exito -> devuelve la URL (3 llamadas);
   timeout siempre -> levanta tras 3 llamadas; error no pasajero ->
   levanta a la primera; estado cancelado -> UploadCancelled.
3) _upload_direct: misma conducta y jamas cae a la ruta "stream";
   _DirectFallback y UploadCancelled se propagan sin reintentar.
Sin red externa: todus_client y _direct_attempt se sustituyen por falsos.
"""
import asyncio
import os
import sys
import tempfile
from pathlib import Path

os.environ.setdefault("TODUS_PHONE", "+5300000000")
os.environ.setdefault("API_ID", "1")
os.environ.setdefault("API_HASH", "x")
os.environ.setdefault("BOT_TOKEN", "x")
os.environ.setdefault("OWNER_ID", "1")

sys.path.insert(0, str(Path(__file__).parent))
import bot  # noqa: E402


class ConnectTimeout(Exception):
    pass


class MaxRetryError(Exception):
    pass


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)
    print(f"  ok: {msg}")


def test_clasificacion():
    print("== clasificacion de errores ==")
    check(bot._es_error_transitorio(ConnectTimeout("x")),
          "ConnectTimeout es transitorio")
    check(bot._es_error_transitorio(ConnectionError("x")),
          "ConnectionError es transitorio")
    check(bot._es_error_transitorio(TimeoutError("x")),
          "TimeoutError es transitorio")
    check(bot._es_error_transitorio(MaxRetryError("x")),
          "MaxRetryError es transitorio")
    check(not bot._es_error_transitorio(ValueError("x")),
          "ValueError no es transitorio")
    check(not bot._es_error_transitorio(bot.UploadCancelled()),
          "UploadCancelled no es transitorio")


class FakeClient:
    def __init__(self, plan):
        self.plan = plan  # lista: excepcion o valor de retorno
        self.calls = 0

    def upload_file(self, data, **kwargs):
        self.calls += 1
        step = self.plan[min(self.calls - 1, len(self.plan) - 1)]
        if isinstance(step, BaseException):
            raise step
        return step

    def login_with_phone_only(self):
        raise AssertionError("no debia re-loguear en estas pruebas")


def test_upload_sync():
    print("== upload_sync (via clasica) ==")
    orig_client = bot.todus_client
    orig_sleep = bot.time.sleep
    bot.time.sleep = lambda s: None
    tmp = Path(tempfile.mkstemp()[1])
    tmp.write_bytes(b"contenido de prueba")
    try:
        bot.todus_client = FakeClient(
            [ConnectTimeout("t1"), ConnectTimeout("t2"), "https://down/1"])
        url = bot.upload_sync(tmp, "f.bin", {})
        check(url == "https://down/1" and bot.todus_client.calls == 3,
              "dos timeouts y exito al tercer intento")

        bot.todus_client = FakeClient([ConnectTimeout("siempre")])
        try:
            bot.upload_sync(tmp, "f.bin", {})
            check(False, "timeout permanente debia levantar")
        except ConnectTimeout:
            check(bot.todus_client.calls == 3,
                  "timeout permanente: 3 intentos y levanta")

        bot.todus_client = FakeClient([ValueError("definitivo")])
        try:
            bot.upload_sync(tmp, "f.bin", {})
            check(False, "error no pasajero debia levantar")
        except ValueError:
            check(bot.todus_client.calls == 1,
                  "error no pasajero: sin reintentos")

        bot.todus_client = FakeClient([ConnectTimeout("t")])
        try:
            bot.upload_sync(tmp, "f.bin", {"cancelled": True, "current": 9})
            check(False, "cancelado debia levantar UploadCancelled")
        except bot.UploadCancelled:
            check(bot.todus_client.calls == 1,
                  "cancelado durante la espera: UploadCancelled")
    finally:
        bot.todus_client = orig_client
        bot.time.sleep = orig_sleep
        tmp.unlink(missing_ok=True)


def test_upload_direct():
    print("== _upload_direct (flujo directo) ==")
    orig_attempt = bot._direct_attempt
    orig_sleep = asyncio.sleep

    async def no_sleep(s):
        return None

    asyncio.sleep = no_sleep
    try:
        async def run(plan):
            calls = []

            async def fake_attempt(job, size, name, route):
                calls.append(route)
                step = plan[min(len(calls) - 1, len(plan) - 1)]
                if isinstance(step, BaseException):
                    raise step
                return step

            bot._direct_attempt = fake_attempt
            job = {"current": 0}
            try:
                url = await bot._upload_direct(job, 1024, "f.bin")
                return url, calls
            except BaseException as e:
                return e, calls

        url, calls = asyncio.run(run(
            [ConnectTimeout("t1"), ConnectTimeout("t2"), "https://down/2"]))
        check(url == "https://down/2" and calls == ["sdk"] * 3,
              "dos timeouts y exito al tercer intento, siempre SDK")

        err, calls = asyncio.run(run([ConnectTimeout("siempre")]))
        check(isinstance(err, ConnectTimeout) and calls == ["sdk"] * 3,
              "timeout permanente: 3 intentos SDK, nunca stream, levanta")

        err, calls = asyncio.run(run([bot._DirectFallback("sin arranque")]))
        check(isinstance(err, bot._DirectFallback) and len(calls) == 1,
              "_DirectFallback se propaga sin reintentar")

        err, calls = asyncio.run(run([bot.UploadCancelled()]))
        check(isinstance(err, bot.UploadCancelled) and len(calls) == 1,
              "UploadCancelled se propaga sin reintentar")
    finally:
        bot._direct_attempt = orig_attempt
        asyncio.sleep = orig_sleep


if __name__ == "__main__":
    test_clasificacion()
    test_upload_sync()
    test_upload_direct()
    print("TODAS LAS PRUEBAS DE REINTENTO SDK PASARON")
