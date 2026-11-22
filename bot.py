# bot.py — Kurigram + toDus, optimizado, progreso cada 3s
# Release 2026-11-12: subida /stream/, historial /links, subida por URL y control de acceso por roles.
# Release 2026-11-15: metodo DevFast (subida SDK con ID de toDus, enlace v4 y enlace directo para IDM).
import os
import re
import gc
import json
import shutil
import uuid
import mimetypes
from datetime import datetime, timezone
from urllib.parse import quote, urlparse, parse_qs
import asyncio
import logging
import time
import threading
from pathlib import Path
import httpx
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, LinkPreviewOptions
from todus import ToDusClient2
from keep_alive import start_keep_alive

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# --- secretos: variables de entorno o .env guardado en Render ----------------
# Las variables de entorno (Render → Environment) mandan. Como respaldo se
# lee un archivo .env desde los Secret Files de Render (/etc/secrets/.env),
# desde la ruta que diga ENV_FILE, o desde la carpeta del bot. Ese archivo
# vive SOLO en Render o en la máquina local: jamás en el repo (.env está en
# .gitignore), porque un secreto en GitHub queda en el historial para siempre.
def _cargar_env_file():
    rutas = [os.environ.get("ENV_FILE", ""), "/etc/secrets/.env",
             str(Path(__file__).with_name(".env"))]
    for ruta in rutas:
        if not ruta:
            continue
        try:
            lineas = Path(ruta).read_text().splitlines()
        except Exception:
            continue
        for linea in lineas:
            linea = linea.strip()
            if not linea or linea.startswith("#") or "=" not in linea:
                continue
            clave, valor = linea.split("=", 1)
            os.environ.setdefault(clave.strip(), valor.strip().strip("\"'"))
        return

_cargar_env_file()

def _requerida(nombre):
    valor = os.environ.get(nombre, "").strip()
    if not valor:
        raise RuntimeError(
            f"Falta la variable {nombre}: cargala en Render, en la pestaña "
            "Environment (o en el Secret File .env), y el servicio se reinicia solo."
        )
    return valor

# --- config ------------------------------------------------------------------
try:
    API_ID = int(_requerida("API_ID"))
except ValueError:
    raise RuntimeError("La variable API_ID debe ser solo el número de tu app de Telegram, sin espacios ni comillas.")
API_HASH = _requerida("API_HASH")
BOT_TOKEN = _requerida("BOT_TOKEN")
TODUS_PHONE = _requerida("TODUS_PHONE")

DL_LIMIT_BPS = int(os.environ.get("DL_LIMIT_BPS", "0"))
# Descarga paralela de Telegram: pedidos upload.GetFile simultaneos
# sobre la sesion de medios. La descarga comun de Kurigram es
# secuencial (un pedido, esperar, otro pedido) y ahi se pierde la
# mayor parte de la velocidad. 0 = desactivada.
DL_WORKERS = int(os.environ.get("DL_WORKERS", "8"))
DL_PARALLEL_MIN_MB = int(os.environ.get("DL_PARALLEL_MIN_MB", "16"))
DL_PARALLEL_MIN_BYTES = DL_PARALLEL_MIN_MB * 1024 * 1024
DL_CHUNK = 1024 * 1024
MAX_CONCURRENT = int(os.environ.get("MAX_CONCURRENT", "2"))
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
UPLOAD_TIMEOUT = int(os.environ.get("UPLOAD_TIMEOUT", "1800"))
MAX_FILE_MB = int(os.environ.get("MAX_FILE_MB", "2000"))
# Subida directa /stream/: PUT sin cuenta ni token al S3 de toDus.
# Con "0" se usa solo la subida clasica por SDK (cuenta toDus).
TODUS_STREAM_UPLOAD = os.environ.get("TODUS_STREAM_UPLOAD", "1") != "0"
# Metodo DevFast: para que el archivo quede registrado en toDus con su ID
# numerico (enlaces /todus/v4/<id>/<archivo>) la subida tiene que entrar
# por la reserva del SDK, no por /stream/. Con "1" (default) las subidas
# no segmentadas que caben en el limite del SDK van SOLO por el SDK y de
# forma estricta: si el SDK falla, la subida falla (nada de caer a
# /stream/ en silencio y que el archivo quede sin ID). Con "0", el
# orden clasico (/stream/ primero, SDK de fallback).
TODUS_SDK_FIRST = os.environ.get("TODUS_SDK_FIRST", "1") != "0"
# El SDK carga el archivo entero en RAM: su ruta solo se usa hasta este
# tamano (2 GB, a pedido de Alan el 2026-11-15; ojo: subir un archivo de
# ese tamano exige un servidor con MAS de 2 GB de RAM libre). La ruta
# /stream/ transmite desde disco y no tiene ese problema.
SDK_FALLBACK_MAX_MB = int(os.environ.get("SDK_FALLBACK_MAX_MB", "2048"))
SDK_FALLBACK_MAX_BYTES = SDK_FALLBACK_MAX_MB * 1024 * 1024
# Flujo directo Telegram -> toDus (2026-11-18): la descarga por rangos
# alimenta la subida en el momento, sin archivo temporal y con una
# ventana acotada en RAM; ni la RAM ni el disco crecen con el tamano.
# Con "0" se vuelve a la via clasica (descargar a disco y subir).
TODUS_DIRECT_FLOW = os.environ.get("TODUS_DIRECT_FLOW", "1") != "0"
DIRECT_WINDOW_MB = int(os.environ.get("DIRECT_WINDOW_MB", "32"))
DIRECT_WINDOW_BYTES = max(4, DIRECT_WINDOW_MB) * 1024 * 1024
MAX_FILE_BYTES = MAX_FILE_MB * 1024 * 1024
# Tope total de un archivo recibido: lo que pase de MAX_FILE_MB se sube
# dividido en partes de ese tamano; esto es solo el techo de seguridad
# (disco del servidor). 0 = sin techo.
MAX_TOTAL_MB = int(os.environ.get("MAX_TOTAL_MB", "10240"))
MAX_TOTAL_BYTES = MAX_TOTAL_MB * 1024 * 1024
# Presupuesto de /tmp por archivo: el volumen temporal de Render (plan
# free) es de 2 GB EN TOTAL y si se llena, Render desaloja la instancia
# a mitad de trabajo (paso el 2026-11-13: SIGTERM en plena descarga y
# la tanda se perdio). Ningun archivo recibido puede ocupar mas que
# esto en /tmp: deja margen para el estado, el .txt de enlaces y el
# resto del sistema. 0 = sin presupuesto (solo manda MAX_TOTAL_MB).
TMP_BUDGET_MB = int(os.environ.get("TMP_BUDGET_MB", "1536"))
TMP_BUDGET_BYTES = TMP_BUDGET_MB * 1024 * 1024
# Espacio libre que se exige en /tmp ADEMAS del archivo en curso.
TMP_RESERVE_MB = int(os.environ.get("TMP_RESERVE_MB", "128"))
TMP_RESERVE_BYTES = TMP_RESERVE_MB * 1024 * 1024

def _limit_bytes() -> int:
    """Techo efectivo de un archivo recibido: el menor entre el techo
    total configurado y el presupuesto de /tmp (un 0 = ese techo no va)."""
    caps = [c for c in (MAX_TOTAL_BYTES, TMP_BUDGET_BYTES) if c > 0]
    return min(caps) if caps else 0

EDIT_INTERVAL = 3.0

# OJO: un InlineKeyboardMarkup vacio es invalido en MTProto (REPLY_MARKUP_INVALID
# al editar); para "sin botones" simplemente no se pasa reply_markup.
NO_PREVIEW = LinkPreviewOptions(is_disabled=True)

# --- referencias /stream/ de toDus -------------------------------------------
# Metodo de descarga directa desde el S3 de toDus: Host y SNI fijados en
# s3.todus.cu y User-Agent de Wget, tal como circula en la receta.
STREAM_RE = re.compile(r"https://s3\.todus\.cu/stream/[^\s\"'<>\)\]]+")
STREAM_HEADERS = {"Host": "s3.todus.cu", "User-Agent": "Wget/1.21.3",
                  "Accept": "*/*", "Accept-Encoding": "identify"}
STREAM_EXTENSIONS = {"sni_hostname": "s3.todus.cu"}
STREAM_BASE_URL = "https://s3.todus.cu/stream"
URL_RE = re.compile(r"https?://[^\s\"'<>\)\]]+")

KB_DELETE = InlineKeyboardMarkup([[InlineKeyboardButton("🗑 Delete file", callback_data="del")]])
DONE_LINKS = {}  # (chat_id, done_msg_id) -> url /stream/ borrable

# --- historial de subidas ------------------------------------------------------
def _history_path() -> Path:
    return TMP_DIR / "history.json"

def _history_load() -> list:
    try:
        return json.loads(_history_path().read_text())
    except Exception:
        return []

def _history_save(items: list):
    try:
        _history_path().write_text(json.dumps(items[-200:]))
    except Exception:
        pass
    _state_touch()

def _history_add(entry: dict):
    items = _history_load()
    items.append(entry)
    _history_save(items)

def _history_mark_deleted(url: str):
    items = _history_load()
    changed = False
    for it in reversed(items):
        if it.get("url") == url and not it.get("deleted"):
            it["deleted"] = True
            changed = True
            break
    if changed:
        _history_save(items)

def _recipe_kwargs(url: str) -> dict:
    kw = {"headers": STREAM_HEADERS}
    if url.startswith("https://s3.todus.cu/"):
        kw["extensions"] = STREAM_EXTENSIONS
    return kw

async def _head_status(url: str):
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(10), trust_env=False,
                                     follow_redirects=True) as c:
            r = await c.head(url, **_recipe_kwargs(url))
            return r.status_code
    except Exception:
        return None

async def _delete_stream_object(url: str) -> bool:
    """DELETE sin autenticacion + verificacion de que el objeto ya no esta."""
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30), trust_env=False,
                                     follow_redirects=True) as c:
            r = await c.request("DELETE", url, **_recipe_kwargs(url))
            if r.status_code not in (200, 204, 404):
                return False
            h = await c.head(url, **_recipe_kwargs(url))
            return h.status_code in (404, 410)
    except Exception:
        return False

def _too_big(total: int) -> bool:
    return _limit_bytes() > 0 and total > _limit_bytes()

if not OWNER_ID:
    log.warning("OWNER_ID no configurado: el bot acepta archivos de CUALQUIER usuario. "
                "Mandale /start al bot para ver tu ID y cargalo como OWNER_ID.")

# --- toDus -------------------------------------------------------------------
todus_client = ToDusClient2(TODUS_PHONE)

def login_todus(attempts=5):
    for i in range(1, attempts + 1):
        try:
            todus_client.login_with_phone_only()
            log.info("toDus login (phone-only) OK para %s", TODUS_PHONE)
            return
        except Exception as e:
            log.warning("toDus login intento %d/%d falló: %s", i, attempts, e)
            if i < attempts:
                time.sleep(10)
    raise RuntimeError("no se pudo iniciar sesión en toDus tras varios intentos")

TMP_DIR = Path(os.environ.get("TMPDIR", "/tmp")) / "todus_uploads"
TMP_DIR.mkdir(parents=True, exist_ok=True)

# --- disciplina de /tmp --------------------------------------------------------
def _tmp_free() -> int:
    try:
        return shutil.disk_usage(TMP_DIR).free
    except Exception:
        return 0

async def _wait_tmp_space(job, needed: int) -> bool:
    """Espera a que /tmp tenga lugar para `needed` bytes + la reserva.
    Los runners son por chat: dos descargas grandes en paralelo llenan
    el disco y Render mata la instancia (la descarga en curso y toda
    la tanda se pierden). False si se cancelo o se agoto la espera."""
    limite = needed + TMP_RESERVE_BYTES
    waited = 0
    while _tmp_free() < limite:
        if job.get("cancelled"):
            return False
        if waited == 0:
            log.info("esperando espacio en /tmp: faltan %s",
                     fmt_bytes(limite - _tmp_free()))
        if waited >= 600:
            return False
        await asyncio.sleep(5)
        waited += 5
    return True

def _sweep_tmp():
    """Barrido de /tmp al arrancar: borra restos de descargas de una
    vida anterior del proceso (si Render mato la instancia en plena
    descarga, el parcial queda ocupando disco y acerca la proxima
    eviccion). Conserva el estado (access.json / history.json)."""
    keep = {"access.json", "history.json"}
    freed = 0
    try:
        entries = list(TMP_DIR.iterdir())
    except Exception:
        return 0
    for p in entries:
        if p.name in keep:
            continue
        try:
            if p.is_dir():
                size = sum(f.stat().st_size for f in p.rglob("*")
                           if f.is_file())
                shutil.rmtree(p, ignore_errors=True)
            else:
                size = p.stat().st_size
                p.unlink(missing_ok=True)
            freed += size
        except Exception:
            pass
    if freed:
        log.info("limpieza de /tmp: %s liberados", fmt_bytes(freed))
    return freed

def _job_cleanup(job):
    """Borra los temporales del job (descarga completa o parcial y la
    carpeta de un enlace): pase lo que pase con el trabajo — fallo,
    cancelacion o error del runner — su archivo no puede quedar
    ocupando /tmp para siempre."""
    for key in ("path", "local_path"):
        p = job.get(key)
        if p:
            try:
                Path(p).unlink(missing_ok=True)
            except Exception:
                pass
    d = job.get("job_dir")
    if d:
        _cleanup_dir(Path(d))

# --- cola --------------------------------------------------------------------
worker_started = False
JOBS = {}  # (chat_id, status_msg_id) -> job, para /cancel

# --- formateo ----------------------------------------------------------------
_UNITS = ("B", "KB", "MB", "GB", "TB")

def fmt_bytes(n: float) -> str:
    i = 0
    while n >= 1024 and i < len(_UNITS) - 1:
        n /= 1024
        i += 1
    return f"{n:.1f} {_UNITS[i]}"

def fmt_eta(s: float) -> str:
    if s <= 0 or s > 86400:
        return "--:--"
    s = int(s)
    m, sec = divmod(s, 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{sec:02d}" if h else f"{m:02d}:{sec:02d}"

_WZML_PARTIALS = ["▤", "▥", "▦", "▧", "▨", "▩", "■"]

def fmt_bar(pct: float, width: int = 12) -> str:
    """Barra de WZML-X (get_progress_bar_string), tal cual: celdas ■,
    parciales ▤▥▦▧▨▩ y vacías □; cada celda entera vale 8%."""
    p = max(0.0, min(100.0, pct))
    c_full = int(p // 8)
    c_part = int(p % 8 - 1)
    s = "■" * c_full
    if c_part >= 0:
        s += _WZML_PARTIALS[c_part]
    s += "□" * (12 - c_full)
    return s

def fmt_eta_left(eta: float) -> str:
    e = fmt_eta(eta)
    return f"⏱ {e} left" if e != "--:--" else "⏱ --:--"

# --- descarga optimizada -----------------------------------------------------
class _ParallelFallback(Exception):
    """La descarga paralela no aplica a este archivo: usar la comun."""


def _job_media(message):
    return (message.document or message.video or message.audio
            or message.voice or message.photo)


def _location_de(file_id_str):
    """(FileId, location) como los arma el get_file de Kurigram."""
    from pyrogram.file_id import FileId, FileType
    from pyrogram import raw
    fid = FileId.decode(file_id_str)
    if fid.file_type == FileType.CHAT_PHOTO:
        raise _ParallelFallback("foto de chat sin location simple")
    if fid.file_type == FileType.PHOTO:
        loc = raw.types.InputPhotoFileLocation(
            id=fid.media_id, access_hash=fid.access_hash,
            file_reference=fid.file_reference,
            thumb_size=fid.thumbnail_size)
    else:
        loc = raw.types.InputDocumentFileLocation(
            id=fid.media_id, access_hash=fid.access_hash,
            file_reference=fid.file_reference,
            thumb_size=fid.thumbnail_size)
    return fid, loc


async def _download_parallel(client, message, media, local_path, job):
    """Descarga por rangos: DL_WORKERS pedidos upload.GetFile
    simultaneos sobre la sesion de medios del DC del archivo, cada
    tramo escrito en su offset con pwrite. Devuelve la ruta.
    _ParallelFallback si el caso no aplica (CDN, tipo raro); cualquier
    otro error lo trata el llamador (reintento / via secuencial)."""
    from pyrogram import raw
    from pyrogram.errors import FileReferenceExpired
    size = media.file_size or 0
    if size < DL_PARALLEL_MIN_BYTES or not getattr(media, "file_id", None):
        raise _ParallelFallback("archivo chico o sin file_id")
    fid, location = _location_de(media.file_id)
    session = await client.get_session(fid.dc_id, is_media=True)
    job["dl_total"] = size
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    state = {"done": 0, "next": 0, "refreshed": False}
    lock = asyncio.Lock()
    fd = os.open(local_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)

    async def _fetch(offset):
        r = await session.invoke(
            raw.functions.upload.GetFile(
                location=location, offset=offset, limit=DL_CHUNK),
            sleep_threshold=60)
        if isinstance(r, raw.types.upload.FileCdnRedirect):
            raise _ParallelFallback("el archivo se sirve por CDN")
        return r.bytes

    async def _worker():
        nonlocal location
        while True:
            async with lock:
                idx = state["next"]
                state["next"] += 1
            offset = idx * DL_CHUNK
            if offset >= size:
                return
            want = min(DL_CHUNK, size - offset)
            data = None
            for intento in range(1, 4):
                try:
                    data = await _fetch(offset)
                    if len(data) != want:
                        raise RuntimeError(
                            f"tramo corto: {len(data)} de {want}")
                    break
                except FileReferenceExpired:
                    if state["refreshed"]:
                        raise
                    state["refreshed"] = True
                    fresh = await client.get_messages(
                        message.chat.id, message.id)
                    fmedia = _job_media(fresh)
                    if fmedia is None:
                        raise
                    _, location = _location_de(fmedia.file_id)
                    log.info("file_reference renovado en la descarga")
                except _ParallelFallback:
                    raise
                except Exception:
                    if intento == 3:
                        raise
                    await asyncio.sleep(2 * intento)
            os.pwrite(fd, data, offset)
            async with lock:
                state["done"] += len(data)
                done = state["done"]
            job["dl_current"] = done
            if DL_LIMIT_BPS > 0:
                elapsed = loop.time() - t0
                if elapsed > 0 and done / elapsed > DL_LIMIT_BPS:
                    await asyncio.sleep(
                        min(0.2, done / DL_LIMIT_BPS - elapsed))

    try:
        os.ftruncate(fd, size)
        await asyncio.gather(
            *[_worker() for _ in range(max(1, DL_WORKERS))])
    finally:
        os.close(fd)
    real = Path(local_path)
    if real.stat().st_size != size:
        raise RuntimeError("el tamano final no coincide con el esperado")
    return real


async def download_with_progress(client, message, local_path: Path, job, attempts=3):
    """Descarga de Telegram a disco; el progreso va al estado del job
    (lo muestra el panel unico del chat). Devuelve (ruta, error).
    Primero intenta la paralela por rangos; si no aplica o falla,
    cae a la secuencial de Kurigram (download_media)."""
    last_err = None
    for attempt in range(1, attempts + 1):
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        job["dl_attempt"] = attempt
        media = _job_media(message)
        size = (media.file_size if media else 0) or 0

        if DL_WORKERS > 0 and media is not None \
                and size >= DL_PARALLEL_MIN_BYTES:
            try:
                real_path = await _download_parallel(
                    client, message, media, local_path, job)
                secs = max(loop.time() - t0, 0.01)
                log.info("descarga paralela OK: %s en %.1fs (%.2f MB/s)",
                         real_path.name, secs, size / secs / 1048576)
                return real_path, None
            except _ParallelFallback as e:
                log.info("descarga paralela no aplica (%s); secuencial", e)
            except Exception as e:
                last_err = e
                log.warning("descarga paralela intento %d/%d: %s",
                            attempt, attempts, e)
                if attempt < attempts:
                    await asyncio.sleep(3)
                continue

        async def progress(current, total):
            job["dl_current"] = current
            if total:
                job["dl_total"] = total
            if DL_LIMIT_BPS > 0:
                elapsed = loop.time() - t0
                if elapsed > 0 and current / elapsed > DL_LIMIT_BPS:
                    await asyncio.sleep(min(0.2, (current / DL_LIMIT_BPS) - elapsed))

        try:
            result = await client.download_media(
                message,
                file_name=str(local_path),
                progress=progress,
            )
            if result:
                real_path = Path(result)
                if real_path.exists() and real_path.stat().st_size > 0:
                    secs = max(loop.time() - t0, 0.01)
                    log.info(
                        "descarga secuencial OK: %s en %.1fs (%.2f MB/s)",
                        real_path.name, secs,
                        real_path.stat().st_size / secs / 1048576)
                    return real_path, None
            last_err = "archivo no quedó en disco"
            log.warning("descarga intento %d/%d: archivo vacío o inexistente", attempt, attempts)
        except Exception as e:
            last_err = e
            log.warning("descarga intento %d/%d: %s", attempt, attempts, e)

        if attempt < attempts:
            await asyncio.sleep(3)

    return None, last_err

# --- subida ------------------------------------------------------------------
class UploadCancelled(Exception):
    pass

# --- error de descarga de enlaces ----------------------------------------------
class StreamFetchError(Exception):
    pass

def _cleanup_dir(d: Path):
    try:
        shutil.rmtree(d, ignore_errors=True)
    except Exception:
        pass

def _es_error_transitorio(e: BaseException) -> bool:
    """Error de red pasajero (timeout o corte de conexion), juzgado por
    el nombre del tipo para servir igual con requests, urllib3 o httpx.
    Contra s3.todus.cu desde fuera de Cuba las conexiones a veces no
    llegan a establecerse y reintentar la misma operacion suele
    funcionar (visto en vivo: un respaldo de estado entro al segundo
    intento tras un ConnectTimeout)."""
    name = type(e).__name__
    if name == "UploadCancelled":
        return False
    return any(k in name for k in
               ("Timeout", "Connect", "Network", "MaxRetry"))


def upload_sync(file_path: Path, file_name: str = "", state: dict | None = None) -> str:
    # El SDK exige los bytes completos; read_bytes evita la doble copia en memoria.
    data = file_path.read_bytes()
    def _cb(cur, tot):
        if state is not None:
            state["current"] = cur
            state["total"] = tot
            if state.get("cancelled"):
                raise UploadCancelled()
    kwargs = dict(file_name=file_name or file_path.name,
                  timeout=UPLOAD_TIMEOUT, progress_callback=_cb)
    # Ante errores de red pasajeros se reintenta la subida por el SDK
    # mismo (nunca se cae a /stream/: el modo estricto se mantiene y el
    # archivo conserva su ID). Las pausas son mas largas que en /stream/
    # porque el host tarda en volver a aceptar conexiones.
    for attempt in range(1, 4):
        try:
            return todus_client.upload_file(data, **kwargs)
        except Exception as e:
            if "Token" in type(e).__name__ or "Auth" in type(e).__name__:
                log.warning("sesión de toDus vencida; re-login y reintento de subida")
                todus_client.login_with_phone_only()
                if state is not None:
                    state["current"] = 0
                return todus_client.upload_file(data, **kwargs)
            if not _es_error_transitorio(e) or attempt == 3:
                raise
            log.warning("subida SDK intento %d/3: %s; reintento",
                        attempt, e)
            if state is not None:
                state["current"] = 0
                if state.get("cancelled"):
                    raise UploadCancelled()
            time.sleep(10 * attempt)

# --- subida directa /stream/ (sin cuenta toDus) -------------------------------
def _stream_object_key(chat_id: int, file_name: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in file_name) or "file.bin"
    return (f"users/tg_{abs(int(chat_id))}/telegram/"
            f"{stamp}-{uuid.uuid4().hex[:6]}/telegram_{uuid.uuid4().hex[:12]}_{safe}")

def _stream_headers(name: str, size: int) -> dict:
    return {
        "Content-Type": mimetypes.guess_type(name)[0] or "application/octet-stream",
        "Content-Length": str(size),
        "Content-Disposition": f"attachment; filename=\"{name}\"" if name.lower().endswith(".apk")
                               else f"inline; filename=\"{name}\"",
        "User-Agent": "Wget/1.21.3",
        "Host": "s3.todus.cu",
        "Accept": "*/*",
        "Accept-Encoding": "identity",
        "Connection": "keep-alive",
    }

def _verify_stream_url(url: str) -> bool:
    """HEAD con la receta: True solo si el objeto responde 200."""
    try:
        with httpx.Client(timeout=httpx.Timeout(30), trust_env=False,
                          follow_redirects=True) as vc:
            return vc.head(url, **_recipe_kwargs(url)).status_code == 200
    except Exception:
        return False

def stream_upload_sync(file_path: Path, file_name: str = "", state: dict | None = None,
                       segment: tuple | None = None) -> str:
    """PUT sin autenticacion a s3.todus.cu/stream/<clave> con las cabeceras de
    la receta (UA Wget, Host fijado). Devuelve la URL publica /stream/.
    Transmite desde disco: no carga el archivo entero en memoria.
    segment=(offset, largo) sube solo ese tramo del archivo (una parte)."""
    full_size = file_path.stat().st_size
    seg_start, size = segment if segment else (0, full_size)
    name = file_name or file_path.name
    chat_id = (state or {}).get("chat_id", 0)
    key = _stream_object_key(chat_id, name)
    url = f"{STREAM_BASE_URL}/{quote(key, safe='/')}"
    headers = _stream_headers(name, size)
    timeout = httpx.Timeout(connect=30, read=UPLOAD_TIMEOUT, write=UPLOAD_TIMEOUT, pool=30)
    last_err = None
    for attempt in range(1, 4):
        if state is not None:
            state["current"] = 0
            state["total"] = size
            if state.get("cancelled"):
                raise UploadCancelled()
        def _gen():
            sent = 0
            with file_path.open("rb") as f:
                f.seek(seg_start)
                while sent < size:
                    if state is not None and state.get("cancelled"):
                        raise UploadCancelled()
                    chunk = f.read(min(1024 * 256, size - sent))
                    if not chunk:
                        break
                    sent += len(chunk)
                    if state is not None:
                        state["current"] = sent
                    yield chunk
        try:
            with httpx.Client(timeout=timeout, trust_env=False, follow_redirects=True) as client:
                resp = client.put(url, content=_gen(), headers=headers)
            if resp.status_code in (200, 201, 204):
                # Verificacion: el enlace solo se entrega si el objeto
                # responde de verdad (un 200 del PUT no basta).
                ok = _verify_stream_url(url)
                if ok:
                    log.info("stream upload OK (verificado) -> %s", url)
                    return url
                last_err = "PUT ok pero el objeto no responde al HEAD"
                log.warning("stream upload intento %d/3: %s", attempt, last_err)
            else:
                last_err = f"HTTP {resp.status_code}"
                log.warning("stream upload intento %d/3: %s", attempt, last_err)
        except UploadCancelled:
            raise
        except Exception as e:
            last_err = e
            log.warning("stream upload intento %d/3: %s", attempt, e)
        if attempt < 3:
            time.sleep(2 * attempt)
    raise RuntimeError(f"stream upload failed: {last_err}")

# --- metodo DevFast: ID de toDus, enlace v4 y enlace directo (IDM) ------------
def _extract_todus_id(url: str) -> str | None:
    """ID numerico de toDus de un archivo subido por el SDK, si la URL de
    descarga lo trae: parametro id/file_id/fid o segmento numerico del
    path (6+ digitos). Devuelve None si no se detecta."""
    try:
        u = urlparse(url)
    except Exception:
        return None
    qs = parse_qs(u.query)
    for k in ("id", "file_id", "fid"):
        for v in qs.get(k, []):
            if v.isdigit() and len(v) >= 6:
                return v
    for seg in u.path.split("/"):
        if seg.isdigit() and len(seg) >= 6:
            return seg
    return None

def _devfast_link(file_id: str, file_name: str) -> str:
    """Enlace en el formato que sirve la app DevFast en el telefono."""
    return f"http://127.0.0.1:7568/todus/v4/{file_id}/{quote(file_name, safe='')}"

def _sdk_extras_sync(down_url: str, file_name: str) -> dict:
    """Extras de una subida SDK: el ID de toDus con su enlace DevFast
    (v4) y la URL real de descarga resuelta por XMPP (enlace directo
    para IDM u otro gestor; es firmada y caduca, por eso no se guarda
    en el historial). Si una parte falla, devuelve lo que se pudo."""
    extras = {}
    fid = _extract_todus_id(down_url)
    if fid:
        extras["id"] = fid
        extras["v4"] = _devfast_link(fid, file_name)
    else:
        log.info("subida SDK sin ID numerico detectable en la URL: %s", down_url)
    try:
        real = todus_client.get_real_download_url(down_url)
        if real and real != down_url:
            extras["idm"] = real
    except Exception as e:
        log.info("no se pudo resolver la URL real de descarga (%s)", e)
    return extras

def _url_para_entregar(url: str, name: str) -> str:
    """URL tal como se entrega al usuario. En los archivos del SDK la
    ruta la asigna toDus (fecha + hash) y no trae el nombre; el nombre
    real se agrega como fragmento (#Nombre%20Real.mp4): el fragmento
    nunca viaja al servidor, la descarga es identica, y los
    descargadores que lo leen (Zunzun) guardan el archivo con su
    nombre real. Las demas rutas quedan como estan (/stream/ ya lleva
    el nombre en la propia clave)."""
    if "s3.todus.cu/todus/file/" in url and name:
        return f"{url}#{quote(name, safe='')}"
    return url


def upload_any(file_path: Path, file_name: str = "", state: dict | None = None,
               segment: tuple | None = None) -> str:
    """Ruta de subida. Sin segmentos: si TODUS_SDK_FIRST esta activo y el
    archivo cabe en el limite del SDK, va SOLO por el SDK (queda
    registrado en toDus con su ID: metodo DevFast) y de forma estricta:
    si el SDK falla, la subida falla. Si no, el orden clasico (/stream/
    primero, SDK de fallback). Los segmentos (partes de un archivo
    dividido) solo van por /stream/: el SDK no puede subir un tramo
    suelto."""
    size = file_path.stat().st_size
    sdk_ok = segment is None and size <= SDK_FALLBACK_MAX_BYTES
    if TODUS_SDK_FIRST and sdk_ok:
        # Estricto: si el SDK falla, la subida falla. Caer a /stream/
        # dejaria el archivo sin ID de toDus, que es justo lo que el
        # metodo viene a evitar.
        return upload_sync(file_path, file_name, state)
    if TODUS_STREAM_UPLOAD:
        try:
            return stream_upload_sync(file_path, file_name, state, segment)
        except UploadCancelled:
            raise
        except Exception as e:
            if segment is not None:
                raise RuntimeError(f"stream upload failed ({e})")
            if size > SDK_FALLBACK_MAX_BYTES:
                raise RuntimeError(
                    f"stream upload failed ({e}) and the file is too large "
                    f"for the SDK fallback (limit {fmt_bytes(SDK_FALLBACK_MAX_BYTES)})"
                )
            log.warning("stream upload no disponible (%s); fallback al SDK", e)
    return upload_sync(file_path, file_name, state)

# --- flujo directo Telegram -> toDus (sin archivo temporal) ----------------
class _DirectFallback(Exception):
    """El flujo directo no llego a empezar: usar la via clasica."""


class _OrderedPipe:
    """Buffer ordenado y acotado entre la descarga por rangos (async)
    y el PUT sincrono (hilo executor). Los tramos llegan fuera de
    orden y se sirven en orden; la ventana limita cuanto puede
    adelantarse la descarga respecto de lo que la subida ya consumio,
    asi la RAM queda plana sin importar el tamano del archivo."""

    def __init__(self, total: int, window: int):
        self.total = total
        self.window = window
        self._buf = {}
        self._read_pos = 0
        self._cond = threading.Condition()
        self._finished = False
        self._error = None
        self._aborted = False
        self._cur = b""
        self._cur_pos = 0

    # lado productor
    def put(self, offset: int, data: bytes):
        with self._cond:
            self._buf[offset] = data
            self._cond.notify_all()

    def finish(self, error=None):
        with self._cond:
            self._finished = True
            if error is not None and self._error is None:
                self._error = error
            self._cond.notify_all()

    def abort(self):
        with self._cond:
            self._aborted = True
            self._cond.notify_all()

    @property
    def read_pos(self) -> int:
        with self._cond:
            return self._read_pos

    @property
    def aborted(self) -> bool:
        with self._cond:
            return self._aborted

    # lado consumidor (bloqueante)
    def read(self, n: int = -1) -> bytes:
        if n is None or n < 0:
            n = 1024 * 256
        while True:
            if self._cur_pos < len(self._cur):
                chunk = self._cur[self._cur_pos:self._cur_pos + n]
                self._cur_pos += len(chunk)
                with self._cond:
                    self._read_pos += len(chunk)
                    self._cond.notify_all()
                if self._cur_pos >= len(self._cur):
                    self._cur = b""
                    self._cur_pos = 0
                return chunk
            with self._cond:
                while (self._read_pos not in self._buf
                       and not self._finished and not self._aborted
                       and self._error is None):
                    self._cond.wait(timeout=1.0)
                # Lo ya bufferado se sirve primero: el error del
                # productor solo salta cuando falta el tramo esperado.
                data = self._buf.pop(self._read_pos, None)
                if data is not None:
                    self._cur = data
                    self._cur_pos = 0
                    continue
                if self._error is not None:
                    raise self._error
                if self._aborted:
                    raise UploadCancelled()
                if self._read_pos >= self.total:
                    return b""
                raise RuntimeError(
                    "flujo directo: la descarga termino sin el tramo "
                    f"esperado en {self._read_pos}")


class _PipeBody:
    """Cuerpo para requests/httpx que sirve el pipe en orden y refleja
    el avance en el estado del trabajo (como el callback del SDK)."""

    def __init__(self, pipe: _OrderedPipe, job):
        self._pipe = pipe
        self._job = job

    def __len__(self) -> int:
        return self._pipe.total

    def read(self, n: int = -1) -> bytes:
        if self._job is not None and self._job.get("cancelled"):
            raise UploadCancelled()
        data = self._pipe.read(1024 * 256 if not n or n < 0 else n)
        if data and self._job is not None:
            self._job["current"] = self._job.get("current", 0) + len(data)
        return data


def _sdk_put_pipe_sync(pipe, size: int, name: str, job) -> str:
    """PUT del SDK en flujo: misma reserva y misma sesion que
    todus_client.upload_file, pero el cuerpo sale del pipe en vez de
    un bytes con el archivo entero en RAM. Devuelve la down_url (con
    su ID, como la subida clasica del SDK)."""
    from todus.types import FileType
    up_url, down_url = todus_client.reserve_upload_url(
        size, FileType.FILE, file_name=name)
    resp = todus_client.session.put(
        up_url,
        data=_PipeBody(pipe, job),
        headers={
            "Content-Type": "application/octet-stream",
            "Content-Length": str(size),
        },
        timeout=UPLOAD_TIMEOUT,
    )
    resp.raise_for_status()
    if job is not None:
        job["current"] = size
    return down_url


def _stream_put_pipe_sync(pipe, size: int, name: str, job) -> str:
    """PUT /stream/ en flujo (un intento): el cuerpo sale del pipe.
    Los reintentos los hace _upload_direct con un pipe nuevo por
    intento, igual que la via clasica reintenta desde el archivo."""
    key = _stream_object_key((job or {}).get("chat_id", 0), name)
    url = f"{STREAM_BASE_URL}/{quote(key, safe='/')}"
    headers = _stream_headers(name, size)
    timeout = httpx.Timeout(connect=30, read=UPLOAD_TIMEOUT,
                            write=UPLOAD_TIMEOUT, pool=30)

    def _gen():
        body = _PipeBody(pipe, job)
        while True:
            chunk = body.read(1024 * 256)
            if not chunk:
                break
            yield chunk

    with httpx.Client(timeout=timeout, trust_env=False,
                      follow_redirects=True) as client:
        resp = client.put(url, content=_gen(), headers=headers)
    if resp.status_code in (200, 201, 204):
        if _verify_stream_url(url):
            log.info("stream upload OK (directo, verificado) -> %s", url)
            return url
        raise RuntimeError("PUT ok pero el objeto no responde al HEAD")
    raise RuntimeError(f"HTTP {resp.status_code}")


async def _download_parallel_pipe(client, message, media, pipe, job,
                                  started):
    """Descarga por rangos que alimenta el pipe en vez de un archivo.
    Gemela de _download_parallel: mismos pedidos GetFile, mismos
    reintentos y misma renovacion de file_reference; cambia el destino
    (el pipe, con su ventana) y avisa por `started` cuando el primer
    tramo ya esta puesto — recien ahi el llamador abre el PUT, asi un
    caso no aplicable (CDN) cae a la via clasica sin haber subido
    ningun byte."""
    from pyrogram import raw
    from pyrogram.errors import FileReferenceExpired
    size = media.file_size or 0
    fid, location = _location_de(media.file_id)
    session = await client.get_session(fid.dc_id, is_media=True)
    job["dl_total"] = size
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    state = {"done": 0, "next": 0, "refreshed": False}
    lock = asyncio.Lock()

    async def _fetch(offset):
        r = await session.invoke(
            raw.functions.upload.GetFile(
                location=location, offset=offset, limit=DL_CHUNK),
            sleep_threshold=60)
        if isinstance(r, raw.types.upload.FileCdnRedirect):
            raise _ParallelFallback("el archivo se sirve por CDN")
        return r.bytes

    async def _worker():
        nonlocal location
        while True:
            if job.get("cancelled"):
                raise UploadCancelled()
            async with lock:
                idx = state["next"]
                state["next"] += 1
            offset = idx * DL_CHUNK
            if offset >= size:
                return
            # Ventana: no adelantar la descarga mas alla de lo que la
            # subida ya consumio, mas la ventana configurada.
            while offset >= pipe.read_pos + pipe.window \
                    and not pipe.aborted:
                if job.get("cancelled"):
                    raise UploadCancelled()
                await asyncio.sleep(0.2)
            if pipe.aborted:
                raise UploadCancelled()
            want = min(DL_CHUNK, size - offset)
            data = None
            for intento in range(1, 4):
                try:
                    data = await _fetch(offset)
                    if len(data) != want:
                        raise RuntimeError(
                            f"tramo corto: {len(data)} de {want}")
                    break
                except FileReferenceExpired:
                    if state["refreshed"]:
                        raise
                    state["refreshed"] = True
                    fresh = await client.get_messages(
                        message.chat.id, message.id)
                    fmedia = _job_media(fresh)
                    if fmedia is None:
                        raise
                    _, location = _location_de(fmedia.file_id)
                    log.info("file_reference renovado en la descarga "
                             "(directo)")
                except _ParallelFallback:
                    raise
                except Exception:
                    if intento == 3:
                        raise
                    await asyncio.sleep(2 * intento)
            pipe.put(offset, data)
            if idx == 0:
                started.set()
            async with lock:
                state["done"] += len(data)
                done = state["done"]
            job["dl_current"] = done
            if DL_LIMIT_BPS > 0:
                elapsed = loop.time() - t0
                if elapsed > 0 and done / elapsed > DL_LIMIT_BPS:
                    await asyncio.sleep(
                        min(0.2, done / DL_LIMIT_BPS - elapsed))

    try:
        await asyncio.gather(
            *[_worker() for _ in range(max(1, DL_WORKERS))])
        if state["done"] != size:
            raise RuntimeError(
                f"descarga incompleta: {state['done']} de {size}")
        pipe.finish()
    except BaseException as e:
        pipe.finish(error=e)
        raise


async def _direct_attempt(job, size: int, name: str, route: str) -> str:
    """Un intento completo de flujo directo por una ruta ("sdk" o
    "stream"). El PUT abre solo cuando el primer tramo ya llego; si la
    descarga no puede arrancar, _DirectFallback y ningun byte subio."""
    media = _job_media(job["message"])
    pipe = _OrderedPipe(size, DIRECT_WINDOW_BYTES)
    started = asyncio.Event()
    job["phase"] = "uploading"
    job["total"] = size
    job["current"] = 0
    prod = asyncio.ensure_future(
        _download_parallel_pipe(app, job["message"], media, pipe, job,
                                started))
    start_wait = asyncio.ensure_future(started.wait())
    await asyncio.wait({prod, start_wait},
                       return_when=asyncio.FIRST_COMPLETED)
    if not started.is_set():
        start_wait.cancel()
        pipe.abort()
        exc = None
        if prod.done() and not prod.cancelled():
            exc = prod.exception()
        raise _DirectFallback(
            str(exc or "la descarga no arranco")) from exc
    start_wait.cancel()
    loop = asyncio.get_running_loop()
    try:
        if route == "sdk":
            url = await loop.run_in_executor(
                None, _sdk_put_pipe_sync, pipe, size, name, job)
        else:
            url = await loop.run_in_executor(
                None, _stream_put_pipe_sync, pipe, size, name, job)
    except BaseException:
        pipe.abort()
        prod.cancel()
        raise
    try:
        await asyncio.wait_for(asyncio.shield(prod), timeout=10)
    except Exception:
        prod.cancel()
    return url


async def _upload_direct(job, size: int, name: str) -> str:
    """Flujo directo con el mismo enrutado que upload_any: SDK
    estricto si TODUS_SDK_FIRST y el archivo cabe en el limite del
    SDK; si no, /stream/ con sus reintentos y el SDK de fallback."""
    sdk_ok = size <= SDK_FALLBACK_MAX_BYTES
    if TODUS_SDK_FIRST and sdk_ok:
        # Estricto igual que en la via clasica: jamas se cae a
        # /stream/, pero ante errores de red pasajeros se reintenta el
        # intento SDK completo (pipe y descarga nuevos en cada uno).
        for attempt in range(1, 4):
            try:
                return await _direct_attempt(job, size, name, "sdk")
            except (UploadCancelled, _DirectFallback):
                raise
            except Exception as e:
                if "Token" in type(e).__name__ or "Auth" in type(e).__name__:
                    log.warning("sesion de toDus vencida; re-login y "
                                "reintento (flujo directo)")
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(
                        None, todus_client.login_with_phone_only)
                    job["current"] = 0
                    return await _direct_attempt(job, size, name, "sdk")
                if not _es_error_transitorio(e) or attempt == 3:
                    raise
                log.warning("subida SDK (directo) intento %d/3: %s; "
                            "reintento", attempt, e)
                job["current"] = 0
                await asyncio.sleep(10 * attempt)
    last_err = None
    for attempt in range(1, 4):
        try:
            return await _direct_attempt(job, size, name, "stream")
        except (UploadCancelled, _DirectFallback):
            raise
        except Exception as e:
            last_err = e
            log.warning("stream upload (directo) intento %d/3: %s",
                        attempt, e)
            if attempt < 3:
                await asyncio.sleep(2 * attempt)
    if not sdk_ok:
        raise RuntimeError(
            f"stream upload failed ({last_err}) and the file is too "
            f"large for the SDK fallback (limit "
            f"{fmt_bytes(SDK_FALLBACK_MAX_BYTES)})")
    log.warning("stream upload (directo) no disponible (%s); fallback "
                "al SDK", last_err)
    return await _direct_attempt(job, size, name, "sdk")


def _direct_applies(job) -> bool:
    """El flujo directo aplica a archivos de Telegram de una sola
    parte y con tamano suficiente para la descarga por rangos."""
    if not TODUS_DIRECT_FLOW or job.get("kind") != "file":
        return False
    size = job.get("total") or 0
    if size < DL_PARALLEL_MIN_BYTES:
        return False
    if len(_parts_for(size, job.get("name", ""))) != 1:
        return False
    media = _job_media(job["message"])
    if media is None or not getattr(media, "file_id", None):
        return False
    try:
        _location_de(media.file_id)
    except Exception:
        return False
    return True


async def _process_job_direct(job):
    """Trabajo completo en flujo directo: descarga y subida fundidas,
    sin archivo temporal. El cierre (historial, tanda y mensaje
    final) replica el de process_job para una sola parte."""
    message = job["message"]
    chat_id = job.get("chat_id", message.chat.id)
    size = job["total"]
    base_name = job.get("name", "") or f"file_{message.id}"
    job["file_size"] = size
    job["phase"] = "uploading"
    job["up_t0"] = time.time()
    job["up_base"] = 0
    job["up_part"] = 1
    job["up_parts"] = 1
    loop = asyncio.get_running_loop()
    try:
        url = await _upload_direct(job, size, base_name)
    except _DirectFallback as e:
        log.info("flujo directo no aplica (%s); via clasica", e)
        if await _job_download_file(job):
            await process_job(job)
        return
    except UploadCancelled:
        job["phase"] = "cancelled"
        try:
            await message.reply("🚫 **Upload cancelled**")
        except Exception:
            pass
        return
    except Exception as e:
        job["phase"] = "failed"
        log.exception("upload failed (flujo directo)")
        try:
            await message.reply(f"❌ **Upload failed**\n`{e}`")
        except Exception:
            pass
        return
    route = "stream" if "/stream/" in url else "sdk"
    extras = {}
    if route == "sdk":
        extras = await loop.run_in_executor(None, _sdk_extras_sync,
                                            url, base_name)
    url = _url_para_entregar(url, base_name)
    entry = {"url": url, "name": base_name, "size": size,
             "ts": int(time.time()), "route": route,
             "chat_id": chat_id, "deleted": False}
    if extras.get("id"):
        entry["id"] = extras["id"]
        entry["v4"] = extras["v4"]
    _history_add(entry)
    _batch_add(chat_id, base_name, url, extras)
    log.info("uploaded (flujo directo) → %s", url)
    job["phase"] = "done"
    _up_t = max(time.time() - job.get("up_t0", time.time()), 0.01)
    _tot_t = time.time() - job.get("t0", job.get("up_t0", time.time()))
    resumen = (f"◷ {_fmt_eta_corta(_tot_t)} · » "
               f"{fmt_bytes(size / _up_t)}/s promedio")
    markup = KB_DELETE if "s3.todus.cu/stream/" in url else None
    try:
        done_msg = await message.reply(
            f"**Proceso terminado**\n`{url}`\n{resumen}",
            link_preview_options=NO_PREVIEW, reply_markup=markup)
        if markup is not None and done_msg is not None:
            DONE_LINKS[(chat_id, done_msg.id)] = url
    except Exception:
        log.exception("no se pudo mandar el mensaje Done")

def _parts_for(size: int, name: str) -> list:
    """[(offset, largo, nombre_parte)] — una sola parte si cabe en
    MAX_FILE_BYTES; si no, partes de ese tamano con sufijo .parteNNN."""
    if not MAX_FILE_BYTES or size <= MAX_FILE_BYTES:
        return [(0, size, name)]
    parts = []
    off = 0
    i = 1
    while off < size:
        length = min(MAX_FILE_BYTES, size - off)
        parts.append((off, length, f"{name}.parte{i:03d}"))
        off += length
        i += 1
    return parts

# --- tanda de enlaces por chat: al final sale un .txt con todos -------------
BATCH_LINKS = {}  # chat_id -> {"items": [(nombre, url, extras)], "last": ts}
BATCH_IDLE_SECONDS = 6  # segundos sin actividad para cerrar la tanda

def _batch_add(chat_id, name, url, extras=None):
    b = BATCH_LINKS.setdefault(chat_id, {"items": [], "last": 0.0})
    b["items"].append((name, url, extras or {}))
    b["last"] = time.time()

def _chat_busy(chat_id) -> bool:
    return any(j.get("chat_id") == chat_id for j in JOBS.values())

async def _send_links_file(chat_id, items):
    fecha = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"Enlaces de toDus — {fecha}", ""]
    for name, url, extras in items:
        lines.append(name)
        lines.append(url)
        if extras.get("v4"):
            lines.append(f"DevFast: {extras['v4']}")
        if extras.get("idm"):
            lines.append(f"Directo (IDM): {extras['idm']}")
        lines.append("")
    path = TMP_DIR / f"enlaces_{chat_id}_{int(time.time())}.txt"
    path.write_text("\n".join(lines), encoding="utf-8")
    try:
        await app.send_document(chat_id, str(path),
                                caption=f"🔗 **{len(items)} enlace(s)** de esta tanda")
    finally:
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass
    log.info("txt de enlaces enviado a %s (%d enlaces)", chat_id, len(items))

async def _batch_flusher():
    """Cada pocos segundos cierra las tandas de los chats que ya quedaron
    libres (sin trabajos activos) y manda el .txt con todos los enlaces."""
    while True:
        await asyncio.sleep(3)
        now = time.time()
        for chat_id in list(BATCH_LINKS.keys()):
            b = BATCH_LINKS.get(chat_id)
            if not b or not b["items"]:
                continue
            if now - b["last"] < BATCH_IDLE_SECONDS or _chat_busy(chat_id):
                continue
            items = b["items"]
            BATCH_LINKS.pop(chat_id, None)
            try:
                await _send_links_file(chat_id, items)
            except Exception:
                log.exception("no se pudo mandar el .txt de enlaces")

# --- estado persistente en S3 -------------------------------------------------
# access.json e history.json viven en /tmp: cada redespliegue de Render los
# borra (asi se perdian los usuarios autorizados). El estado se guarda como
# un objeto JSON en el bucket /stream/ con una clave fija derivada del
# hash del BOT_TOKEN (imposible de adivinar sin el token): PUT al cambiar,
# GET al arrancar, fusionando con lo local para no pisar cambios nuevos.
# (Via Telegram no se puede: los bots no pueden leer historiales de chat,
# Telegram devuelve BOT_METHOD_INVALID a messages.GetHistory.)
import hashlib
STATE_FILE_NAME = "todus-state.json"
# Los objetos /stream/ de toDus son temporales (~24 h): el estado se
# re-guarda cada 6 h aunque no haya cambios, para que el respaldo nunca
# venza mientras el bot este vivo.
STATE_REFRESH_SECONDS = 6 * 3600
_state_dirty = False
_state_last_put = 0.0
_state_lock = asyncio.Lock()
_state_boot_task = None
_state_loop_started = False

def _state_url():
    admin = _admin_id()
    if not admin:
        return None
    digest = hashlib.sha256(BOT_TOKEN.encode()).hexdigest()[:24]
    return f"{STREAM_BASE_URL}/users/tg_{admin}/telegram/state-{digest}.json"

def _state_touch(immediate=False):
    global _state_dirty
    _state_dirty = True
    if immediate:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(_state_backup())

async def _state_backup():
    global _state_dirty, _state_last_put
    url = _state_url()
    if not url:
        return
    async with _state_lock:
        _state_dirty = False
        _state_last_put = time.time()
        try:
            state = {"v": 1, "access": _access_load(), "history": _history_load()}
            data = json.dumps(state).encode()
            async with httpx.AsyncClient(timeout=httpx.Timeout(30), trust_env=False) as c:
                r = await c.put(url, content=data,
                                headers=_stream_headers(STATE_FILE_NAME, len(data)))
            if r.status_code not in (200, 201):
                raise RuntimeError(f"PUT del estado -> HTTP {r.status_code}")
            log.info("estado respaldado en S3")
        except Exception:
            _state_dirty = True
            log.exception("respaldo de estado falló")

async def _state_loop():
    while True:
        await asyncio.sleep(60)
        por_vencer = time.time() - _state_last_put > STATE_REFRESH_SECONDS
        if _state_dirty or por_vencer:
            await _state_backup()

async def _state_restore():
    url = _state_url()
    if not url:
        return
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30), trust_env=False,
                                     follow_redirects=True) as c:
            r = await c.get(url, **_recipe_kwargs(url))
        if r.status_code == 404:
            log.info("sin respaldo de estado previo en S3")
            return
        if r.status_code != 200:
            log.warning("restaurar estado: GET -> HTTP %s", r.status_code)
            return
        data = json.loads(r.content.decode("utf-8"))
        # fusion de accesos: union de permitidos, admin local manda
        la = _access_load()
        ba = data.get("access") or {}
        def _ids(d):
            out = set()
            for x in d.get("allowed", []):
                try:
                    out.add(int(x))
                except Exception:
                    pass
            return out
        allowed = sorted(_ids(la) | _ids(ba))
        admin_v = int(la.get("admin") or 0) or int(ba.get("admin") or 0)
        _access_path().write_text(json.dumps({"admin": admin_v, "allowed": allowed}))
        # fusion de historial por URL (lo local gana en duplicados)
        lh = _history_load()
        bh = data.get("history") or []
        seen = {}
        for e in list(bh) + list(lh):
            if isinstance(e, dict) and e.get("url"):
                seen[e["url"]] = e
        merged = sorted(seen.values(), key=lambda e: e.get("ts", 0))[-200:]
        _history_path().write_text(json.dumps(merged))
        log.info("estado restaurado desde S3: %d permitidos, %d en historial",
                 len(allowed), len(merged))
    except Exception:
        log.exception("restaurar estado falló")

async def _state_boot_once():
    """Restaura el estado la primera vez que un handler lo pide y deja
    corriendo el respaldo periodico. Llamadas siguientes no hacen nada."""
    global _state_boot_task, _state_loop_started
    if _state_boot_task is None:
        _state_boot_task = asyncio.ensure_future(_state_restore())
    try:
        await _state_boot_task
    except Exception:
        pass
    # cada arranque tambien renueva la fecha del respaldo en S3
    _state_touch()
    if not _state_loop_started:
        _state_loop_started = True
        asyncio.ensure_future(_state_loop())

# --- tandas por chat: de 1 en 1, un solo mensaje de estado -------------------
# Cada chat tiene una tanda: los archivos se procesan EN ORDEN (descargar y
# subir uno, despues el siguiente; el resto espera en cola) y UN solo
# mensaje muestra el archivo actual: Archivo X / N, progreso, velocidad y
# ETA. Al drenar la tanda el mensaje se borra y llega el .txt de enlaces.
from collections import deque
BATCHES = {}  # chat_id -> estado de la tanda
BATCH_MAX_PENDING = 16

def _batch_pending(st) -> int:
    return len(st["queue"]) + (1 if st["current"] else 0)

def _batch_enqueue(chat_id, job):
    st = BATCHES.get(chat_id)
    if st is None:
        st = {"queue": deque(), "current": None, "done_count": 0, "total": 0,
              "status_msg": None, "renderer": None, "runner": None,
              "snap_t": 0.0, "snap_cur": 0, "snap_key": None, "last_speed": 0.0}
        BATCHES[chat_id] = st
        st["runner"] = asyncio.ensure_future(_batch_runner(chat_id, job["message"]))
    st["total"] += 1
    st["queue"].append(job)

def _batch_speed(st, job, cur):
    now = time.time()
    if st["snap_key"] != job["key"]:
        st["snap_key"] = job["key"]
        st["snap_t"] = now
        st["snap_cur"] = cur
        st["last_speed"] = 0.0
        return 0.0
    dt = now - st["snap_t"]
    if dt < 0.5:
        return st["last_speed"]
    st["last_speed"] = max(0.0, (cur - st["snap_cur"]) / dt)
    st["snap_t"] = now
    st["snap_cur"] = cur
    return st["last_speed"]

def _fmt_eta_corta(eta):
    if eta <= 0:
        return "—"
    if eta < 120:
        return f"{eta:.0f}s"
    return f"{int(eta // 60)}m {int(eta % 60)}s"

def _batch_lines(st):
    job = st["current"]
    n_total = st["total"]
    if job is None:
        if st["queue"]:
            nxt = st["queue"][0]
            return (f"⏳ **En cola**\n"
                    f"📂 Archivo {st['done_count'] + 1} / {n_total}: {nxt.get('display', '')}\n"
                    f"⏳ Esperando turno…")
        return "⭐ **Preparando…**"
    idx = st["done_count"] + 1
    name = job.get("display") or job.get("name", "archivo")
    ph = job.get("phase", "")
    if ph == "downloading":
        head = "⭐ Descargando desde Telegram..."
        cur, tot = job.get("dl_current", 0), job.get("dl_total", 0)
    elif ph == "uploading":
        head = "⭐ Subiendo a toDus..."
        cur = job.get("up_base", 0) + job.get("current", 0)
        tot = job.get("file_size", 0) or job.get("total", 0)
    else:
        head = "⏳ **En cola**"
        cur, tot = 0, job.get("total", 0)
    # Bloque en árbol de WZML-X (get_readable_message), con las
    # etiquetas traducidas; el «head» de fase pasa a ser la línea
    # Estado del árbol y el lote se ve en el número (idx / total).
    estado = ("Subiendo a toDus" if ph == "uploading"
              else "Descargando desde Telegram")
    lines = [f"**{idx} / {n_total}. {name}**"]
    if tot > 0:
        pct = cur * 100 / tot
        speed = _batch_speed(st, job, cur)
        eta = (tot - cur) / speed if speed > 0 else 0
        lines.append(f"┟ [{fmt_bar(pct)}] {pct:.2f}%")
        lines.append(f"┠ Procesado → {fmt_bytes(cur)} de {fmt_bytes(tot)}")
        if ph == "uploading" and job.get("up_parts", 1) > 1:
            lines.append(f"┠ Parte → {job.get('up_part', 1)} / {job['up_parts']}")
        lines.append(f"┠ Estado → {estado}")
        lines.append(f"┠ Velocidad → {fmt_bytes(speed)}/s")
        if speed > 0:
            lines.append(f"┠ Tiempo → {_fmt_eta_corta(eta)} de "
                         f"{_fmt_eta_corta(tot / speed)} "
                         f"( {_fmt_eta_corta(cur / speed)} )")
        else:
            lines.append("┠ Tiempo → —")
    else:
        lines.append(f"┠ Procesado → {fmt_bytes(cur)}")
        lines.append(f"┠ Estado → {estado}")
    # Cierre tipo leech (WZML-X): el comando /cancel_<id> de ESTA
    # tarea, tocable desde el propio mensaje de progreso.
    if job.get("cancelled"):
        lines.append("┖ Cancelando…")
    else:
        lines.append(f"┖ Cancelar → /cancel_{job['key'][1]}")
    return "\n".join(lines)

async def _batch_render_loop(chat_id):
    st = BATCHES.get(chat_id)
    while BATCHES.get(chat_id) is st:
        await asyncio.sleep(EDIT_INTERVAL)
        msg = st.get("status_msg")
        if msg is None:
            continue
        try:
            # Sin botones: la cancelación va en el propio mensaje,
            # con el /cancel_<id> de la última línea del árbol.
            await msg.edit_text(_batch_lines(st), link_preview_options=NO_PREVIEW)
        except Exception:
            pass

async def _job_download_file(job) -> bool:
    """Descarga el archivo de Telegram a disco (fase del runner)."""
    message = job["message"]
    job["phase"] = "downloading"
    needed = job.get("total") or job.get("dl_total") or 0
    if needed and not await _wait_tmp_space(job, needed):
        try:
            if job.get("cancelled"):
                await message.reply("🚫 **Cancelled**")
            else:
                await message.reply(
                    "❌ **Not enough temporary space** on the server "
                    "right now — try again in a bit.")
        except Exception:
            pass
        return False
    real_path, dl_err = await download_with_progress(app, message, job["local_path"], job, attempts=3)
    if real_path is None:
        try:
            job["local_path"].unlink(missing_ok=True)
        except Exception:
            pass
        try:
            await message.reply(f"❌ **Download failed**\n`{dl_err}`")
        except Exception:
            pass
        return False
    if job.get("cancelled"):
        try:
            real_path.unlink(missing_ok=True)
        except Exception:
            pass
        try:
            await message.reply("🚫 **Cancelled**")
        except Exception:
            pass
        return False
    job["path"] = real_path
    return True

async def _job_fetch_url(job) -> bool:
    """Descarga el archivo de un enlace directo a disco (fase del runner)."""
    message = job["message"]
    job["phase"] = "downloading"
    job_dir = TMP_DIR / f"u{message.chat.id}_{message.id}"
    job_dir.mkdir(parents=True, exist_ok=True)
    job["job_dir"] = job_dir
    tmp_path = job_dir / "download.bin"
    state = {"cancelled": False, "current": 0, "total": 0}
    task = asyncio.ensure_future(fetch_url(job["url"], tmp_path, state))
    while not task.done():
        await asyncio.sleep(1)
        if job.get("cancelled"):
            state["cancelled"] = True
        job["dl_current"] = state.get("current", 0)
        job["dl_total"] = state.get("total", 0)
    try:
        size, suggested = task.result()
    except UploadCancelled:
        try:
            await message.reply("🚫 **Cancelled**")
        except Exception:
            pass
        _cleanup_dir(job_dir)
        return False
    except StreamFetchError as e:
        code = str(e)
        if code == "too_big":
            txt = (f"❌ That file is over the server limit of {fmt_bytes(_limit_bytes())}.\n"
                   "You can change the limit with the MAX_TOTAL_MB and TMP_BUDGET_MB variables.")
        else:
            txt = f"❌ **Fetch failed**\n`{code}`"
        try:
            await message.reply(txt)
        except Exception:
            pass
        _cleanup_dir(job_dir)
        return False
    except Exception as e:
        log.exception("url fetch failed")
        try:
            await message.reply(f"❌ **Fetch failed**\n`{e}`")
        except Exception:
            pass
        _cleanup_dir(job_dir)
        return False
    safe_name = "".join(c if c.isalnum() or c in "._-" else "_" for c in suggested) or "download.bin"
    real_path = TMP_DIR / f"{message.chat.id}_{message.id}_{safe_name}"
    try:
        shutil.move(str(tmp_path), str(real_path))
    except Exception:
        real_path = tmp_path
    if real_path != tmp_path:
        _cleanup_dir(job_dir)
    job["name"] = safe_name
    job["display"] = safe_name
    job["path"] = real_path
    job["total"] = size
    return True

async def _batch_runner(chat_id, anchor_msg):
    st = BATCHES[chat_id]
    try:
        try:
            st["status_msg"] = await anchor_msg.reply("⭐ **Preparando…**")
        except Exception:
            st["status_msg"] = None
        st["renderer"] = asyncio.ensure_future(_batch_render_loop(chat_id))
        while True:
            if not st["queue"]:
                await asyncio.sleep(4)  # gracia por si llega otro archivo pegado
                if not st["queue"]:
                    break
                continue
            job = st["queue"].popleft()
            st["current"] = job
            job["t0"] = time.time()
            try:
                if job.get("cancelled"):
                    try:
                        await job["message"].reply("🚫 **Cancelled**")
                    except Exception:
                        pass
                elif job["kind"] == "file":
                    if _direct_applies(job):
                        await _process_job_direct(job)
                    elif await _job_download_file(job):
                        await process_job(job)
                else:
                    if await _job_fetch_url(job):
                        await process_job(job)
            except Exception as e:
                log.exception("batch job failed")
                try:
                    await job["message"].reply(f"❌ {e}")
                except Exception:
                    pass
            finally:
                st["current"] = None
                st["done_count"] += 1
                JOBS.pop(job.get("key"), None)
                _job_cleanup(job)
                gc.collect()
    finally:
        if st.get("renderer"):
            st["renderer"].cancel()
        msg = st.get("status_msg")
        BATCHES.pop(chat_id, None)
        if msg is not None:
            try:
                await msg.delete()
            except Exception:
                pass

# --- worker ------------------------------------------------------------------
async def process_job(job):
    message = job["message"]  # mensaje original del usuario (archivo o enlace)
    path = job["path"]
    chat_id = job.get("chat_id", message.chat.id)
    size = path.stat().st_size
    job["file_size"] = size
    base_name = job.get("name", "") or path.name
    parts = _parts_for(size, base_name)
    multi = len(parts) > 1
    loop = asyncio.get_running_loop()
    job["phase"] = "uploading"
    job["up_t0"] = time.time()
    try:
        urls = []
        for idx, (off, length, part_name) in enumerate(parts, 1):
            if job.get("cancelled"):
                raise UploadCancelled()
            job["total"] = length
            job["current"] = 0
            job["up_base"] = off
            job["up_part"] = idx
            job["up_parts"] = len(parts)
            upload_task = asyncio.ensure_future(
                loop.run_in_executor(None, upload_any, path, part_name, job,
                                     (off, length) if multi else None)
            )
            while not upload_task.done():
                await asyncio.sleep(1)
            url = upload_task.result()
            route = "stream" if "/stream/" in url else "sdk"
            extras = {}
            if route == "sdk":
                extras = await loop.run_in_executor(None, _sdk_extras_sync,
                                                    url, part_name)
            url = _url_para_entregar(url, part_name)
            entry = {"url": url, "name": part_name, "size": length,
                     "ts": int(time.time()), "route": route,
                     "chat_id": chat_id,
                     "deleted": False}
            if extras.get("id"):
                entry["id"] = extras["id"]
                entry["v4"] = extras["v4"]
            _history_add(entry)
            urls.append(url)
            _batch_add(chat_id, part_name, url, extras)
            log.info("uploaded → %s", url)
        job["phase"] = "done"
        _up_t = max(time.time() - job.get("up_t0", time.time()), 0.01)
        _tot_t = time.time() - job.get("t0", job.get("up_t0", time.time()))
        resumen = (f"◷ {_fmt_eta_corta(_tot_t)} · » "
                   f"{fmt_bytes(size / _up_t)}/s promedio")
        if not multi:
            markup = KB_DELETE if "s3.todus.cu/stream/" in urls[0] else None
            try:
                done_msg = await message.reply(f"**Proceso terminado**\n`{urls[0]}`\n{resumen}",
                                               link_preview_options=NO_PREVIEW,
                                               reply_markup=markup)
                if markup is not None and done_msg is not None:
                    DONE_LINKS[(chat_id, done_msg.id)] = urls[0]
            except Exception:
                log.exception("no se pudo mandar el mensaje Done")
        else:
            lines = [f"**Proceso terminado** — `{base_name}` ({fmt_bytes(size)}) split into {len(urls)} parts:"]
            for i, u in enumerate(urls, 1):
                lines.append(f"**Part {i}:**\n`{u}`")
            lines.append(f"To join: `cat {base_name}.parte* > {base_name}` (or 7-Zip / HJSplit).")
            lines.append(resumen)
            try:
                await message.reply("\n".join(lines), link_preview_options=NO_PREVIEW)
            except Exception:
                log.exception("no se pudo mandar el mensaje Done (partes)")
    except UploadCancelled:
        job["phase"] = "cancelled"
        try:
            await message.reply("🚫 **Upload cancelled**")
        except Exception:
            pass
    except Exception as e:
        job["phase"] = "failed"
        log.exception("upload failed")
        try:
            await message.reply(f"❌ **Upload failed**\n`{e}`")
        except Exception:
            pass
    finally:
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass

# --- Kurigram ----------------------------------------------------------------
app = Client(
    "todus_bot",
    api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN,
    workers=4, sleep_threshold=30,
)

def ensure_worker():
    global worker_started
    if not worker_started:
        worker_started = True
        asyncio.create_task(_batch_flusher())
        log.info("flusher de tandas iniciado")

# --- control de acceso ---------------------------------------------------------
# Admin: OWNER_ID (variable de entorno) o el reclamado con /claim (access.json).
# El resto: denegado, salvo los ID que el admin autorice con /allow.
def _access_path() -> Path:
    return TMP_DIR / "access.json"

def _access_load() -> dict:
    try:
        data = json.loads(_access_path().read_text())
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {"admin": 0, "allowed": []}

def _access_save(data: dict):
    try:
        _access_path().write_text(json.dumps(data))
    except Exception:
        pass
    _state_touch(immediate=True)

def _admin_id() -> int:
    if OWNER_ID:
        return OWNER_ID
    return int(_access_load().get("admin") or 0)

def _allowed_ids() -> set:
    return {int(x) for x in _access_load().get("allowed", []) if str(x).lstrip("-").isdigit()}

def _uid(message) -> int:
    return message.from_user.id if message is not None and message.from_user else 0

def _is_admin(message) -> bool:
    uid = _uid(message)
    return bool(uid) and uid == _admin_id()

def _can_use_id(uid: int) -> bool:
    return bool(uid) and (uid == _admin_id() or uid in _allowed_ids())

def _can_use(message) -> bool:
    return _can_use_id(_uid(message))

DENIED_NOTIFIED = set()

async def _deny(message):
    uid = _uid(message)
    try:
        await message.reply(
            f"🚫 **Access denied.**\nYour Telegram ID is `{uid}`.\n"
            f"Ask the admin to grant you access with: /allow {uid}"
        )
    except Exception:
        pass
    admin = _admin_id()
    if admin and uid and uid != admin and uid not in DENIED_NOTIFIED:
        DENIED_NOTIFIED.add(uid)
        try:
            who = message.from_user
            name = ((who.first_name or "Someone") if who else "Someone")
            if who and who.username:
                name += f" (@{who.username})"
            await app.send_message(
                admin,
                f"🔔 {name} (`{uid}`) tried to use the bot and was denied.\n"
                f"Grant access with: /allow {uid}",
            )
        except Exception:
            pass

# --- handlers ----------------------------------------------------------------
@app.on_message(filters.command("start"))
async def cmd_start(client, message):
    ensure_worker()
    await _state_boot_once()
    if not _can_use(message):
        if not _admin_id():
            uid = _uid(message)
            await message.reply(
                "🔒 **This bot has no admin yet.**\n"
                f"Your Telegram ID is `{uid}`.\n"
                "If this is your bot, send /claim to become its admin. "
                "Otherwise, ask the admin to grant you access."
            )
            return
        await _deny(message)
        return
    admin_help = ""
    if _is_admin(message):
        admin_help = ("\n👑 **Admin:** /allow <id> grants access · /revoke <id> removes it · "
                      "/users lists who has access.")
    await message.reply(
        "📤 **Send me a file and I'll upload it to toDus.**\n"
        "Or send me a direct download link (http/https) and I'll fetch it and upload it.\n"
        "/links shows your recent uploads and whether they're still alive.\n"
        "To cancel an upload, tap the /cancel_<id> shown in the progress message, or reply /cancel to the file you sent.\n"
        f"Queue: {sum(_batch_pending(st) for st in BATCHES.values())} · Download: {'unlimited' if DL_LIMIT_BPS == 0 else fmt_bytes(DL_LIMIT_BPS)+'/s'}"
        + admin_help
    )

@app.on_message(filters.command("claim"))
async def cmd_claim(client, message):
    await _state_boot_once()
    if _admin_id():
        await message.reply("An admin is already set for this bot.")
        return
    uid = _uid(message)
    if not uid:
        return
    data = _access_load()
    data["admin"] = uid
    _access_save(data)
    await message.reply(
        f"👑 **You are now the admin of this bot** (ID `{uid}`).\n"
        "Only you — and the users you grant with /allow <id> — can use it.\n"
        "Tip: for a permanent anchor, also set OWNER_ID in the server variables."
    )

@app.on_message(filters.command("allow"))
async def cmd_allow(client, message):
    await _state_boot_once()
    if not _is_admin(message):
        await _deny(message)
        return
    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].lstrip("-").isdigit():
        await message.reply("Usage: /allow <telegram_id>")
        return
    uid = int(parts[1])
    if uid == _admin_id():
        await message.reply("That ID is the admin — it already has full access.")
        return
    data = _access_load()
    allowed = [int(x) for x in data.get("allowed", [])]
    if uid in allowed:
        await message.reply(f"`{uid}` already has access.")
        return
    allowed.append(uid)
    data["allowed"] = allowed
    _access_save(data)
    await message.reply(f"✅ Access granted to `{uid}`.")
    try:
        await app.send_message(uid, "✅ The admin granted you access to the toDus bot. Send /start to begin.")
    except Exception:
        pass

@app.on_message(filters.command("revoke"))
async def cmd_revoke(client, message):
    await _state_boot_once()
    if not _is_admin(message):
        await _deny(message)
        return
    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].lstrip("-").isdigit():
        await message.reply("Usage: /revoke <telegram_id>")
        return
    uid = int(parts[1])
    data = _access_load()
    allowed = [int(x) for x in data.get("allowed", [])]
    if uid not in allowed:
        await message.reply(f"`{uid}` doesn't have access.")
        return
    data["allowed"] = [x for x in allowed if x != uid]
    _access_save(data)
    await message.reply(f"🚫 Access revoked for `{uid}`.")
    try:
        await app.send_message(uid, "🚫 Your access to the toDus bot was revoked by the admin.")
    except Exception:
        pass

@app.on_message(filters.command("users"))
async def cmd_users(client, message):
    if not _is_admin(message):
        await _deny(message)
        return
    allowed = sorted(_allowed_ids())
    lines = [f"👑 Admin: `{_admin_id()}`", f"👥 Allowed users: {len(allowed)}"]
    lines += [f"· `{uid}`" for uid in allowed]
    if not allowed:
        lines.append("Grant access with /allow <telegram_id>.")
    await message.reply("\n".join(lines))

@app.on_message(filters.command("cancel"))
async def cmd_cancel(client, message):
    if not _can_use(message):
        return
    target = message.reply_to_message
    if not target:
        await message.reply("Reply with /cancel to the file (or link message) you want to cancel, or tap the /cancel_<id> shown in the progress message.")
        return
    job = JOBS.get((message.chat.id, target.id))
    if not job:
        await message.reply("That upload already finished or isn't running.")
        return
    job["cancelled"] = True
    await message.reply("🚫 Cancellation requested.")

@app.on_message(filters.regex(r"^/cancel_\d+\s*$"))
async def cmd_cancel_id(client, message):
    """Cancelar por id de tarea, estilo leech: el /cancel_<id> que el
    propio mensaje de progreso muestra en su última línea (┖)."""
    if not _can_use(message):
        return
    m = re.match(r"^/cancel_(\d+)", message.text or "")
    if not m:
        return
    job = JOBS.get((message.chat.id, int(m.group(1))))
    if not job:
        await message.reply("That upload already finished or isn't running.")
        return
    job["cancelled"] = True
    await message.reply("🚫 Cancellation requested.")

@app.on_message(filters.command("links"))
async def cmd_links(client, message):
    await _state_boot_once()
    if not _can_use(message):
        await _deny(message)
        return
    items = [it for it in _history_load() if it.get("chat_id") == message.chat.id][-10:]
    if not items:
        await message.reply("No uploads recorded yet in this chat.")
        return
    status_msg = await message.reply("🔎 Checking your links…")
    lines = ["🔗 **Your recent uploads**"]
    checks = []
    for it in items:
        if it.get("deleted"):
            checks.append(None)
        else:
            checks.append(_head_status(it["url"]))
    results = await asyncio.gather(*[c for c in checks if c is not None], return_exceptions=True)
    ri = 0
    for idx, it in enumerate(items, 1):
        when = datetime.fromtimestamp(it.get("ts", 0), timezone.utc).strftime("%b %d, %H:%M UTC")
        head = f"{idx}. {it.get('name', 'file')} — {fmt_bytes(it.get('size', 0))} · {when}"
        if it.get("deleted"):
            lines.append(f"🗑 ~~{head}~~ (deleted)")
            continue
        st = results[ri] if ri < len(results) else None
        ri += 1
        if isinstance(st, int) and st in (200, 206):
            icon = "✅"
        elif isinstance(st, int) and st in (404, 410):
            icon = "💀 expired"
        elif isinstance(st, int):
            icon = f"❓ HTTP {st}"
        else:
            icon = "❓ unreachable"
        lines.append(f"{icon} {head}\n`{it['url']}`")
        if it.get("v4"):
            lines.append(f"📲 DevFast: `{it['v4']}`")
    try:
        await status_msg.edit_text("\n".join(lines), link_preview_options=NO_PREVIEW)
    except Exception:
        pass

@app.on_callback_query()
async def cb_cancel(client, cq):
    if not _can_use_id(cq.from_user.id if cq.from_user else 0):
        await cq.answer("🚫 Access denied.", show_alert=True)
        return
    if cq.data == "del" and cq.message:
        url = DONE_LINKS.get((cq.message.chat.id, cq.message.id))
        if not url:
            await cq.answer("That file is no longer registered for deletion.")
            return
        await cq.answer("🗑 Deleting…")
        if await _delete_stream_object(url):
            DONE_LINKS.pop((cq.message.chat.id, cq.message.id), None)
            _history_mark_deleted(url)
            try:
                await cq.message.edit_text("🗑 **Deleted**\nThe file was removed from toDus.",
                                           )
            except Exception:
                pass
        else:
            await cq.answer("❌ Could not delete it — the file is still up.", show_alert=True)
        return
    job = None
    if cq.message and cq.data and cq.data.startswith("cancel:"):
        try:
            job = JOBS.get((cq.message.chat.id, int(cq.data.split(":", 1)[1])))
        except ValueError:
            job = None
    elif cq.message:
        job = JOBS.get((cq.message.chat.id, cq.message.id))
    if not job:
        await cq.answer("That upload already finished.")
        return
    job["cancelled"] = True
    await cq.answer("🚫 Cancellation requested.")

@app.on_message(filters.document | filters.video | filters.audio | filters.photo | filters.voice)
async def handle_file(client, message: Message):
    ensure_worker()
    await _state_boot_once()
    if not _can_use(message):
        await _deny(message)
        return
    media = message.document or message.video or message.audio or message.photo or message.voice
    if not media:
        return

    raw_name = (
        getattr(message.document, "file_name", None)
        or getattr(message.video, "file_name", None)
        or getattr(message.audio, "file_name", None)
        or f"file_{message.id}"
    )
    safe_name = "".join(c if c.isalnum() or c in "._-" else "_" for c in raw_name)
    local_path = TMP_DIR / f"{message.chat.id}_{message.id}_{safe_name}"

    total = getattr(media, "file_size", 0) or 0
    if _too_big(total):
        await message.reply(
            f"❌ That file is {fmt_bytes(total)} and the server limit is "
            f"{fmt_bytes(_limit_bytes())}.\n"
            "You can change the limit with the MAX_TOTAL_MB and TMP_BUDGET_MB variables."
        )
        return

    st = BATCHES.get(message.chat.id)
    if st is not None and _batch_pending(st) >= BATCH_MAX_PENDING:
        try:
            await message.reply("❌ The upload queue is full — try again in a bit.")
        except Exception:
            pass
        return

    job = {"message": message, "path": None, "name": safe_name, "display": safe_name,
           "kind": "file", "local_path": local_path,
           "cancelled": False, "current": 0, "total": total,
           "dl_current": 0, "dl_total": total, "phase": "waiting",
           "key": (message.chat.id, message.id), "chat_id": message.chat.id}
    JOBS[job["key"]] = job
    _batch_enqueue(message.chat.id, job)

# --- subida desde un enlace directo -------------------------------------------
async def fetch_url(url: str, dest: Path, state: dict):
    """Descarga un enlace http/https directo a disco. Devuelve (bytes, nombre)."""
    total = 0
    suggested = ""
    timeout = httpx.Timeout(connect=30, read=120, write=30, pool=30)
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                             "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
               "Accept": "*/*"}
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, trust_env=False) as client:
        async with client.stream("GET", url, headers=headers) as resp:
            if resp.status_code != 200:
                raise StreamFetchError(f"HTTP {resp.status_code}")
            cd = resp.headers.get("content-disposition", "")
            m = re.search(r"filename\*=UTF-8''([^;]+)", cd) or re.search(r'filename="?([^";]+)"?', cd)
            if m:
                from urllib.parse import unquote
                suggested = unquote(m.group(1)).strip()
            declared = int(resp.headers.get("content-length") or 0)
            if _too_big(declared):
                raise StreamFetchError("too_big")
            state["total"] = declared
            with dest.open("wb") as f:
                async for chunk in resp.aiter_bytes(1024 * 256):
                    if state.get("cancelled"):
                        raise UploadCancelled()
                    total += len(chunk)
                    if _too_big(total):
                        raise StreamFetchError("too_big")
                    state["current"] = total
                    f.write(chunk)
    if not suggested:
        from urllib.parse import unquote, urlsplit
        suggested = unquote(urlsplit(url).path.rsplit("/", 1)[-1]) or "download.bin"
    return total, suggested

@app.on_message(filters.regex(r"https?://") & filters.text)
async def handle_url_link(client, message: Message):
    ensure_worker()
    await _state_boot_once()
    if not _can_use(message):
        await _deny(message)
        return
    text = message.text or message.caption or ""
    if STREAM_RE.search(text):
        return  # los enlaces /stream/ de toDus se ignoran: esa descarga se quito
    m = URL_RE.search(text)
    if not m:
        return
    url = m.group(0).rstrip(".,")
    if "s3.todus.cu/stream/" in url:
        return
    st = BATCHES.get(message.chat.id)
    if st is not None and _batch_pending(st) >= BATCH_MAX_PENDING:
        try:
            await message.reply("❌ The upload queue is full — try again in a bit.")
        except Exception:
            pass
        return
    display = url.rsplit("/", 1)[-1][:40] or "enlace"
    job = {"message": message, "path": None, "name": "", "display": display,
           "kind": "url", "url": url,
           "cancelled": False, "current": 0, "total": 0,
           "dl_current": 0, "dl_total": 0, "phase": "waiting",
           "key": (message.chat.id, message.id), "chat_id": message.chat.id}
    JOBS[job["key"]] = job
    _batch_enqueue(message.chat.id, job)

# --- main --------------------------------------------------------------------
def main():
    _sweep_tmp()
    start_keep_alive()
    login_todus()
    log.info("bot starting")
    app.run()

if __name__ == "__main__":
    main()
