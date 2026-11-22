import os, sys, asyncio
os.environ.setdefault("API_ID", "123456")
os.environ.setdefault("API_HASH", "0" * 32)
os.environ.setdefault("BOT_TOKEN", "123456:dummy-token-for-local-test")
os.environ.setdefault("TODUS_PHONE", "+5300000000")
sys.path.insert(0, ".")
from pathlib import Path
import bot

bot.TMP_DIR = Path("/tmp/todus_access_test")
import shutil; shutil.rmtree(bot.TMP_DIR, ignore_errors=True)
bot.TMP_DIR.mkdir(parents=True)

class U:
    def __init__(self, id, name="User", username=None):
        self.id = id; self.first_name = name; self.username = username
class Chat:
    def __init__(self, id): self.id = id
class M:
    def __init__(self, uid, text="", name="User", username=None):
        self.from_user = U(uid, name, username)
        self.chat = Chat(uid)
        self.text = text
        self.replies = []
    async def reply(self, text, **kw):
        self.replies.append(text)
        return self

async def main():
    # Sin admin: nadie puede usar el bot
    assert bot._admin_id() == 0
    assert not bot._can_use_id(111)
    print("OK: sin admin configurado, todo denegado")

    # /claim: el primero se convierte en admin
    alan = M(111, "/claim", "Alan")
    await bot.cmd_claim(None, alan)
    assert bot._admin_id() == 111 and bot._is_admin(alan)
    assert "admin of this bot" in alan.replies[-1]
    print("OK: /claim convierte al primero en admin")

    # Un segundo /claim ya no funciona
    otro = M(222, "/claim", "Otro")
    await bot.cmd_claim(None, otro)
    assert bot._admin_id() == 111 and "already set" in otro.replies[-1]
    print("OK: segundo /claim rechazado")

    # Extraño: /start denegado con su ID visible
    eve = M(333, "/start", "Eve", "eve")
    await bot.cmd_start(None, eve)
    assert "Access denied" in eve.replies[-1] and "333" in eve.replies[-1]
    print("OK: extraño recibe Access denied con su ID")

    # Extraño intentando /allow: denegado, sin cambios
    await bot.cmd_allow(None, M(333, "/allow 333"))
    assert not bot._can_use_id(333)
    print("OK: un extraño no puede auto-autorizarse")

    # Admin concede acceso por ID
    await bot.cmd_allow(None, M(111, "/allow 333"))
    assert bot._can_use_id(333) and not bot._is_admin(M(333))
    print("OK: /allow <id> concede acceso sin hacerlo admin")

    # El autorizado usa /start y NO ve la seccion de admin
    eve2 = M(333, "/start", "Eve", "eve")
    await bot.cmd_start(None, eve2)
    assert "Send me a file" in eve2.replies[-1] and "Admin:" not in eve2.replies[-1]
    # El admin si la ve
    alan2 = M(111, "/start", "Alan")
    await bot.cmd_start(None, alan2)
    assert "Admin:" in alan2.replies[-1] and "/allow" in alan2.replies[-1]
    print("OK: /start muestra ayuda segun el rol")

    # /users lista admin y autorizados
    lu = M(111, "/users")
    await bot.cmd_users(None, lu)
    assert "111" in lu.replies[-1] and "333" in lu.replies[-1]
    print("OK: /users lista admin y autorizados")

    # /revoke quita el acceso
    await bot.cmd_revoke(None, M(111, "/revoke 333"))
    assert not bot._can_use_id(333)
    print("OK: /revoke quita el acceso al instante")

    # OWNER_ID (entorno) tiene prioridad sobre el admin del archivo
    bot.OWNER_ID = 999
    assert bot._admin_id() == 999 and bot._can_use_id(999) and not bot._can_use_id(111)
    bot.OWNER_ID = 0
    assert bot._admin_id() == 111
    print("OK: OWNER_ID de entorno manda si esta cargado; si no, vale el de /claim")

asyncio.run(main())
print("TODAS LAS PRUEBAS DE ACCESO OK")
