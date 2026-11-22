import os
import threading
import time
import logging
import httpx
from flask import Flask

log = logging.getLogger(__name__)
app = Flask(__name__)

@app.route("/")
@app.route("/ping")
def ping():
    return "alive", 200

def _ping_loop():
    url = os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("SELF_URL")
    if not url:
        log.warning("no RENDER_EXTERNAL_URL — pinger disabled")
        return
    target = url.rstrip("/") + "/ping"
    time.sleep(10)
    while True:
        try:
            with httpx.Client(timeout=10.0) as c:
                r = c.get(target)
            log.info("ping %s → %s", target, r.status_code)
        except Exception as e:
            log.warning("ping failed: %s", e)
        time.sleep(14 * 60)

def start_keep_alive():
    port = int(os.environ.get("PORT", 10000))
    def _serve():
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)
    threading.Thread(target=_serve, daemon=True).start()
    threading.Thread(target=_ping_loop, daemon=True).start()
    log.info("keep_alive on :%d", port)
