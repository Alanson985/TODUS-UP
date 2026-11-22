import httpx

URL = "https://s3.todus.cu/stream/1e129177_-1003735367650_3202__D-C_+817.mp4"
HEADERS = {"Host": "s3.todus.cu", "User-Agent": "Wget/1.21.3", "Accept": "*/*", "Accept-Encoding": "identify"}
EXT = {"sni_hostname": "s3.todus.cu"}

def show(tag, resp):
    print(f"--- {tag}: HTTP {resp.status_code}")
    for k in ("content-length", "content-type", "accept-ranges", "content-range", "server", "x-amz-request-id"):
        if k in resp.headers:
            print(f"    {k}: {resp.headers[k]}")

with httpx.Client(timeout=30, trust_env=False) as c:
    try:
        r = c.head(URL, headers=HEADERS, extensions=EXT, follow_redirects=True)
        show("HEAD tal cual lo mando tu colega (Wget + SNI)", r)
    except Exception as e:
        print("HEAD colega -> ERROR:", type(e).__name__, str(e)[:160])
    try:
        r = c.head(URL, follow_redirects=True)
        show("HEAD normal (sin headers especiales)", r)
    except Exception as e:
        print("HEAD normal -> ERROR:", type(e).__name__, str(e)[:160])
    try:
        r = c.get(URL, headers={**HEADERS, "Range": "bytes=0-1023"}, extensions=EXT, follow_redirects=True)
        show("GET rango 0-1023 con headers del colega", r)
        print("    bytes recibidos:", len(r.content))
    except Exception as e:
        print("GET rango -> ERROR:", type(e).__name__, str(e)[:160])
