from flask import Flask, Response, jsonify, request, stream_with_context
from flask_cors import CORS
from flask_sock import Sock
import requests
import websocket
import threading
import json
import os

app = Flask(__name__)
sock = Sock(app)

# ✅ เปิดใช้ CORS ให้ frontend เรียกได้
CORS(
    app,
    supports_credentials=True,
    origins=["*"]   # หรือจะระบุ ["http://localhost:3020", "http://10.0.32.18:3020"] ก็ได้
)

# -------------------------------
# โหลด service.json แบบ hot reload
# -------------------------------
def load_services():
    try:
        with open("service.json", "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️ Error loading service.json: {e}")
        return {}

# -------------------------------
# Proxy API ทุก service
# -------------------------------
@app.route("/api/<service>/<path:endpoint>", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
def gateway(service, endpoint):
    services = load_services()
    if service not in services:
        return jsonify({"error": f"Service '{service}' not found"}), 404

    base_url = services[service].rstrip("/")  # กัน slash ซ้ำ
    url = f"{base_url}/{endpoint}"

    try:
        # เตรียม Headers ใหม่ โดยเพิ่ม Proxy Headers เข้าไป
        proxy_headers = {k: v for k, v in request.headers if k.lower() != "host"}
        proxy_headers["X-Forwarded-Prefix"] = f"/api/{service}"
        proxy_headers["X-Forwarded-For"] = request.remote_addr
        proxy_headers["X-Forwarded-Proto"] = request.scheme

        resp = requests.request(
            method=request.method,
            url=url,
            headers=proxy_headers,
            params=request.args,
            json=request.get_json(silent=True),
            stream=True,
            timeout=(10, None)
        )

        # ✅ กรอง headers ที่ Flask ไม่รองรับ
        excluded_headers = ["content-encoding", "transfer-encoding", "connection"]
        headers = [(k, v) for k, v in resp.headers.items() if k.lower() not in excluded_headers]

        content_type = resp.headers.get("content-type", "").lower()
        if content_type.startswith("text/event-stream"):
            headers = [(k, v) for k, v in headers if k.lower() != "content-length"]
            if not any(k.lower() == "x-accel-buffering" for k, _ in headers):
                headers.append(("X-Accel-Buffering", "no"))

            def generate():
                try:
                    # Forward small SSE frames immediately instead of waiting
                    # for requests/urllib3 to fill its default buffer.
                    for chunk in resp.iter_content(chunk_size=1):
                        if chunk:
                            yield chunk
                finally:
                    resp.close()

            return Response(
                stream_with_context(generate()),
                status=resp.status_code,
                headers=headers,
                direct_passthrough=True,
            )

        try:
            return (resp.content, resp.status_code, headers)
        finally:
            resp.close()

    except Exception as e:
        return jsonify({"error": "Gateway Error", "details": str(e)}), 500

# -------------------------------
# Proxy WebSocket ทุก service
# -------------------------------
@sock.route("/api/<service>/<path:endpoint>")
def ws_gateway(ws, service, endpoint):
    services = load_services()
    if service not in services:
        ws.send(json.dumps({"error": f"Service '{service}' not found"}))
        ws.close()
        return

    base_url = services[service].rstrip("/")
    # แปลง HTTP เป็น WS
    ws_base_url = base_url.replace("http://", "ws://").replace("https://", "wss://")
    target_url = f"{ws_base_url}/{endpoint}"

    try:
        target_ws = websocket.create_connection(target_url)
    except Exception as e:
        ws.send(json.dumps({"error": "Target WS Connection Failed", "details": str(e)}))
        ws.close()
        return

    # Thread อ่านข้อมูลจาก Target แล้วส่งให้ Client
    def forward_target_to_client():
        try:
            while True:
                data = target_ws.recv()
                ws.send(data)
        except Exception:
            try:
                ws.close()
            except:
                pass

    t = threading.Thread(target=forward_target_to_client, daemon=True)
    t.start()

    # Loop หลัก อ่านข้อมูลจาก Client แล้วส่งให้ Target
    try:
        while True:
            data = ws.receive()
            target_ws.send(data)
    except Exception:
        pass
    finally:
        target_ws.close()

# -------------------------------
# Health check ของ Gateway
# -------------------------------
@app.route("/api/health", methods=["GET"])
def health():
    services = load_services()
    return jsonify({
        "status": "ok",
        "services": list(services.keys())
    }), 200

# -------------------------------
# Main run
# -------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=port, debug=debug, threaded=True)
