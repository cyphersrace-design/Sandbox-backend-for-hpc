import os
import json
from typing import Dict
from flask import Flask, jsonify, request
from flask_cors import CORS
import redis
import uuid

# ----------------------------
# App configuration
# ----------------------------
APP_NAME = "Sandbox Backend Flask"
APP_VERSION = "1.0.0"

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

app = Flask(__name__)
CORS(app)

# ----------------------------
# Redis client
# ----------------------------
try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True
    )
    redis_client.ping()
    REDIS_STATUS = "connected"
except Exception as exc:
    redis_client = None
    REDIS_STATUS = f"disconnected: {str(exc)}"

# ----------------------------
# In-memory sandbox storage
# ----------------------------
sandbox_store: Dict = {}

# ----------------------------
# Helpers
# ----------------------------
def create_sandbox_id() -> str:
    return str(uuid.uuid4())[:8]

def validate_sandbox_payload(data):
    if not data or "name" not in data:
        return False, "Missing required field: name"
    return True, None

def store_in_redis(key, value):
    if redis_client is not None:
        try:
            redis_client.set(key, json.dumps(value))
            return True
        except Exception:
            return False
    return False

def get_from_redis(key):
    if redis_client is not None:
        try:
            value = redis_client.get(key)
            return json.loads(value) if value else None
        except Exception:
            return None
    return None

# ----------------------------
# Routes
# ----------------------------

@app.route("/", methods=["GET"])
def root():
    return jsonify({
        "app": APP_NAME,
        "version": APP_VERSION,
        "redis": REDIS_STATUS
    }), 200

@app.route("/health", methods=["GET"])
def health():
    if redis_client is not None:
        try:
            redis_client.ping()
            redis_status = "ok"
        except Exception as exc:
            redis_status = f"error: {str(exc)}"
    else:
        redis_status = "not_configured"

    return jsonify({
        "status": "ok",
        "redis": redis_status
    }), 200

@app.route("/sandboxes", methods=["GET"])
def list_sandboxes():
    return jsonify({
        "count": len(sandbox_store),
        "sandboxes": list(sandbox_store.values())
    }), 200

@app.route("/sandboxes", methods=["POST"])
def create_sandbox():
    data = request.get_json()

    is_valid, error_msg = validate_sandbox_payload(data)
    if not is_valid:
        return jsonify({"error": error_msg}), 400

    sandbox_id = create_sandbox_id()
    language = data.get("language", "python")
    resources = data.get("resources", {"cpu": 1, "memory": 512})

    sandbox = {
        "id": sandbox_id,
        "name": data["name"],
        "language": language,
        "status": "created",
        "resources": resources,
    }

    sandbox_store[sandbox_id] = sandbox
    store_in_redis(f"sandbox:{sandbox_id}", sandbox)

    return jsonify(sandbox), 201

@app.route("/sandboxes/<sandbox_id>", methods=["GET"])
def get_sandbox(sandbox_id):
    sandbox = sandbox_store.get(sandbox_id)
    if not sandbox:
        return jsonify({"error": "Sandbox not found"}), 404
    return jsonify(sandbox), 200

@app.route("/sandboxes/<sandbox_id>/start", methods=["POST"])
def start_sandbox(sandbox_id):
    sandbox = sandbox_store.get(sandbox_id)
    if not sandbox:
        return jsonify({"error": "Sandbox not found"}), 404

    sandbox["status"] = "running"
    sandbox_store[sandbox_id] = sandbox
    store_in_redis(f"sandbox:{sandbox_id}:status", "running")

    return jsonify({
        "id": sandbox_id,
        "status": "running",
        "message": "Sandbox started successfully"
    }), 200

@app.route("/sandboxes/<sandbox_id>/stop", methods=["POST"])
def stop_sandbox(sandbox_id):
    sandbox = sandbox_store.get(sandbox_id)
    if not sandbox:
        return jsonify({"error": "Sandbox not found"}), 404

    sandbox["status"] = "stopped"
    sandbox_store[sandbox_id] = sandbox
    store_in_redis(f"sandbox:{sandbox_id}:status", "stopped")

    return jsonify({
        "id": sandbox_id,
        "status": "stopped",
        "message": "Sandbox stopped successfully"
    }), 200

@app.route("/sandboxes/<sandbox_id>", methods=["DELETE"])
def delete_sandbox(sandbox_id):
    if sandbox_id not in sandbox_store:
        return jsonify({"error": "Sandbox not found"}), 404

    del sandbox_store[sandbox_id]

    if redis_client is not None:
        try:
            redis_client.delete(f"sandbox:{sandbox_id}")
            redis_client.delete(f"sandbox:{sandbox_id}:status")
        except Exception:
            pass

    return jsonify({"message": "Sandbox deleted successfully"}), 200

@app.route("/sandboxes/<sandbox_id>/logs", methods=["GET"])
def get_sandbox_logs(sandbox_id):
    sandbox = sandbox_store.get(sandbox_id)
    if not sandbox:
        return jsonify({"error": "Sandbox not found"}), 404

    logs = get_from_redis(f"sandbox:{sandbox_id}:logs")
    if not logs:
        logs = ["No logs available"]

    return jsonify({
        "id": sandbox_id,
        "logs": logs
    }), 200

@app.route("/sandboxes/<sandbox_id>/exec", methods=["POST"])
def exec_in_sandbox(sandbox_id):
    sandbox = sandbox_store.get(sandbox_id)
    if not sandbox:
        return jsonify({"error": "Sandbox not found"}), 404

    data = request.get_json()
    command = data.get("command")

    if not command:
        return jsonify({"error": "Missing command"}), 400

    # Mock execution result
    result = {
        "id": sandbox_id,
        "command": command,
        "output": f"Executed: {command}",
        "exit_code": 0
    }

    # Store in Redis
    store_in_redis(f"sandbox:{sandbox_id}:exec:{uuid.uuid4()}", result)

    return jsonify(result), 200

@app.route("/sandboxes/<sandbox_id>/resources", methods=["PUT"])
def update_sandbox_resources(sandbox_id):
    sandbox = sandbox_store.get(sandbox_id)
    if not sandbox:
        return jsonify({"error": "Sandbox not found"}), 404

    data = request.get_json()
    resources = data.get("resources")

    if not resources:
        return jsonify({"error": "Missing resources"}), 400

    sandbox["resources"] = resources
    sandbox_store[sandbox_id] = sandbox
    store_in_redis(f"sandbox:{sandbox_id}", sandbox)

    return jsonify({
        "id": sandbox_id,
        "resources": resources,
        "message": "Resources updated"
    }), 200

# ----------------------------
# Error handlers
# ----------------------------

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Route not found"}), 404

@app.errorhandler(500)
def server_error(error):
    return jsonify({"error": "Internal server error"}), 500

# ----------------------------
# Main
# ----------------------------

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        debug=os.getenv("FLASK_ENV") == "development"
    )