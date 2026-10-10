import os
import json
import uuid
from typing import Dict, List, Any
from flask import Flask, jsonify, request
from flask_cors import CORS
import redis
import threading
import time
from dataclasses import dataclass, asdict
from enum import Enum

# ----------------------------
# Configuration
# ----------------------------
APP_NAME = "Server A - Sandbox System"
APP_VERSION = "1.0.0"

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

app = Flask(__name__)
CORS(app)

try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True
    )
    redis_client.ping()
    REDIS_STATUS = "connected"
except Exception as exc:
    redis_client = None
    REDIS_STATUS = f"disconnected: {str(exc)}"

# ----------------------------
# Data Models
# ----------------------------

@dataclass
class Subprocess:
    id: str
    process_id: str
    status: str
    created_at: float

@dataclass
class Process:
    id: str
    port: int
    status: str
    subprocesses: Dict[str, Subprocess]
    created_at: float

@dataclass
class Port:
    port_num: int
    box_id: str
    status: str
    processes: Dict[str, Process]
    created_at: float

@dataclass
class Box:
    id: str
    server: str
    status: str
    ports: Dict[int, Port]
    created_at: float

# ----------------------------
# Sandbox System (Server A)
# ----------------------------

class SandboxSystem:
    """
    Server A: 3 Groups of 12 boxes each = 36 total boxes
    Each box: 10 ports
    Each port: 5 processes
    Each process: 5 subprocesses
    
    Total: 36 boxes × 10 ports × 5 processes × 5 subprocesses = 9,000 subprocesses
    """

    def __init__(self, num_groups=3, boxes_per_group=12):
        self.num_groups = num_groups
        self.boxes_per_group = boxes_per_group
        self.total_boxes = num_groups * boxes_per_group  # 36 boxes
        self.ports_per_box = 10
        self.processes_per_port = 5
        self.subprocesses_per_process = 5

        self.boxes: Dict[str, Box] = {}
        self.ring_connections: Dict[str, str] = {}

        self._initialize_boxes()

    def _initialize_boxes(self):
        """Create all boxes with ports, processes, and subprocesses"""
        for group in range(self.num_groups):
            for box_num in range(self.boxes_per_group):
                box_id = f"box-g{group}-b{box_num}"
                box = Box(
                    id=box_id,
                    server="A",
                    status="initialized",
                    ports={},
                    created_at=time.time()
                )

                # Initialize 10 ports per box
                for port_num in range(self.ports_per_box):
                    port = Port(
                        port_num=8000 + port_num,
                        box_id=box_id,
                        status="idle",
                        processes={},
                        created_at=time.time()
                    )

                    # Initialize 5 processes per port
                    for proc_num in range(self.processes_per_port):
                        process = Process(
                            id=f"proc-{port_num}-{proc_num}",
                            port=8000 + port_num,
                            status="idle",
                            subprocesses={},
                            created_at=time.time()
                        )

                        # Initialize 5 subprocesses per process
                        for subprocess_num in range(self.subprocesses_per_process):
                            subprocess = Subprocess(
                                id=f"sub-{proc_num}-{subprocess_num}",
                                process_id=f"proc-{port_num}-{proc_num}",
                                status="idle",
                                created_at=time.time()
                            )
                            process.subprocesses[subprocess.id] = subprocess

                        port.processes[process.id] = process

                    box.ports[port.port_num] = port

                self.boxes[box_id] = box

    def get_box_hierarchy(self, box_id: str) -> Dict[str, Any]:
        """Get complete hierarchy of a box"""
        box = self.boxes.get(box_id)
        if not box:
            return None

        return {
            "box_id": box.id,
            "server": box.server,
            "status": box.status,
            "created_at": box.created_at,
            "ports_count": len(box.ports),
            "ports": {
                port_num: {
                    "port_num": port.port_num,
                    "status": port.status,
                    "processes_count": len(port.processes),
                    "processes": {
                        proc_id: {
                            "id": proc.id,
                            "port": proc.port,
                            "status": proc.status,
                            "subprocesses_count": len(proc.subprocesses),
                            "subprocesses": list(proc.subprocesses.keys())
                        }
                        for proc_id, proc in port.processes.items()
                    }
                }
                for port_num, port in box.ports.items()
            }
        }

    def activate_port(self, box_id: str, port_num: int):
        """Activate a port and all its processes/subprocesses"""
        box = self.boxes.get(box_id)
        if not box:
            return {"error": "Box not found"}

        port = box.ports.get(port_num)
        if not port:
            return {"error": "Port not found"}

        port.status = "active"
        for process in port.processes.values():
            process.status = "running"
            for subprocess in process.subprocesses.values():
                subprocess.status = "running"

        return {"message": "Port activated", "box_id": box_id, "port": port_num}

    def get_statistics(self) -> Dict[str, Any]:
        """Get system statistics"""
        total_boxes = len(self.boxes)
        total_ports = total_boxes * self.ports_per_box
        total_processes = total_ports * self.processes_per_port
        total_subprocesses = total_processes * self.subprocesses_per_process

        active_boxes = sum(1 for b in self.boxes.values() if b.status == "active")
        active_ports = sum(
            1 for b in self.boxes.values()
            for p in b.ports.values() if p.status == "active"
        )
        running_processes = sum(
            1 for b in self.boxes.values()
            for p in b.ports.values()
            for pr in p.processes.values() if pr.status == "running"
        )

        return {
            "server": "A",
            "total_boxes": total_boxes,
            "total_ports": total_ports,
            "total_processes": total_processes,
            "total_subprocesses": total_subprocesses,
            "active_boxes": active_boxes,
            "active_ports": active_ports,
            "running_processes": running_processes,
        }


# Initialize sandbox system
sandbox_system = SandboxSystem(num_groups=3, boxes_per_group=12)

# ----------------------------
# Ring Connection Logic
# ----------------------------

class RingRouter:
    """
    Ring routing connection logic for Server A ↔ Server B
    2 connections with 6 boxes each
    Ring topology: A1 → A2 → ... → B1 → B2 → ... → A1
    """

    def __init__(self):
        self.ring_nodes: List[str] = []
        self.node_index: Dict[str, int] = {}
        self._initialize_ring()

    def _initialize_ring(self):
        """Initialize ring with 6 boxes from A and 6 from B"""
        # Server A: 6 selected boxes
        for i in range(6):
            node_id = f"server-a-ring-{i}"
            self.ring_nodes.append(node_id)
            self.node_index[node_id] = len(self.ring_nodes) - 1

        # Server B: 6 selected boxes (will be populated by server B)
        for i in range(6):
            node_id = f"server-b-ring-{i}"
            self.ring_nodes.append(node_id)
            self.node_index[node_id] = len(self.ring_nodes) - 1

    def get_next_node(self, current_node: str) -> str:
        """Get next node in ring"""
        if current_node not in self.node_index:
            return None

        current_index = self.node_index[current_node]
        next_index = (current_index + 1) % len(self.ring_nodes)
        return self.ring_nodes[next_index]

    def get_prev_node(self, current_node: str) -> str:
        """Get previous node in ring"""
        if current_node not in self.node_index:
            return None

        current_index = self.node_index[current_node]
        prev_index = (current_index - 1) % len(self.ring_nodes)
        return self.ring_nodes[prev_index]

    def route_message(self, source: str, destination: str, message: str) -> Dict[str, Any]:
        """Route message through ring"""
        if source not in self.node_index or destination not in self.node_index:
            return {"error": "Invalid source or destination"}

        path = []
        current = source
        visited = set()

        while current != destination and current not in visited:
            path.append(current)
            visited.add(current)
            current = self.get_next_node(current)

        if current == destination:
            path.append(destination)
        else:
            return {"error": "Destination unreachable"}

        return {
            "source": source,
            "destination": destination,
            "path": path,
            "message": message,
            "hops": len(path) - 1
        }


ring_router = RingRouter()

# ----------------------------
# Flask Routes - Server A
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
    redis_status = "ok" if redis_client else "disconnected"
    return jsonify({
        "status": "ok",
        "server": "A",
        "redis": redis_status
    }), 200

@app.route("/stats", methods=["GET"])
def stats():
    return jsonify(sandbox_system.get_statistics()), 200

@app.route("/boxes", methods=["GET"])
def list_boxes():
    return jsonify({
        "count": len(sandbox_system.boxes),
        "boxes": list(sandbox_system.boxes.keys())
    }), 200

@app.route("/boxes/<box_id>", methods=["GET"])
def get_box(box_id):
    hierarchy = sandbox_system.get_box_hierarchy(box_id)
    if not hierarchy:
        return jsonify({"error": "Box not found"}), 404
    return jsonify(hierarchy), 200

@app.route("/boxes/<box_id>/activate", methods=["POST"])
def activate_box(box_id):
    result = sandbox_system.activate_port(box_id, 8000)
    return jsonify(result), 200 if "error" not in result else 400

@app.route("/ring/nodes", methods=["GET"])
def ring_nodes():
    return jsonify({
        "ring_nodes": ring_router.ring_nodes,
        "total_nodes": len(ring_router.ring_nodes)
    }), 200

@app.route("/ring/route", methods=["POST"])
def ring_route():
    data = request.get_json()
    source = data.get("source")
    destination = data.get("destination")
    message = data.get("message", "")

    result = ring_router.route_message(source, destination, message)
    return jsonify(result), 200 if "error" not in result else 400

@app.route("/ring/next/<node_id>", methods=["GET"])
def ring_next(node_id):
    next_node = ring_router.get_next_node(node_id)
    if not next_node:
        return jsonify({"error": "Node not found"}), 404
    return jsonify({"current": node_id, "next": next_node}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)