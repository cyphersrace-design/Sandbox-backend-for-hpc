import os
import json
import uuid
import time
import threading
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import defaultdict, OrderedDict
import logging
from queue import Queue, PriorityQueue
import hashlib

# ----------------------------
# Logging Configuration
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ----------------------------
# Enums
# ----------------------------

class MemoryType(Enum):
    TEMPORAL = "temporal"
    PERSPECTIVE = "perspective"
    CACHE = "cache"

class CacheLevel(Enum):
    L1 = "l1"  # Fastest, smallest
    L2 = "l2"  # Medium speed/size
    L3 = "l3"  # Slowest, largest

class JobState(Enum):
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"

class BufferState(Enum):
    EMPTY = "empty"
    FILLING = "filling"
    FULL = "full"
    FLUSHING = "flushing"

# ----------------------------
# Data Models
# ----------------------------

@dataclass
class Subprocess:
    id: str
    job_id: str
    status: JobState
    created_at: float
    execution_time: float = 0.0
    result: Any = None

@dataclass
class Job:
    id: str
    port_id: str
    status: JobState
    subprocesses: Dict[str, Subprocess] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    execution_time: float = 0.0
    result: Any = None

@dataclass
class Port:
    id: str
    port_number: int
    memory_box_id: str
    status: str
    jobs: Dict[str, Job] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)

@dataclass
class MemoryBox:
    id: str
    box_number: int
    server: str
    status: str
    ports: Dict[int, Port] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)

@dataclass
class TemporalMemory:
    """Tracks sequential events over time"""
    id: str
    timestamp: float
    event_type: str
    source: str
    data: Dict[str, Any]
    ttl: int = 3600  # 1 hour default
    created_at: float = field(default_factory=time.time)

@dataclass
class PerspectiveMemory:
    """Stores contextual state and relationships"""
    id: str
    entity_id: str
    perspective: str  # Context/viewpoint
    data: Dict[str, Any]
    relationships: List[str] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)

@dataclass
class CacheEntry:
    """Cache entry with metadata"""
    key: str
    value: Any
    level: CacheLevel
    timestamp: float = field(default_factory=time.time)
    hits: int = 0
    size: int = 0
    ttl: int = 300  # 5 minutes default

@dataclass
class NetworkBuffer:
    """Network buffering for cache handling"""
    id: str
    stage: int  # Stage 1 or 2
    state: BufferState
    capacity: int = 1024 * 1024  # 1MB
    current_size: int = 0
    data_queue: List[Tuple[str, Any]] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)

# ----------------------------
# Cache System with Three Levels
# ----------------------------

class CacheSystem:
    """
    Three-level cache system:
    L1: Ultra-fast, tiny (hottest data)
    L2: Medium speed/size
    L3: Slowest, largest
    """

    def __init__(self):
        self.l1_cache: OrderedDict = OrderedDict()
        self.l2_cache: OrderedDict = OrderedDict()
        self.l3_cache: OrderedDict = OrderedDict()

        self.l1_max_size = 1024 * 100  # 100KB
        self.l2_max_size = 1024 * 1024 * 10  # 10MB
        self.l3_max_size = 1024 * 1024 * 100  # 100MB

        self.l1_current_size = 0
        self.l2_current_size = 0
        self.l3_current_size = 0

        self.cache_lock = threading.Lock()
        self.hit_count = 0
        self.miss_count = 0

    def _get_entry_size(self, value: Any) -> int:
        """Estimate size of cached value"""
        try:
            return len(json.dumps(value))
        except:
            return 1024  # Default size

    def put(self, key: str, value: Any, level: CacheLevel = CacheLevel.L1) -> bool:
        """Put value in cache"""
        with self.cache_lock:
            size = self._get_entry_size(value)
            entry = CacheEntry(key=key, value=value, level=level, size=size)

            if level == CacheLevel.L1:
                return self._put_l1(key, entry)
            elif level == CacheLevel.L2:
                return self._put_l2(key, entry)
            else:
                return self._put_l3(key, entry)

    def _put_l1(self, key: str, entry: CacheEntry) -> bool:
        """Put in L1 cache with eviction"""
        size = entry.size

        if self.l1_current_size + size > self.l1_max_size:
            # Evict oldest entry and move to L2
            if self.l1_cache:
                evict_key, evict_entry = self.l1_cache.popitem(last=False)
                self.l1_current_size -= evict_entry.size
                self._put_l2(evict_key, evict_entry)

        self.l1_cache[key] = entry
        self.l1_current_size += size
        return True

    def _put_l2(self, key: str, entry: CacheEntry) -> bool:
        """Put in L2 cache with eviction"""
        size = entry.size

        if self.l2_current_size + size > self.l2_max_size:
            if self.l2_cache:
                evict_key, evict_entry = self.l2_cache.popitem(last=False)
                self.l2_current_size -= evict_entry.size
                self._put_l3(evict_key, evict_entry)

        self.l2_cache[key] = entry
        self.l2_current_size += size
        return True

    def _put_l3(self, key: str, entry: CacheEntry) -> bool:
        """Put in L3 cache with eviction"""
        size = entry.size

        if self.l3_current_size + size > self.l3_max_size:
            if self.l3_cache:
                evict_key, evict_entry = self.l3_cache.popitem(last=False)
                self.l3_current_size -= evict_entry.size
                # Data is lost if evicted from L3

        self.l3_cache[key] = entry
        self.l3_current_size += size
        return True

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache (checks all levels)"""
        with self.cache_lock:
            # Check L1
            if key in self.l1_cache:
                entry = self.l1_cache[key]
                entry.hits += 1
                self.hit_count += 1
                return entry.value

            # Check L2
            if key in self.l2_cache:
                entry = self.l2_cache[key]
                entry.hits += 1
                self.hit_count += 1
                # Move to L1 on hit
                self.l2_cache.pop(key)
                self.l2_current_size -= entry.size
                self._put_l1(key, entry)
                return entry.value

            # Check L3
            if key in self.l3_cache:
                entry = self.l3_cache[key]
                entry.hits += 1
                self.hit_count += 1
                # Move to L2 on hit
                self.l3_cache.pop(key)
                self.l3_current_size -= entry.size
                self._put_l2(key, entry)
                return entry.value

            self.miss_count += 1
            return None

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        with self.cache_lock:
            return {
                "l1": {
                    "entries": len(self.l1_cache),
                    "current_size": self.l1_current_size,
                    "max_size": self.l1_max_size,
                    "utilization": f"{(self.l1_current_size / self.l1_max_size * 100):.2f}%"
                },
                "l2": {
                    "entries": len(self.l2_cache),
                    "current_size": self.l2_current_size,
                    "max_size": self.l2_max_size,
                    "utilization": f"{(self.l2_current_size / self.l2_max_size * 100):.2f}%"
                },
                "l3": {
                    "entries": len(self.l3_cache),
                    "current_size": self.l3_current_size,
                    "max_size": self.l3_max_size,
                    "utilization": f"{(self.l3_current_size / self.l3_max_size * 100):.2f}%"
                },
                "hit_count": self.hit_count,
                "miss_count": self.miss_count,
                "hit_rate": f"{(self.hit_count / (self.hit_count + self.miss_count) * 100):.2f}%" if (self.hit_count + self.miss_count) > 0 else "0%"
            }

# ----------------------------
# Memory System (Temporal + Perspective)
# ----------------------------

class MemorySystem:
    """
    Combined temporal and perspective memory
    """

    def __init__(self):
        self.temporal_events: Dict[str, TemporalMemory] = OrderedDict()
        self.perspective_states: Dict[str, PerspectiveMemory] = {}
        self.memory_lock = threading.Lock()

    def record_temporal_event(self, event_type: str, source: str, data: Dict[str, Any],
                             ttl: int = 3600) -> str:
        """Record temporal event"""
        event_id = str(uuid.uuid4())[:12]

        event = TemporalMemory(
            id=event_id,
            timestamp=time.time(),
            event_type=event_type,
            source=source,
            data=data,
            ttl=ttl
        )

        with self.memory_lock:
            self.temporal_events[event_id] = event
            # Clean expired events
            self._cleanup_temporal_events()

        logger.info(f"Temporal event recorded: {event_id} ({event_type})")
        return event_id

    def _cleanup_temporal_events(self):
        """Remove expired temporal events"""
        current_time = time.time()
        expired = [
            eid for eid, event in self.temporal_events.items()
            if current_time - event.created_at > event.ttl
        ]
        for eid in expired:
            del self.temporal_events[eid]

    def get_temporal_events(self, source: str = None, 
                           event_type: str = None) -> List[Dict[str, Any]]:
        """Get temporal events with optional filtering"""
        with self.memory_lock:
            events = []
            for event in self.temporal_events.values():
                if source and event.source != source:
                    continue
                if event_type and event.event_type != event_type:
                    continue
                event_dict = asdict(event)
                events.append(event_dict)
            return sorted(events, key=lambda x: x['timestamp'])

    def store_perspective_memory(self, entity_id: str, perspective: str,
                                data: Dict[str, Any], 
                                relationships: List[str] = None) -> str:
        """Store perspective memory (contextual state)"""
        memory_id = str(uuid.uuid4())[:12]

        memory = PerspectiveMemory(
            id=memory_id,
            entity_id=entity_id,
            perspective=perspective,
            data=data,
            relationships=relationships or []
        )

        with self.memory_lock:
            self.perspective_states[memory_id] = memory

        logger.info(f"Perspective memory stored: {memory_id} ({perspective})")
        return memory_id

    def get_perspective_memory(self, entity_id: str = None,
                              perspective: str = None) -> List[Dict[str, Any]]:
        """Get perspective memory with optional filtering"""
        with self.memory_lock:
            memories = []
            for memory in self.perspective_states.values():
                if entity_id and memory.entity_id != entity_id:
                    continue
                if perspective and memory.perspective != perspective:
                    continue
                memories.append(asdict(memory))
            return memories

# ----------------------------
# Network Buffer System (Two Stages)
# ----------------------------

class NetworkBufferSystem:
    """
    Network buffering with two stages for cache handling
    """

    def __init__(self):
        self.stage1_buffer = NetworkBuffer(
            id="buffer-stage1",
            stage=1,
            state=BufferState.EMPTY,
            capacity=1024 * 512  # 512KB
        )
        self.stage2_buffer = NetworkBuffer(
            id="buffer-stage2",
            stage=2,
            state=BufferState.EMPTY,
            capacity=1024 * 1024  # 1MB
        )
        self.buffer_lock = threading.Lock()

    def put_to_buffer(self, stage: int, key: str, value: Any) -> bool:
        """Put data into network buffer"""
        with self.buffer_lock:
            buffer_obj = self.stage1_buffer if stage == 1 else self.stage2_buffer
            size = len(json.dumps(value))

            if buffer_obj.current_size + size > buffer_obj.capacity:
                buffer_obj.state = BufferState.FULL
                return False

            buffer_obj.data_queue.append((key, value))
            buffer_obj.current_size += size
            buffer_obj.state = BufferState.FILLING

            return True

    def flush_buffer(self, stage: int) -> List[Tuple[str, Any]]:
        """Flush buffer data"""
        with self.buffer_lock:
            buffer_obj = self.stage1_buffer if stage == 1 else self.stage2_buffer
            buffer_obj.state = BufferState.FLUSHING

            data = buffer_obj.data_queue.copy()
            buffer_obj.data_queue.clear()
            buffer_obj.current_size = 0
            buffer_obj.state = BufferState.EMPTY

            logger.info(f"Buffer stage {stage} flushed: {len(data)} entries")
            return data

    def get_buffer_status(self) -> Dict[str, Any]:
        """Get buffer status"""
        with self.buffer_lock:
            return {
                "stage1": {
                    "id": self.stage1_buffer.id,
                    "state": self.stage1_buffer.state.value,
                    "current_size": self.stage1_buffer.current_size,
                    "capacity": self.stage1_buffer.capacity,
                    "utilization": f"{(self.stage1_buffer.current_size / self.stage1_buffer.capacity * 100):.2f}%",
                    "entries": len(self.stage1_buffer.data_queue)
                },
                "stage2": {
                    "id": self.stage2_buffer.id,
                    "state": self.stage2_buffer.state.value,
                    "current_size": self.stage2_buffer.current_size,
                    "capacity": self.stage2_buffer.capacity,
                    "utilization": f"{(self.stage2_buffer.current_size / self.stage2_buffer.capacity * 100):.2f}%",
                    "entries": len(self.stage2_buffer.data_queue)
                }
            }

# ----------------------------
# Sandbox Ring Memory (for Ring to Server B)
# ----------------------------

class SandboxRingMemory:
    """
    Temporal memory file for ring connections between Server A and B
    Stores ring routing information and connection states
    """

    def __init__(self, ring_name: str = "ring-a-b"):
        self.ring_name = ring_name
        self.ring_nodes: Dict[str, Dict[str, Any]] = {}
        self.connection_log: List[Dict[str, Any]] = []
        self.ring_lock = threading.Lock()

    def register_ring_node(self, node_id: str, server: str, box_id: str,
                          memory_box_number: int) -> Dict[str, Any]:
        """Register node in ring"""
        with self.ring_lock:
            self.ring_nodes[node_id] = {
                "node_id": node_id,
                "server": server,
                "box_id": box_id,
                "memory_box_number": memory_box_number,
                "connected": True,
                "last_heartbeat": time.time(),
                "hops": 0
            }

            logger.info(f"Ring node registered: {node_id}")
            return {"message": "Node registered", "node_id": node_id}

    def get_next_ring_node(self, current_node: str) -> Optional[str]:
        """Get next node in ring (circular)"""
        with self.ring_lock:
            nodes = list(self.ring_nodes.keys())
            if not nodes:
                return None

            current_index = nodes.index(current_node) if current_node in nodes else -1
            next_index = (current_index + 1) % len(nodes)
            return nodes[next_index]

    def log_connection(self, source_node: str, dest_node: str, 
                      data: Any, hops: int = 1) -> str:
        """Log connection/data transfer"""
        log_id = str(uuid.uuid4())[:12]

        with self.ring_lock:
            log_entry = {
                "log_id": log_id,
                "timestamp": time.time(),
                "source": source_node,
                "destination": dest_node,
                "hops": hops,
                "data_hash": hashlib.md5(json.dumps(data, default=str).encode()).hexdigest(),
                "status": "transmitted"
            }
            self.connection_log.append(log_entry)

        return log_id

    def get_ring_topology(self) -> Dict[str, Any]:
        """Get ring topology visualization"""
        with self.ring_lock:
            return {
                "ring_name": self.ring_name,
                "total_nodes": len(self.ring_nodes),
                "nodes": list(self.ring_nodes.keys()),
                "connections": len(self.connection_log)
            }

# ----------------------------
# Server A - Main Architecture
# ----------------------------

class ServerA:
    """
    Server A: Sandbox System with Complete Architecture
    3 Memory Boxes × 9 Ports × 5 Jobs × 10 Subprocesses
    = 1,350 subprocesses total
    """

    def __init__(self, server_id: str = "server-a-001"):
        self.server_id = server_id
        self.server_type = "A"

        # 3 Memory Boxes (9 ports each)
        self.memory_boxes: Dict[str, MemoryBox] = {}
        self.jobs_total = 0
        self.subprocesses_total = 0

        # Systems
        self.cache_system = CacheSystem()
        self.memory_system = MemorySystem()
        self.network_buffer_system = NetworkBufferSystem()
        self.sandbox_ring_memory = SandboxRingMemory()

        # Statistics
        self.stats_lock = threading.Lock()
        self.total_jobs_executed = 0
        self.total_subprocesses_executed = 0

        self._initialize_architecture()

    def _initialize_architecture(self):
        """Initialize 3 memory boxes with complete hierarchy"""
        for box_num in range(3):
            box_id = f"mbox-{box_num}"
            box = MemoryBox(
                id=box_id,
                box_number=box_num,
                server=self.server_type,
                status="initialized"
            )

            # 9 ports per box
            for port_num in range(9):
                port_id = f"port-{box_num}-{port_num}"
                port = Port(
                    id=port_id,
                    port_number=8000 + (box_num * 9) + port_num,
                    memory_box_id=box_id,
                    status="idle"
                )

                # 5 jobs per port
                for job_num in range(5):
                    job_id = f"job-{port_num}-{job_num}"
                    job = Job(
                        id=job_id,
                        port_id=port_id,
                        status=JobState.IDLE
                    )

                    # 10 subprocesses per job
                    for sub_num in range(10):
                        sub_id = f"sub-{job_num}-{sub_num}"
                        subprocess = Subprocess(
                            id=sub_id,
                            job_id=job_id,
                            status=JobState.IDLE,
                            created_at=time.time()
                        )
                        job.subprocesses[sub_id] = subprocess
                        self.subprocesses_total += 1

               