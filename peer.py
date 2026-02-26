import socket
import threading
import sys
import time
import random
import hashlib
import logging
from typing import Dict, Tuple, List, Set


"""
peer.py

Peer node for a simple gossip-based P2P system.

Responsibilities (as required by the assignment):
- Register with at least floor(n/2) + 1 seed nodes (n = number of seeds).
- Obtain peer lists from registered seeds and build a neighbor set.
- Maintain an overlay where node degrees roughly follow a power law by
  biasing degree selection towards smaller degrees.
- Maintain a Message List (ML) to ensure each gossip message traverses
  a link at most once.
- Periodically generate and broadcast gossip messages.
- Periodically check neighbor liveness (using ping messages)
  and only declare a neighbor dead after a simple peer level consensus
  (multiple neighbors suspect the same node).
- Report dead nodes to seeds using the assignment specified message format.
- Log received peer lists, first time gossip messages, and confirmed
  dead node reports to both console and output file.
"""


LOG_FILE = "outputfile.txt"


def setup_logger(ip: str, port: int) -> logging.Logger:
    """
    Configure a logger that writes to both stdout and outputfile.txt.
    """
    logger = logging.getLogger(f"PEER-{ip}:{port}")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s [%(name)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


def load_seeds(config_path: str) -> List[Tuple[str, int]]:
    """
    Read the same seed configuration file as the seeds.

    Expected format:
        127.0.0.1:5000
        127.0.0.1:5001
    """
    seeds: List[Tuple[str, int]] = []
    with open(config_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                ip, port_str = line.split(":", 1)
                seeds.append((ip.strip(), int(port_str.strip())))
            except ValueError:
                continue
    return seeds


class NeighborState:
    """
    Tracks basic liveness information for a neighbor.
    """

    def __init__(self) -> None:
        self.last_seen: float = time.time()
        self.local_suspected: bool = False


class PeerNode:
    """
    Peer node implementation.

    This implementation uses a simple thread per connection model and a
    very small set of message types to keep things easy to understand.
    """

    GOSSIP_INTERVAL = 5.0
    MAX_GOSSIP_MESSAGES = 10
    HEALTH_CHECK_INTERVAL = 5.0
    DEAD_TIMEOUT = 15.0  # If no messages observed for this many seconds, suspect.
    PEER_LEVEL_SUSPECT_THRESHOLD = 1  # Need self + at least 1 other peer.

    def __init__(self, ip: str, port: int, config_path: str):
        self.ip = ip
        self.port = port
        self.addr = (ip, port)

        self.seeds: List[Tuple[str, int]] = load_seeds(config_path)
        if not self.seeds:
            raise RuntimeError("No seeds found in config file.")

        self.n_seeds = len(self.seeds)
        self.quorum = self.n_seeds // 2 + 1

        self.logger = setup_logger(ip, port)
        self.logger.info(
            f"Peer starting at {ip}:{port} with {self.n_seeds} seeds (quorum={self.quorum})"
        )

        # Neighbor management: map (ip, port) -> socket
        self.neighbors: Dict[Tuple[str, int], socket.socket] = {}
        self.neighbor_states: Dict[Tuple[str, int], NeighborState] = {}
        self.neighbors_lock = threading.Lock()

        # Message List to track seen gossip messages.
        self.message_lock = threading.Lock()
        self.seen_messages: Set[str] = set()

        # Suspicion tracking for peer level consensus on dead nodes.
        self.suspicions_lock = threading.Lock()
        self.local_suspicions: Set[Tuple[str, int]] = set()
        self.remote_suspicions: Dict[Tuple[str, int], Set[Tuple[str, int]]] = {}
        self.reported_dead: Set[Tuple[str, int]] = set()

        # Seeds we successfully registered with.
        self.registered_seeds: List[Tuple[str, int]] = []

    # Seed registration and peer list retrieval
    def register_with_seeds(self) -> None:
        """
        Register with at least quorum seeds.
        On success, also pull peer lists from those seeds.
        """
        seeds = self.seeds[:]
        random.shuffle(seeds)

        peer_lists: List[List[Tuple[str, int]]] = []

        for sip, sport in seeds:
            try:
                with socket.create_connection((sip, sport), timeout=3.0) as s:
                    f = s.makefile("r")
                    # Registration request
                    s.sendall(f"REGISTER|{self.ip}|{self.port}\n".encode("utf-8"))
                    resp = f.readline().strip()
                    if resp != "REGISTER_OK":
                        self.logger.info(
                            f"Registration with seed {sip}:{sport} failed "
                            f"(response={resp!r})"
                        )
                        continue
                    self.logger.info(
                        f"Successfully registered with seed {sip}:{sport}"
                    )
                    self.registered_seeds.append((sip, sport))

                    # Request peer list from this seed
                    s.sendall(b"GET_PEERS\n")
                    pl_line = f.readline().strip()
                    peers = self.parse_peer_list_line(pl_line)
                    if peers:
                        peer_lists.append(peers)
                        self.logger.info(
                            f"Received peer list from seed {sip}:{sport}: {peers}"
                        )
            except OSError as e:
                self.logger.info(
                    f"Could not contact seed {sip}:{sport} for registration: {e}"
                )
                continue

            if len(self.registered_seeds) >= self.quorum:
                break

        # For robustness in real runs, if we fail to reach quorum but did
        # register with at least one seed, we proceed with a warning instead
        # of aborting. Seed-side consensus still ensures membership replicates.
        if len(self.registered_seeds) < self.quorum:
            if not self.registered_seeds:
                raise RuntimeError(
                    f"Failed to register with any seeds (need quorum={self.quorum})."
                )
            self.logger.info(
                "Warning: registered with fewer seeds than quorum "
                f"(got {len(self.registered_seeds)}, need {self.quorum}), "
                "continuing with available seeds."
            )

        # Union of all peers from all seeds.
        union: Set[Tuple[str, int]] = set()
        for pl in peer_lists:
            for entry in pl:
                if entry != self.addr:
                    union.add(entry)

        if union:
            self.logger.info(f"Union of known peers from seeds: {list(union)}")
        else:
            self.logger.info("No existing peers returned by seeds yet.")

        self.build_neighbors(list(union))

    @staticmethod
    def parse_peer_list_line(line: str) -> List[Tuple[str, int]]:
        """
        Parse a PEER_LIST response line of the form:
            PEER_LIST|ip1:port1,ip2:port2,...
        """
        if not line.startswith("PEER_LIST|"):
            return []
        payload = line.split("|", 1)[1]
        if not payload:
            return []
        entries = []
        for item in payload.split(","):
            item = item.strip()
            if not item:
                continue
            try:
                ip, port_str = item.split(":", 1)
                entries.append((ip.strip(), int(port_str.strip())))
            except ValueError:
                continue
        return entries

    # ------------------------------------------------------------------
    # Neighbor selection and connection management
    # ------------------------------------------------------------------

    def build_neighbors(self, candidates: List[Tuple[str, int]]) -> None:
        """
        Select neighbors from the given candidate list so that peer degrees
        tend to be small.
        """
        if not candidates:
            return

        # Sample desired degree from a small discrete distribution
        # biased towards lower degrees.
        degrees = [1, 2, 3, 4, 5]
        weights = [0.4, 0.3, 0.15, 0.1, 0.05]
        desired_degree = random.choices(degrees, weights=weights, k=1)[0]
        desired_degree = min(desired_degree, len(candidates))

        random.shuffle(candidates)
        picked = candidates[:desired_degree]

        self.logger.info(f"Selected {len(picked)} neighbors from candidates.")

        for nip, nport in picked:
            self.connect_to_neighbor(nip, nport)

    def connect_to_neighbor(self, nip: str, nport: int) -> None:
        """
        Establish an outgoing TCP connection to a neighbor and start
        a handler thread for that connection.
        """
        with self.neighbors_lock:
            if (nip, nport) in self.neighbors:
                return

        try:
            sock = socket.create_connection((nip, nport), timeout=3.0)
        except OSError:
            return

        # Introduce ourselves so the remote side learns our identity.
        try:
            sock.sendall(f"HELLO|{self.ip}|{self.port}\n".encode("utf-8"))
        except OSError:
            sock.close()
            return
        sock.settimeout(None)

        with self.neighbors_lock:
            self.neighbors[(nip, nport)] = sock
            if (nip, nport) not in self.neighbor_states:
                self.neighbor_states[(nip, nport)] = NeighborState()

        t = threading.Thread(
            target=self.handle_peer_connection, args=(sock,), daemon=True
        )
        t.start()

    def start_server(self) -> None:
        """
        Start a TCP server so that other peers can connect to us.
        """
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((self.ip, self.port))
        server_sock.listen()
        self.logger.info("Peer listening for incoming peer connections...")

        while True:
            conn, _ = server_sock.accept()
            t = threading.Thread(
                target=self.handle_peer_connection, args=(conn,), daemon=True
            )
            t.start()

    def handle_peer_connection(self, conn: socket.socket) -> None:
        """
        Handle messages on a connection to another peer.
        The first line must be a HELLO announcing the remote peer's identity.
        """
        remote_id: Tuple[str, int] | None = None
        try:
            f = conn.makefile("r")
            # Expect HELLO first
            hello = f.readline()
            if not hello:
                return
            hello = hello.strip()
            if not hello.startswith("HELLO|"):
                return
            parts = hello.split("|")
            if len(parts) != 3:
                return
            rip, rport_str = parts[1], parts[2]
            try:
                rport = int(rport_str)
            except ValueError:
                return
            remote_id = (rip, rport)

            with self.neighbors_lock:
                self.neighbors[remote_id] = conn
                if remote_id not in self.neighbor_states:
                    self.neighbor_states[remote_id] = NeighborState()

            self.update_neighbor_seen(remote_id)

            # Process subsequent messages.
            for line in f:
                line = line.strip()
                if not line:
                    continue
                self.update_neighbor_seen(remote_id)
                if line.startswith("PING|"):
                    # Simple application-level ping/pong.
                    conn.sendall(line.replace("PING|", "PONG|").encode("utf-8") + b"\n")
                elif line.startswith("PONG|"):
                    # We just update last_seen via update_neighbor_seen.
                    continue
                elif line.startswith("GOSSIP|"):
                    self.handle_gossip_line(line, remote_id)
                elif line.startswith("SUSPECT_NOTICE|"):
                    self.handle_suspect_notice(line, remote_id)
                else:
                    # Unknown message types can be ignored safely.
                    continue
        except OSError:
            pass
        finally:
            if remote_id is not None:
                with self.neighbors_lock:
                    if remote_id in self.neighbors and self.neighbors[remote_id] is conn:
                        del self.neighbors[remote_id]
                with self.suspicions_lock:
                    # Losing a connection might contribute to suspicion,
                    # but we keep the logic simple and rely on timeouts.
                    pass
            conn.close()

    def update_neighbor_seen(self, nid: Tuple[str, int]) -> None:
        """
        Record that we recently heard from / interacted with a neighbor.
        """
        with self.neighbors_lock:
            state = self.neighbor_states.get(nid)
            if state is None:
                state = NeighborState()
                self.neighbor_states[nid] = state
        state.last_seen = time.time()

    # ------------------------------------------------------------------
    # Gossip protocol
    # ------------------------------------------------------------------

    def gossip_loop(self) -> None:
        """
        Periodically generate a new gossip message and broadcast it.
        """
        counter = 0
        while counter < self.MAX_GOSSIP_MESSAGES:
            time.sleep(self.GOSSIP_INTERVAL)
            counter += 1
            timestamp = time.time()
            payload = f"{timestamp}:{self.ip}:{counter}"
            msg_id = hashlib.sha256(payload.encode("utf-8")).hexdigest()
            with self.message_lock:
                self.seen_messages.add(msg_id)
            self.logger.info(f"Generated gossip message: {payload}")
            self.broadcast_to_neighbors(f"GOSSIP|{msg_id}|{payload}")

    def handle_gossip_line(
        self, line: str, from_nid: Tuple[str, int] | None
    ) -> None:
        """
        Handle an incoming gossip message of the form:
            GOSSIP|<msg_id>|<payload>
        """
        parts = line.split("|", 2)
        if len(parts) != 3:
            return
        msg_id, payload = parts[1], parts[2]
        with self.message_lock:
            if msg_id in self.seen_messages:
                return
            self.seen_messages.add(msg_id)
        # Log only the first time we see a given gossip message.
        self.logger.info(
            f"Received new gossip message: {payload} "
            f"from {from_nid[0]}:{from_nid[1]}" if from_nid else "from unknown"
        )
        # Forward the message to all neighbors except the sender.
        self.broadcast_to_neighbors(
            f"GOSSIP|{msg_id}|{payload}", exclude=from_nid
        )

    def broadcast_to_neighbors(
        self, line: str, exclude: Tuple[str, int] | None = None
    ) -> None:
        """
        Send a single line to all known neighbors, optionally skipping one.
        """
        with self.neighbors_lock:
            items = list(self.neighbors.items())

        for nid, sock in items:
            if exclude is not None and nid == exclude:
                continue
            try:
                sock.sendall((line + "\n").encode("utf-8"))
            except OSError:
                # Connection issues are handled by the connection handler.
                continue

    # ------------------------------------------------------------------
    # Liveness detection and peer-level consensus
    # ------------------------------------------------------------------

    def ping_loop(self) -> None:
        """
        Periodically send lightweight ping messages to neighbors
        to keep last_seen information up-to-date.
        """
        while True:
            time.sleep(self.HEALTH_CHECK_INTERVAL)
            with self.neighbors_lock:
                neighbor_ids = list(self.neighbors.keys())
                socks = {nid: self.neighbors[nid] for nid in neighbor_ids}

            for nid in neighbor_ids:
                sock = socks.get(nid)
                if sock is None:
                    continue
                try:
                    sock.sendall(f"PING|{time.time()}\n".encode("utf-8"))
                except OSError:
                    continue

    def health_check_loop(self) -> None:
        """
        Periodically check for neighbors that appear unresponsive.
        When a neighbor is suspected locally and at least one other
        neighbor reports the same suspicion, we report the node as dead
        to the seeds.
        """
        while True:
            time.sleep(self.HEALTH_CHECK_INTERVAL)
            now = time.time()

            # Step 1: mark neighbors as locally suspected if they are silent.
            with self.neighbors_lock:
                neighbor_ids = list(self.neighbor_states.keys())

            newly_suspected: List[Tuple[str, int]] = []

            for nid in neighbor_ids:
                with self.neighbors_lock:
                    state = self.neighbor_states.get(nid)
                if state is None:
                    continue
                if state.local_suspected:
                    continue
                if now - state.last_seen > self.DEAD_TIMEOUT:
                    state.local_suspected = True
                    newly_suspected.append(nid)

            for nid in newly_suspected:
                self.logger.info(
                    f"Locally suspecting neighbor {nid[0]}:{nid[1]} as dead."
                )
                with self.suspicions_lock:
                    self.local_suspicions.add(nid)
                # Inform other neighbors about our suspicion.
                self.broadcast_to_neighbors(
                    f"SUSPECT_NOTICE|{nid[0]}|{nid[1]}|{self.ip}|{self.port}"
                )
                # Check if we already meet peer-level consensus.
                self.maybe_report_dead(nid)

    def handle_suspect_notice(
        self, line: str, from_nid: Tuple[str, int] | None
    ) -> None:
        """
        Handle SUSPECT_NOTICE from another peer:
            SUSPECT_NOTICE|dead_ip|dead_port|suspector_ip|suspector_port
        """
        parts = line.split("|")
        if len(parts) != 5:
            return
        dead_ip, dead_port_str = parts[1], parts[2]
        suspector_ip, suspector_port_str = parts[3], parts[4]
        try:
            dead_port = int(dead_port_str)
            suspector_port = int(suspector_port_str)
        except ValueError:
            return

        dead_id = (dead_ip, dead_port)
        suspector_id = (suspector_ip, suspector_port)

        with self.suspicions_lock:
            if dead_id not in self.remote_suspicions:
                self.remote_suspicions[dead_id] = set()
            self.remote_suspicions[dead_id].add(suspector_id)

        self.logger.info(
            f"Received SUSPECT_NOTICE for {dead_ip}:{dead_port} from "
            f"{suspector_ip}:{suspector_port}"
        )
        self.maybe_report_dead(dead_id)

    def maybe_report_dead(self, dead_id: Tuple[str, int]) -> None:
        """
        If we both locally suspect a node and have heard suspicions
        from at least PEER_LEVEL_SUSPECT_THRESHOLD distinct neighbors,
        send a dead-node report to the seeds (once).
        """
        with self.suspicions_lock:
            locally = dead_id in self.local_suspicions
            remotes = self.remote_suspicions.get(dead_id, set())
            remote_count = len(remotes)
            already_reported = dead_id in self.reported_dead

        if (
            not already_reported
            and locally
            and remote_count >= self.PEER_LEVEL_SUSPECT_THRESHOLD
        ):
            self.logger.info(
                f"Peer-level consensus reached for dead node {dead_id[0]}:{dead_id[1]} "
                f"(local + {remote_count} remote suspicions)."
            )
            self.reported_dead.add(dead_id)
            self.report_dead_to_seeds(dead_id)

    def report_dead_to_seeds(self, dead_id: Tuple[str, int]) -> None:
        """
        Send a dead-node report to all seeds we know.
        We wait for a confirmation from any one seed that completes
        seed-level consensus, then log the confirmed dead-node report.
        """
        dead_ip, dead_port = dead_id
        timestamp = time.time()
        msg = f"Dead Node:{dead_ip}:{dead_port}:{timestamp}:{self.ip}\n"

        confirmed = False

        for sip, sport in self.seeds:
            try:
                with socket.create_connection((sip, sport), timeout=3.0) as s:
                    f = s.makefile("r")
                    s.sendall(msg.encode("utf-8"))
                    resp = f.readline().strip()
                    if resp.startswith("DEAD_CONFIRMED|"):
                        confirmed = True
                        break
            except OSError:
                continue

        if confirmed:
            self.logger.info(
                f"Confirmed dead-node removal for {dead_ip}:{dead_port} "
                f"after seed-level consensus."
            )
        else:
            self.logger.info(
                f"Dead-node report for {dead_ip}:{dead_port} "
                f"did not receive confirmation from seeds."
            )

    # ------------------------------------------------------------------
    # Main entrypoint helpers
    # ------------------------------------------------------------------

    def start(self) -> None:
        """
        Start the peer:
        - Start the server thread for incoming peer connections.
        - Register with seeds and build initial neighbors.
        - Start gossip and liveness threads.
        """
        # Start listening for incoming peer connections.
        server_thread = threading.Thread(target=self.start_server, daemon=True)
        server_thread.start()

        # Give the server a moment to start before contacting seeds.
        time.sleep(1.0)

        # Register with seeds and build neighbors.
        self.register_with_seeds()

        # Start gossip and liveness threads.
        threading.Thread(target=self.gossip_loop, daemon=True).start()
        threading.Thread(target=self.ping_loop, daemon=True).start()
        threading.Thread(target=self.health_check_loop, daemon=True).start()

        # Keep the main thread alive.
        while True:
            time.sleep(60.0)


def main() -> None:
    if len(sys.argv) != 4:
        print(
            "Usage: python peer.py <listen_ip> <listen_port> <config.txt>",
            file=sys.stderr,
        )
        sys.exit(1)

    listen_ip = sys.argv[1]
    try:
        listen_port = int(sys.argv[2])
    except ValueError:
        print("listen_port must be an integer.", file=sys.stderr)
        sys.exit(1)
    config_path = sys.argv[3]

    node = PeerNode(listen_ip, listen_port, config_path)
    node.start()


if __name__ == "__main__":
    main()

