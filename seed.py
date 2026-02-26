import socket
import threading
import sys
import time
import logging
from typing import List, Tuple, Set


"""
seed.py

Seed node for a simple gossip-based P2P system.

Responsibilities (as required by the assignment):
- Maintain a Peer List (PL) of known peers (IP:port).
- Support consensus-based peer registration with other seeds.
- Support consensus-based peer removal (on dead-node reports from peers).
- Log proposals, consensus outcomes, and confirmed removals
  to both the console and an output file.

Design notes (kept intentionally simple for a teaching-oriented implementation):
- All seeds share the same config file listing seed IP:port pairs (one per line).
- Consensus is implemented as a best-effort majority vote using
  one-round "propose/vote/commit" messages between seeds.
- For simplicity, every seed always votes "OK" on proposals; the
  majority condition still prevents unilateral changes because a
  single seed cannot commit unless it can contact a majority.
"""


LOG_FILE = "outputfile.txt"


def setup_logger(role: str, ip: str, port: int) -> logging.Logger:
    """
    Configure a logger that writes to both stdout and outputfile.txt.
    """
    logger = logging.getLogger(f"{role}-{ip}:{port}")
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
    Read the seed configuration file.

    Expected format (one entry per line, comments allowed):
        # This is a comment
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
                # Ignore malformed lines to keep the parser forgiving.
                continue
    return seeds


class SeedNode:
    """
    Seed node implementation with simple majority-based consensus.
    """

    def __init__(self, ip: str, port: int, config_path: str):
        self.ip = ip
        self.port = port
        self.addr = (ip, port)
        self.seeds: List[Tuple[str, int]] = load_seeds(config_path)
        if not self.seeds:
            raise RuntimeError("No seeds found in config file.")

        self.n = len(self.seeds)
        self.quorum = self.n // 2 + 1

        self.peer_list: Set[Tuple[str, int]] = set()
        self.peer_list_lock = threading.Lock()

        self.logger = setup_logger("SEED", ip, port)
        self.logger.info(
            f"Seed starting at {ip}:{port} with {self.n} seeds (quorum={self.quorum})"
        )

    # ------------------------------------------------------------------
    # Networking helpers
    # ------------------------------------------------------------------

    def start(self) -> None:
        """
        Start the TCP server that listens for peers and other seeds.
        """
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((self.ip, self.port))
        server_sock.listen()
        self.logger.info("Seed listening for connections...")

        while True:
            conn, addr = server_sock.accept()
            t = threading.Thread(
                target=self.handle_connection, args=(conn, addr), daemon=True
            )
            t.start()

    def handle_connection(self, conn: socket.socket, addr) -> None:
        """
        Handle a TCP connection from a peer or another seed.
        Messages are line-based and parsed by a simple prefix.
        """
        try:
            file_obj = conn.makefile("r")
            for line in file_obj:
                line = line.strip()
                if not line:
                    continue
                # Peer registration
                if line.startswith("REGISTER|"):
                    parts = line.split("|")
                    if len(parts) == 3:
                        peer_ip = parts[1]
                        try:
                            peer_port = int(parts[2])
                        except ValueError:
                            conn.sendall(b"REGISTER_FAIL\n")
                            continue
                        self.handle_peer_register(conn, peer_ip, peer_port)
                    else:
                        conn.sendall(b"REGISTER_FAIL\n")
                # Peer list request
                elif line == "GET_PEERS":
                    self.handle_get_peers(conn)
                # Seed-to-seed join consensus messages
                elif line.startswith("JOIN_PROPOSE|"):
                    self.handle_join_propose(conn, line)
                elif line.startswith("JOIN_COMMIT|"):
                    self.handle_join_commit(line)
                # Seed-to-seed dead-node consensus messages
                elif line.startswith("DEAD_PROPOSE|"):
                    self.handle_dead_propose(conn, line)
                elif line.startswith("DEAD_COMMIT|"):
                    self.handle_dead_commit(line)
                # Dead-node report from a peer (assignment-specified format)
                elif line.startswith("Dead Node:"):
                    self.handle_dead_report(conn, line)
                else:
                    # Unknown message type is ignored; this keeps protocol simple.
                    continue
        except OSError:
            pass
        finally:
            conn.close()

    def send_one_line(self, ip: str, port: int, line: str, timeout: float = 3.0) -> str:
        """
        Utility: open a short-lived TCP connection, send a single line,
        and optionally read a single response line.
        """
        with socket.create_connection((ip, port), timeout=timeout) as s:
            s.sendall((line + "\n").encode("utf-8"))
            f = s.makefile("r")
            resp = f.readline()
            return resp.strip() if resp else ""

    # ------------------------------------------------------------------
    # Peer registration and consensus
    # ------------------------------------------------------------------

    def handle_peer_register(
        self, conn: socket.socket, peer_ip: str, peer_port: int
    ) -> None:
        """
        Handle REGISTER from a peer.
        If the peer is already known, immediately acknowledge.
        Otherwise, run a simple majority-vote consensus among seeds.
        """
        self.logger.info(
            f"Received registration proposal for peer {peer_ip}:{peer_port}"
        )

        with self.peer_list_lock:
            already_known = (peer_ip, peer_port) in self.peer_list

        if already_known:
            conn.sendall(b"REGISTER_OK\n")
            return

        success = self.run_join_consensus(peer_ip, peer_port)
        if success:
            conn.sendall(b"REGISTER_OK\n")
        else:
            conn.sendall(b"REGISTER_FAIL\n")

    def run_join_consensus(self, peer_ip: str, peer_port: int) -> bool:
        """
        Start a majority-vote consensus among seeds to add a new peer.
        This seed acts as the proposer.
        """
        # If already in peer list, no need to re-run consensus.
        with self.peer_list_lock:
            if (peer_ip, peer_port) in self.peer_list:
                return True

        votes = 1  # this seed implicitly votes "OK"

        for sip, sport in self.seeds:
            if (sip, sport) == self.addr:
                continue
            try:
                resp = self.send_one_line(
                    sip, sport, f"JOIN_PROPOSE|{peer_ip}|{peer_port}"
                )
                if resp.startswith("JOIN_VOTE|"):
                    parts = resp.split("|")
                    if len(parts) == 4 and parts[3] == "OK":
                        votes += 1
            except OSError:
                # If a seed is unreachable, we simply skip its vote.
                continue

        if votes >= self.quorum:
            with self.peer_list_lock:
                if (peer_ip, peer_port) not in self.peer_list:
                    self.peer_list.add((peer_ip, peer_port))
            # Broadcast commit so other seeds update their Peer Lists.
            for sip, sport in self.seeds:
                if (sip, sport) == self.addr:
                    continue
                try:
                    self.send_one_line(
                        sip, sport, f"JOIN_COMMIT|{peer_ip}|{peer_port}"
                    )
                except OSError:
                    continue

            self.logger.info(
                f"Consensus SUCCESS for registration of {peer_ip}:{peer_port} "
                f"(votes={votes}/{self.n})"
            )
            return True

        self.logger.info(
            f"Consensus FAILED for registration of {peer_ip}:{peer_port} "
            f"(votes={votes}/{self.n})"
        )
        return False

    def handle_join_propose(self, conn: socket.socket, line: str) -> None:
        """
        Handle JOIN_PROPOSE from another seed.
        This implementation always votes 'OK' but could be extended
        to perform local checks before voting.
        """
        parts = line.split("|")
        if len(parts) != 3:
            return
        peer_ip, peer_port_str = parts[1], parts[2]
        try:
            peer_port = int(peer_port_str)
        except ValueError:
            return
        # Always vote OK in this simple design.
        vote_line = f"JOIN_VOTE|{peer_ip}|{peer_port}|OK\n"
        conn.sendall(vote_line.encode("utf-8"))

    def handle_join_commit(self, line: str) -> None:
        """
        Handle JOIN_COMMIT from another seed and update local PL.
        """
        parts = line.split("|")
        if len(parts) != 3:
            return
        peer_ip, peer_port_str = parts[1], parts[2]
        try:
            peer_port = int(peer_port_str)
        except ValueError:
            return
        with self.peer_list_lock:
            if (peer_ip, peer_port) not in self.peer_list:
                self.peer_list.add((peer_ip, peer_port))
        self.logger.info(f"Applied JOIN_COMMIT for peer {peer_ip}:{peer_port}")

    def handle_get_peers(self, conn: socket.socket) -> None:
        """
        Send the current Peer List to a requesting peer.
        """
        with self.peer_list_lock:
            payload = ",".join(f"{ip}:{port}" for ip, port in self.peer_list)
        resp = f"PEER_LIST|{payload}\n"
        conn.sendall(resp.encode("utf-8"))

    # ------------------------------------------------------------------
    # Dead-node reporting and removal consensus
    # ------------------------------------------------------------------

    def handle_dead_report(self, conn: socket.socket, line: str) -> None:
        """
        Handle a dead-node report from a peer.
        Incoming format (per assignment):
            Dead Node:<DeadNode.IP>:<DeadNode.Port>:<timestamp>:<reporter.IP>
        """
        # Strip the initial "Dead Node:" and then split on ":".
        try:
            _, rest = line.split("Dead Node:", 1)
            parts = rest.split(":")
            if len(parts) < 4:
                return
            dead_ip = parts[0]
            dead_port = int(parts[1])
            timestamp = parts[2]
            reporter_ip = parts[3]
        except Exception:
            return

        self.logger.info(
            f"Received dead-node report for {dead_ip}:{dead_port} "
            f"from {reporter_ip} at {timestamp}"
        )

        success = self.run_dead_consensus(dead_ip, dead_port)
        if success:
            # Inform the reporting peer that removal was confirmed.
            resp = f"DEAD_CONFIRMED|{dead_ip}|{dead_port}\n"
        else:
            resp = f"DEAD_REJECTED|{dead_ip}|{dead_port}\n"
        conn.sendall(resp.encode("utf-8"))

    def run_dead_consensus(self, dead_ip: str, dead_port: int) -> bool:
        """
        Start a majority-vote consensus among seeds to remove a dead peer.
        """
        with self.peer_list_lock:
            if (dead_ip, dead_port) not in self.peer_list:
                # If we no longer track the peer locally, treat it as "already removed".
                return True

        votes = 1  # this seed votes "OK"

        for sip, sport in self.seeds:
            if (sip, sport) == self.addr:
                continue
            try:
                resp = self.send_one_line(
                    sip, sport, f"DEAD_PROPOSE|{dead_ip}|{dead_port}"
                )
                if resp.startswith("DEAD_VOTE|"):
                    parts = resp.split("|")
                    if len(parts) == 4 and parts[3] == "OK":
                        votes += 1
            except OSError:
                continue

        if votes >= self.quorum:
            with self.peer_list_lock:
                if (dead_ip, dead_port) in self.peer_list:
                    self.peer_list.remove((dead_ip, dead_port))

            # Broadcast commit so other seeds remove the peer.
            for sip, sport in self.seeds:
                if (sip, sport) == self.addr:
                    continue
                try:
                    self.send_one_line(
                        sip, sport, f"DEAD_COMMIT|{dead_ip}|{dead_port}"
                    )
                except OSError:
                    continue

            self.logger.info(
                f"Consensus SUCCESS for dead-node removal {dead_ip}:{dead_port} "
                f"(votes={votes}/{self.n})"
            )
            return True

        self.logger.info(
            f"Consensus FAILED for dead-node removal {dead_ip}:{dead_port} "
            f"(votes={votes}/{self.n})"
        )
        return False

    def handle_dead_propose(self, conn: socket.socket, line: str) -> None:
        """
        Handle DEAD_PROPOSE from another seed.
        This seed always votes 'OK' but only removes the peer on DEAD_COMMIT.
        """
        parts = line.split("|")
        if len(parts) != 3:
            return
        dead_ip, dead_port_str = parts[1], parts[2]
        try:
            dead_port = int(dead_port_str)
        except ValueError:
            return
        vote_line = f"DEAD_VOTE|{dead_ip}|{dead_port}|OK\n"
        conn.sendall(vote_line.encode("utf-8"))

    def handle_dead_commit(self, line: str) -> None:
        """
        Handle DEAD_COMMIT from another seed and update local PL.
        """
        parts = line.split("|")
        if len(parts) != 3:
            return
        dead_ip, dead_port_str = parts[1], parts[2]
        try:
            dead_port = int(dead_port_str)
        except ValueError:
            return
        with self.peer_list_lock:
            if (dead_ip, dead_port) in self.peer_list:
                self.peer_list.remove((dead_ip, dead_port))
        self.logger.info(f"Applied DEAD_COMMIT for peer {dead_ip}:{dead_port}")


def main() -> None:
    if len(sys.argv) != 4:
        print(
            "Usage: python seed.py <listen_ip> <listen_port> <config.txt>",
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

    node = SeedNode(listen_ip, listen_port, config_path)
    node.start()


if __name__ == "__main__":
    main()

