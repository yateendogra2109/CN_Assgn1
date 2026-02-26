CSL3080 Assignment 1 – Gossip-Based P2P Network (Python)
By ->  Yateen(B23CS1082)
  |->  Shlok Kanani(B23CS1068)
========================================================

This submission implements a simple gossip-based peer-to-peer (P2P) system with:

- Seed nodes (`seed.py`) that manage membership and run consensus.
- Peer nodes (`peer.py`) that join the network, gossip messages, and detect failures.




Files
-----

- `seed.py` – Seed node implementation.
- `peer.py` – Peer node implementation.
- `config.txt` – Seed configuration file (IP:port pairs).
- `outputfile.txt` – Shared log file (created at runtime).


Python Version and Dependencies
-------------------------------

- Tested with **Python 3.10+**.
- Uses only the Python standard library (`socket`, `threading`, `time`, `logging`, etc.).
- No external packages are required.


Configuration (`config.txt`)
----------------------------

The same `config.txt` is used by both seeds and peers.

Format (one seed per line, comments allowed):

    # Example with three seed nodes on localhost
    127.0.0.1:5000
    127.0.0.1:5001
    127.0.0.1:5002


How to Run Seed Nodes
---------------------

1. Open **three terminals** (or more, but minimum 2 seeds required by the assignment).
2. In each terminal, change directory to the project folder:

    `cd <path-to-project-folder>`

3. Start each seed with:

    `python seed.py <listen_ip> <listen_port> config.txt`

   Examples (matching the default `config.txt`):

   - Terminal 1:

         python seed.py 127.0.0.1 5000 config.txt

   - Terminal 2:

         python seed.py 127.0.0.1 5001 config.txt

   - Terminal 3:

         python seed.py 127.0.0.1 5002 config.txt

4. Each seed:

   - Reads the list of all seeds from `config.txt`.
   - Computes `n` and the quorum `⌊n/2⌋ + 1`.
   - Listens for connections from peers and from other seeds.
   - Logs registration proposals, consensus outcomes, and confirmed
     dead-node removals to **both** the console and `outputfile.txt`.


How to Run Peer Nodes
---------------------

1. Make sure the seeds are already running.
2. For each peer, open a **new terminal** and run:

    python peer.py <listen_ip> <listen_port> config.txt

   Example peers on the same machine:

   - Peer A:

         python peer.py 127.0.0.1 6000 config.txt

   - Peer B:

         python peer.py 127.0.0.1 6001 config.txt

   - Peer C:

         python peer.py 127.0.0.1 6002 config.txt

3. Each peer:

   - Starts a TCP server to receive connections from other peers.
   - Reads the same seed list from `config.txt`.
   - Registers with randomly chosen seeds until it reaches a majority
     (`⌊n/2⌋ + 1` seeds). Seeds use a majority-vote consensus before
     adding the peer to their Peer Lists (PLs).
   - Requests peer lists from registered seeds, merges them, and chooses
     neighbors with a **small, biased degree** (approximate power-law).
   - Establishes TCP connections (`HELLO` handshake) with the chosen neighbors.
   - Generates gossip messages every 5 seconds (up to 10 messages), with
     payloads of the form:

         <self.timestamp>:<self.IP>:<self.Msg#>

   - Maintains a Message List (ML) using SHA-256 hashes to ensure each
     gossip message is forwarded over each link at most once.
   - Logs:
     - Received peer lists (first time).
     - Every **new** gossip message it sees for the first time (with
       timestamp and sender).
     - Confirmed dead-node reports after seed-level consensus.


Liveness Detection and Dead-Node Reporting

- Each peer periodically sends lightweight `PING` messages to neighbors
  and records when it last heard from them.
- If a neighbor is silent for a configurable timeout (default 15 seconds),
  the peer **locally suspects** the neighbor is dead and:
  - Marks the neighbor as suspected.
  - Broadcasts a `SUSPECT_NOTICE` to other neighbors.
- When a peer both:
  - Locally suspects a neighbor, **and**
  - Has received at least one `SUSPECT_NOTICE` from other peers for
    the same neighbor,
  it treats this as **peer-level consensus** that the node is dead.

At that point, the peer sends the assignment-specified message to seeds:

    Dead Node:<DeadNode.IP>:<DeadNode.Port>:<self.timestamp>:<self.IP>

The seeds, in turn, run a majority-vote consensus among themselves and
only remove the peer from their Peer Lists after reaching agreement.
Once a seed-level consensus is reached, the reporting peer receives a
`DEAD_CONFIRMED` response and logs the **confirmed dead-node report**.


Logging and `outputfile.txt`
----------------------------

- Both `seed.py` and `peer.py` write logs to:
  - The terminal (console).
  - A shared file named `outputfile.txt` in the project directory.
- Logs include:
  - For seeds:
    - Registration proposals and consensus results.
    - JOIN/DEAD commit applications from other seeds.
    - Confirmed dead-node removals.
  - For peers:
    - Seed registration results and peer lists.
    - First-time gossip messages (with sender info).
    - Suspicions and peer-level consensus decisions.
    - Confirmed dead-node removals (after seed-level consensus).


Important Simplifying Assumptions
---------------------------------

- **Consensus**:
  - Seeds always vote "OK" on proposals but still require a **majority of
    reachable seeds** to complete a commit, preventing unilateral changes.
  - Peer-level consensus uses a simple rule: a node must be locally
    suspected **and** suspected by at least one other neighbor.
- **Ping implementation**: Liveness checks use application-level `PING`/`PONG`
  messages over existing TCP connections rather than OS-level `ping`,
  which keeps the implementation portable but still matches the spirit
  of periodic liveness checks.


How to Test Quickly
-------------------

1. Start three seeds as described above.
2. Start two or three peers.
3. Watch:
   - Seeds log registrations and consensus decisions.
   - Peers log received peer lists and gossip messages.
4. To simulate a peer failure, stop one peer process and wait for other
   peers to:
   - Suspect the stopped peer as dead.
   - Reach peer-level consensus.
   - Report the dead peer to seeds and log the confirmed removal.






