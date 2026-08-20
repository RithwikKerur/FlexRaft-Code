from curses import meta

import matplotlib.pyplot as plt
import matplotlib as mpl
import pandas as pd
from collections import defaultdict
from typing import Dict, List, Optional
import random
import math
import numpy as np
import logging
import csv

logging.basicConfig(
    filename='simulation.log',
    filemode='w',  # overwrite each run; use 'a' to append
    level=logging.DEBUG,
    format='%(message)s'
)

# Generate the same set of delays
def generate_entry_delays(num_entries: int, N: int, seed: int = 42):
    rng = random.Random(seed)
    delays = {}
    for entry_id in range(num_entries):
        delays[entry_id] = {}
        for node_id in range(N):
            delays[entry_id][node_id] = max(0.1, rng.gauss(0.8, 0.15))

    return delays

class LogEntry:
    def __init__(self, entry_id: int, tick: float):
        self.id = entry_id
        self.created_tick = tick
        self.phase = "PROBING" 
        self.disseminated_tick = None
        self.committed = False
        self.commit_tick = None
        
    def get_latency(self) -> Optional[float]:
        if self.committed and self.commit_tick is not None:
            return self.commit_tick - self.created_tick
        return None
    
    def get_commit_latency(self) -> Optional[float]:
        if self.committed and self.commit_tick is not None:
            return self.commit_tick - self.disseminated_tick  # pure consensus
        return None

    def get_queuing_latency(self) -> Optional[float]:
        if self.disseminated_tick is not None:
            return self.disseminated_tick - self.created_tick  # time in queue
        return None

class Node:
    def __init__(self, node_id: int, mean_delay, delay_variance):
        self.id = node_id
        self.online = True
        self.delay = mean_delay # How many ticks it takes for this node to acknowledge a shard
        self.delay_variance = delay_variance
        self.storage = {}  
        self.pending_acks = {}  #pending messages from leader to be acknowledged back
    
    #Returns the tick at which the ack will be received by the leader, or -1 if node is offline
    def receive_shard(self, entry_id: int, shard_size: float, tick: int, delay: float) -> int:
        if not self.online:
            return -1
        self.storage[entry_id] = shard_size
        actual_delay = delay
        ack_tick = tick + actual_delay
        self.pending_acks[entry_id] = (shard_size, ack_tick)
        return ack_tick
    
    # For Rafure/I-Raft: update the shard size for an entry based on new optimization calculations
    def update_shard_size(self, entry_id: int, new_size: float):
        # Adjust shard size for an entry (For I-Raft optimization)
        if entry_id in self.storage:
            self.storage[entry_id] = new_size
    def catch_up(self, entry_id: int, shard_size: float):
        self.storage[entry_id] = shard_size
    
    def get_total_storage(self) -> float:
        return sum(self.storage.values())
    
    def set_online(self, online: bool):
        self.online = online
        if not online:
            self.pending_acks.clear()

    #For H-Raft
    def receive_extra_shard(self, entry_id: int, shard_size: float, tick: int, delay: float):
        if not self.online:
            return -1
        self.storage[entry_id] = self.storage.get(entry_id, 0) + shard_size
        actual_delay = delay
        ack_tick = tick + actual_delay
        self.pending_acks[entry_id] = (shard_size, ack_tick)
        return ack_tick


#Leader class sets up the cluster with the set node delays
class Leader:
    def __init__(self, F: int, log_size_kb: float = 4.0, entry_delays: dict = None):
        self.F = F
        self.N = 2 * F + 1
        self.k = F + 1
        self.log_size_kb = log_size_kb
        self.fragment_size = log_size_kb / self.k
        self.entry_delays = entry_delays
        
        self.nodes: Dict[int, Node] = {
            i: Node(i, mean_delay=0.8, delay_variance=0.15) for i in range(self.N)
        }
        self.log_entries: Dict[int, LogEntry] = {}
        
        self.pending_responses = defaultdict(dict) #pending from nodes
        self.received_responses = defaultdict(set)
        self.probe_responses = defaultdict(set) # For C-Raft probing phase
        
        self.perceived_nodes_online = set(range(self.N))
        self.last_response_tick = {i: 0 for i in range(self.N)} # Time node responded last
        self.response_timeout = 1.1 # How long leader has to wait before considering a node offline
        self.entry_bandwidth: Dict[int, float] = defaultdict(float) # Bandwidth used for each entry


        self.timeout_count = 0          # entries that timed out at least once
        self.timed_out_entries = set()  # track which entries already counted


    #Old perception logic
    def update_perception(self, current_tick: int):
        timed_out = set()
        for node_id in list(self.perceived_nodes_online):
            if current_tick - self.last_response_tick[node_id] > self.response_timeout:
                timed_out.add(node_id)
                self.perceived_nodes_online.discard(node_id)
        
    def get_online_nodes(self) -> List[int]:
        return [node_id for node_id, node in self.nodes.items() if node.online]

    def handle_node_failure(self, node_id: int, tick: int):
        self.nodes[node_id].set_online(False)

    def handle_node_reconnection(self, node_id: int, tick: int):
        self.nodes[node_id].set_online(True)
        self.catchup_node(node_id, tick)
        if node_id not in self.perceived_nodes_online:
            self.perceived_nodes_online.add(node_id)
            self.last_response_tick[node_id] = tick

    def get_cluster_storage(self) -> float:
        return sum(node.get_total_storage() for node in self.nodes.values())

    def get_live_storage(self) -> float:
        return sum(node.get_total_storage() for node in self.nodes.values() if node.online)
    
    def catchup_node(self, node_id: int, tick: int):
        raise NotImplementedError


class CRaftLeader(Leader):
    def __init__(self, F: int, log_size_kb: float = 4.0, entry_delays: dict = None):
        super().__init__(F, log_size_kb, entry_delays)
        self.log_history = []
        self.ec_timeout = 1.1  # wait this long for all N EC acks
        self.entry_delays = entry_delays or defaultdict(dict)

        self.logger = logging.getLogger('craft')
        if not self.logger.handlers:
            handler = logging.FileHandler('craft.log', mode='w')
            handler.setFormatter(logging.Formatter('%(message)s'))
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.DEBUG)
            self.logger.propagate = False

    #Start with EC dissemination
    def start_entry(self, tick: float, entry_id: int):
        entry = LogEntry(entry_id, tick)
        self.log_entries[entry_id] = entry
        entry.phase = "DISSEMINATING"
        entry.disseminated_tick = tick
        self.logger.debug(f"Tick {tick}: Starting entry {entry_id} dissemination")

        entry_metadata = {
            'id': entry_id,
            'is_ec': True,
            'disseminated_tick': tick,
            'retransmitted': False,
            'shards': {i: 0.0 for i in range(self.N)}
        }
        self.log_history.append(entry_metadata)

        # Send EC shard (1 fragment) to all N nodes immediately
        shard_size = self.fragment_size
        for node_id in self.nodes:
            #Pull delay from pre-generated delays for consistency across runs
            delay = self.entry_delays[entry_id][node_id]
            ack_tick = self.nodes[node_id].receive_shard(entry_id, shard_size, tick, delay)
            self.logger.debug(f"Tick {tick}: Sent entry {entry_id} to node {node_id} with EC shard, ack_tick={ack_tick}")
            #Node recieved shard
            if ack_tick > 0:
                self.pending_responses[entry_id][node_id] = ack_tick
                entry_metadata['shards'][node_id] = shard_size
                self.entry_bandwidth[entry_id] = shard_size

    # If EC times out, retransmit as full replication with k shards and lower commit threshold to k
    def _retransmit_as_replication(self, entry_id: int, tick: float):
        """Switch entry from EC to full replication and lower commit threshold to k."""
        entry_metadata = next(m for m in self.log_history if m['id'] == entry_id)
        if entry_metadata['retransmitted']:
            return
        #Set flags
        entry_metadata['is_ec'] = False
        entry_metadata['retransmitted'] = True

        #Re-tally responses
        self.received_responses[entry_id].clear()

        # Retransmit full log entry to all nodes
        for node_id in self.nodes:
            delay = self.entry_delays[entry_id][node_id]
            ack_tick = self.nodes[node_id].receive_shard(entry_id, self.log_size_kb, tick, delay)
            if ack_tick > 0:
                self.pending_responses[entry_id][node_id] = ack_tick
                entry_metadata['shards'][node_id] = self.log_size_kb
                self.entry_bandwidth[entry_id] = self.log_size_kb

    def collect_responses(self, current_tick: float):

        for entry_id in list(self.pending_responses.keys()):
            acks_due = self.pending_responses[entry_id]
            entry_metadata = next((m for m in self.log_history if m['id'] == entry_id), None)
            #Only look at disseminated entries
            if entry_metadata is None:
                continue
            #Add node to recieved responses if acked by now

            latest_ack_tick = entry_metadata.get('latest_ack_tick', 0.0)
            for node_id in list(acks_due.keys()):
                ack_tick = acks_due[node_id]
                if current_tick >= ack_tick:
                    self.received_responses[entry_id].add(node_id)
                    del acks_due[node_id]
                    latest_ack_tick = max(latest_ack_tick, ack_tick)

                    #Old perception logic does not affect for now
                    self.last_response_tick[node_id] = current_tick
                    if node_id not in self.perceived_nodes_online:
                        self.perceived_nodes_online.add(node_id)
            entry_metadata['latest_ack_tick'] = latest_ack_tick

            # Per-entry timeout check: if EC and timeout exceeded, retransmit
            disseminated_tick = entry_metadata['disseminated_tick']
            if (entry_metadata['is_ec'] and
                    not entry_metadata['retransmitted'] and
                    round(current_tick - disseminated_tick, 6) >= self.ec_timeout):
                #Did not recieve responses in time
                if len(self.received_responses[entry_id]) < self.N:
                    #Add to timeout counter
                    if entry_id not in self.timed_out_entries:
                        self.timeout_count += 1
                        self.timed_out_entries.add(entry_id)
                    self.logger.debug(f"Tick {current_tick}: Entry {entry_id} EC timeout, "
                         f"only {len(self.received_responses[entry_id])}/{self.N} responses, retransmitting")
                    self._retransmit_as_replication(entry_id, current_tick)
                    continue
                else:
                    #Did recieve responses, moving on
                    self.logger.debug(f"Tick {current_tick}: Entry {entry_id} EC timeout, but all nodes responded, no retransmission needed") 
                    entry_metadata['retransmitted'] = True  # prevent future checks

            # Commit threshold: N for EC, k for replication
            commit_threshold = self.N if entry_metadata['is_ec'] else self.k

            entry = self.log_entries[entry_id]
            if not entry.committed and len(self.received_responses[entry_id]) >= commit_threshold:
                entry.committed = True
                entry.commit_tick = entry_metadata['latest_ack_tick']
                self.logger.debug(f"Tick {current_tick}: Committed entry {entry_id} with {len(self.received_responses[entry_id])} responses (threshold: {commit_threshold}) with latency {entry.get_latency()}")
        self.update_perception(current_tick)


    def catchup_node(self, node_id: int, tick: float):
        node = self.nodes[node_id]
        for entry in self.log_history:
            if entry['shards'].get(node_id, 0) == 0:
                node.catch_up(entry['id'], self.log_size_kb)
                entry['shards'][node_id] = self.log_size_kb


class IRaftLeader(Leader):
    def __init__(self, F: int, log_size_kb: float = 4.0, entry_delays: dict = None):
        super().__init__(F, log_size_kb)
        self.actual_responders = defaultdict(set)  # entry_id -> set(node_ids that responded)
        self.log_history: List[dict] = []
        self.entry_delays = entry_delays or defaultdict(dict)


    def optimize_shards(self, entry_id: int):
        """
        Reduce shard sizes based on actual responders.
        Formula: new_shard_size = fragment_size * (N / actual_count)
        """
        if entry_id not in self.actual_responders:
            return
        
        actual_count = len(self.actual_responders[entry_id])
        
        # Only optimize if we have at least k responses (committed)
        if actual_count < self.k:
            return
        
        # Calculate optimal shard size based on how many nodes responded
        new_shard_size = self.fragment_size * (self.N / actual_count)
        
        # Update metadata
        entry_metadata = self.log_history[entry_id]
        entry_metadata['actual_responses'] = actual_count
        
        # Update all nodes that have this entry
        for node_id in self.actual_responders[entry_id]:
            old_size = entry_metadata['shard_size_at_node'][node_id]
            
            if abs(old_size - new_shard_size) > 0.001:  # Avoid floating point issues
                # Update node's physical storage
                self.nodes[node_id].update_shard_size(entry_id, new_shard_size)
                
                # Update metadata
                entry_metadata['shard_size_at_node'][node_id] = new_shard_size

    def catchup_node(self, node_id: int, tick: int):
        """
        Give reconnected node all missing entries and re-optimize.
        """
        node = self.nodes[node_id]
        
        for entry_metadata in self.log_history:
            entry_id = entry_metadata['id']
            
            # If node doesn't have this entry, give it a shard
            if entry_metadata['shard_size_at_node'][node_id] == 0:
                
                # Add this node to actual responders
                if entry_id not in self.actual_responders:
                    self.actual_responders[entry_id] = set()
                
                self.actual_responders[entry_id].add(node_id)
                
                # Calculate appropriate shard size
                actual_count = len(self.actual_responders[entry_id])
                
                if actual_count >= self.k:
                    # Entry is committed, use optimized size
                    shard_size = self.fragment_size * (self.N / actual_count)
                else:
                    # Entry not yet committed, use initial bloated size
                    shard_size = (self.F + 1) * self.fragment_size
                
                # Give node the shard
                node.catch_up(entry_id, shard_size)
                entry_metadata['shard_size_at_node'][node_id] = shard_size
        
        # Re-optimize all committed entries with the new responder count
        for entry_id in self.actual_responders:
            self.optimize_shards(entry_id)

class eAID(IRaftLeader):
    def __init__(self, F: int, log_size_kb: float = 4.0, heartbeat_interval: float = 0.7,
                 ec_timeout: float = 1.1, entry_delays: dict = None):
        super().__init__(F, log_size_kb, entry_delays)
        self.last_heartbeat_responses = self.N
        self.heartbeat_interval = heartbeat_interval
        self.last_heartbeat_tick = 0
        self.ec_timeout = ec_timeout
        self.last_fragments_sent = 2  # global perception starts at 2 fragments
        self.entry_delays = entry_delays or defaultdict(dict)

        #adding logger
        self.logger = logging.getLogger('adaptive_iraft')
        if not self.logger.handlers:
            handler = logging.FileHandler('adaptive_iraft.log', mode='w')
            handler.setFormatter(logging.Formatter('%(message)s'))
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.DEBUG)
            self.logger.propagate = False

    def calculate_fragments_to_send(self, responses: int) -> int:
        t = responses - self.F
        t = max(1, t)
        return max(2, math.ceil(self.F / t))

    def calculate_commit_threshold(self, entry_id: int, tick: float) -> int:
        entry_metadata = self.log_history[entry_id]
        fragments_sent = entry_metadata.get('fragments_sent')
        t = max(1, math.ceil(self.F / fragments_sent))
        threshold = math.ceil(self.F + t)
        threshold = max(self.k, min(self.N, threshold))
        #commit threshold should be same as what leader distributed for with fragments
        return threshold

    def _initial_commit_threshold(self, E: int) -> int:
        """3N/4 rounded up"""
        return math.ceil(3 * self.N / 4)

    def disseminate(self, tick: float, entry_id: int):
        # Use last known fragment count as starting point
        fragments_to_send = self.last_fragments_sent
        shard_size = fragments_to_send * self.fragment_size

        entry = LogEntry(entry_id, tick)
        self.log_entries[entry_id] = entry

        entry_metadata = {
            'id': entry_id,
            'shard_size_at_node': {i: 0.0 for i in range(self.N)},
            'fragments_sent': fragments_to_send,
            'initial_fragments': fragments_to_send,
            'initial_E': len(self.perceived_nodes_online),
            'actual_responses': 0,
            'disseminated_tick': tick,
            'round': 0,
            'commit_threshold': self._initial_commit_threshold(len(self.perceived_nodes_online)),
        }
        #Disseminate to all nodes with calculated shard size, pull delay from pre-generated delays for consistency
        for node_id in self.nodes:
            delay = self.entry_delays[entry_id][node_id]
            ack_tick = self.nodes[node_id].receive_shard(entry_id, shard_size, tick, delay)
            if ack_tick > 0:
                self.entry_bandwidth[entry_id] = shard_size
                self.pending_responses[entry_id][node_id] = ack_tick
                entry_metadata['shard_size_at_node'][node_id] = shard_size


        self.logger.debug(f"Tick {tick}: Disseminated entry {entry_id} with {fragments_to_send} fragments ")
        self.log_history.append(entry_metadata)

    # If EC times out, recalculate fragments based on actual responses and re-disseminate with new shard amounts
    def _reencode_entry(self, entry_id: int, tick: float):
        entry_metadata = self.log_history[entry_id]
        actual_responses = len(self.received_responses[entry_id])

        self.perceived_nodes_online = set(self.received_responses[entry_id])

        # Recalculate fragments based on actual responses, at least one more than before
        new_fragments = max(
            entry_metadata['fragments_sent'] + 1,
            self.calculate_fragments_to_send(actual_responses)
        )
        entry_metadata['fragments_sent'] = new_fragments
        new_shard_size = new_fragments * self.fragment_size
        new_threshold = self.calculate_commit_threshold(entry_id, tick)

        self.logger.debug(f"Tick {tick}: Re-encoding entry {entry_id} with {new_fragments} fragments and commit threshold {new_threshold} due to only {actual_responses} responses")

        # Clear existing acks
        self.received_responses[entry_id].clear()
        self.pending_responses[entry_id].clear()

        entry_metadata['fragments_sent'] = new_fragments
        entry_metadata['disseminated_tick'] = tick
        entry_metadata['commit_threshold'] = new_threshold
        entry_metadata['round'] += 1
        entry_metadata['initial_E'] = len(self.perceived_nodes_online)

        # Update global perception
        self.last_fragments_sent = new_fragments
        #resend to all nodes with new shard size, pull delay from pre-generated delays for consistency
        for node_id in self.nodes:
            delay = self.entry_delays[entry_id][node_id]
            ack_tick = self.nodes[node_id].receive_shard(entry_id, new_shard_size, tick, delay)
            if ack_tick > 0:
                self.pending_responses[entry_id][node_id] = ack_tick
                self.logger.debug(f"Tick {tick}: Node {node_id} acknowledged entry {entry_id}")
                entry_metadata['shard_size_at_node'][node_id] = new_shard_size
                self.entry_bandwidth[entry_id] = new_shard_size

    def collect_responses(self, current_tick: float):
        for entry_id in list(self.pending_responses.keys()):
            acks_due = self.pending_responses[entry_id]
            entry_metadata = self.log_history[entry_id]
            entry_metadata['latest_ack_tick'] = entry_metadata.get('latest_ack_tick', 0.0)
            for node_id in list(acks_due.keys()):
                ack_tick = acks_due[node_id]
                if current_tick >= ack_tick:
                    self.received_responses[entry_id].add(node_id)
                    del acks_due[node_id]
                    #old perception logic does not affect
                    entry_metadata['latest_ack_tick'] = max(entry_metadata['latest_ack_tick'], ack_tick)
                    if node_id not in self.perceived_nodes_online:
                        self.perceived_nodes_online.add(node_id)
                    self.last_response_tick[node_id] = current_tick


                    if entry_id not in self.actual_responders:
                        self.actual_responders[entry_id] = set()
                    self.actual_responders[entry_id].add(node_id)
                    self.optimize_shards(entry_id)
            entry_metadata['latest_ack_tick'] = entry_metadata.get('latest_ack_tick', 0.0)

            disseminated_tick = entry_metadata['disseminated_tick']
            timed_out = round(current_tick - disseminated_tick, 6) >= self.ec_timeout
            actual_responses = len(self.received_responses[entry_id])
            commit_threshold = entry_metadata['commit_threshold']
            entry = self.log_entries[entry_id]

            if timed_out and not entry.committed:
                if actual_responses < commit_threshold:
                    # Not enough responses — re-encode with more fragments
                    if entry_id not in self.timed_out_entries:
                        self.timeout_count += 1
                        self.timed_out_entries.add(entry_id)
                    self._reencode_entry(entry_id, current_tick)
                    self.logger.debug(f"Tick {current_tick}: Entry {entry_id} timed out with only {actual_responses} responses, re-encoding")
                    continue
                else:
                    # Got enough — update global fragment count downward
                    self.last_fragments_sent = entry_metadata['fragments_sent']

            elif not timed_out and not entry.committed and actual_responses > commit_threshold:
                # More responses than expected — update global perception for future entries
                new_fragments = self.calculate_fragments_to_send(actual_responses)
                new_fragments = min(new_fragments, entry_metadata['fragments_sent'])
                self.last_fragments_sent = new_fragments
                self.logger.debug(f"Tick {current_tick}: Entry {entry_id} has {actual_responses} responses, adjusting future entries to use {new_fragments} fragments")

            if not entry.committed and actual_responses >= commit_threshold:
                entry.committed = True
                entry.commit_tick = entry_metadata['latest_ack_tick']
                self.logger.debug(f"Tick {current_tick}: Committed entry {entry_id} with {actual_responses} responses (threshold: {commit_threshold}) and latency {entry.get_latency()}")

        self.update_perception(current_tick)

#not needed anymore for now
class SmartRafture(IRaftLeader):
    def __init__(self, F: int, log_size_kb: float = 4.0, heartbeat_interval: float = 0.7,
                 latency_threshold: float = 0.5, trust_window: int = 5, min_trusted: int = 2, entry_delays: dict = None):
        super().__init__(F, log_size_kb, entry_delays)
        self.last_heartbeat_responses = self.N
        self.heartbeat_interval = heartbeat_interval
        self.last_heartbeat_tick = 0
        self.trusted_nodes: set = set()
        self.node_latencies: Dict[int, list] = {i: [] for i in range(self.N)}
        
        self.latency_threshold = latency_threshold  # Max delay to be considered "fast"
        self.trust_window = trust_window            # How many recent entries to evaluate
        self.min_trusted = min_trusted              # Min trusted nodes before smart mode activates
        self.entry_delays = entry_delays or defaultdict(dict)


    def _record_latency(self, node_id: int, ack_tick: float, disseminate_tick: float):
        latency = ack_tick - disseminate_tick
        history = self.node_latencies[node_id]
        history.append(latency)
        if len(history) > self.trust_window:
            history.pop(0)

    def _update_trust(self, node_id: int):
        history = self.node_latencies[node_id]
        if len(history) >= self.trust_window:
            avg = sum(history) / len(history)
            if avg <= self.latency_threshold:
                self.trusted_nodes.add(node_id)
                logging.debug(f"Node {node_id} TRUSTED, history={history}")
                return
        self.trusted_nodes.discard(node_id)

    def _smart_mode_active(self) -> bool:
        return len(self.trusted_nodes) >= self.F + 1
    
    def calculate_fragments_to_send(self, entry_id: int, tick: float) -> int:
        if self._smart_mode_active():
            responses = len(self.trusted_nodes)
        else:
            responses = len(self.perceived_nodes_online)
        
        t = responses - self.F
        t = max(1, t)  # Avoid division by zero or negative
        logging.debug(f"Tick {tick} calculating fragments for entry {entry_id}: responses={responses}, t={t}, fragments={max(2, math.ceil(self.F / t))}")
        return max(2, math.ceil(self.F / t))
    

    def calculate_commit_threshold(self, entry_id: int, tick: float) -> int:
        entry_metadata = self.log_history[entry_id]
        fragments_sent = entry_metadata.get('fragments_sent')
        t = max(1, math.ceil(self.F / fragments_sent))
        threshold = self.F + t

        # if self._smart_mode_active():
        #     smart_N = entry_metadata.get('initial_trusted_E')
        #     if smart_N is not None:
        #         smart_k = max(1, math.ceil(smart_N / 2)) 
        #         threshold = max(smart_k, min(threshold, smart_N))
        #     else:
        #         # Entry was disseminated before smart mode activated, fall back
        #         initial_E = entry_metadata.get('initial_E', self.N)
        #         threshold = max(self.k, min(threshold, self.N, initial_E))
        # else:
        #     initial_E = entry_metadata.get('initial_E', self.N)
        #     threshold = max(self.k, min(threshold, self.N, initial_E))
        logging.debug(f"Tick {tick} commit threshold for entry {entry_id}: {threshold} trusted: {len(self.trusted_nodes)})")
        return threshold
    
    def disseminate(self, tick: float, entry_id: int):
        fragments_to_send = self.calculate_fragments_to_send(entry_id, tick)
        shard_size = fragments_to_send * self.fragment_size
        single_shard_size = self.fragment_size  # Untrusted nodes get one fragment

        entry = LogEntry(entry_id, tick)
        self.log_entries[entry_id] = entry

        entry_metadata = {
            'id': entry_id,
            'shard_size_at_node': {i: 0.0 for i in range(self.N)},
            'fragments_sent': fragments_to_send,
            'cluster_health': self.last_heartbeat_responses,
            'initial_E': len(self.perceived_nodes_online),
            'initial_trusted_E': len(self.trusted_nodes) if self._smart_mode_active() else None,
            'actual_responses': 0
        }

        for node_id in self.perceived_nodes_online:
            if self._smart_mode_active() and node_id not in self.trusted_nodes:
                # Untrusted: single shard, not part of commit quorum
                size = single_shard_size
            else:
                size = shard_size

            ack_tick = self.nodes[node_id].receive_shard(entry_id, size, tick)
            logging.debug(f"Tick {tick} sending entry {entry_id} to node {node_id} with shard size {size}")
            if ack_tick > 0:
                self.entry_bandwidth[entry_id] = size
                self.pending_responses[entry_id][node_id] = ack_tick
                entry_metadata['shard_size_at_node'][node_id] = size

        self.log_history.append(entry_metadata)

    def collect_responses(self, current_tick: float):
        for entry_id in list(self.pending_responses.keys()):
            acks_due = self.pending_responses[entry_id]
            disseminate_tick = self.log_entries[entry_id].created_tick  # When this entry was sent

            for node_id in list(acks_due.keys()):
                ack_tick = acks_due[node_id]

                if current_tick >= ack_tick:
                    self.received_responses[entry_id].add(node_id)
                    del acks_due[node_id]

                    if node_id not in self.perceived_nodes_online:
                        self.perceived_nodes_online.add(node_id)

                    self.last_response_tick[node_id] = current_tick
                    
                    if entry_id not in self.actual_responders:
                        self.actual_responders[entry_id] = set()
                    self.actual_responders[entry_id].add(node_id)

                    self._record_latency(node_id, ack_tick, disseminate_tick)


                    self.optimize_shards(entry_id)

                    # Only count trusted nodes toward commit (in smart mode)
                    if self._smart_mode_active():
                        trusted_responses = self.received_responses[entry_id] & self.trusted_nodes
                        response_count = len(trusted_responses)
                    else:
                        response_count = len(self.received_responses[entry_id])

                    commit_threshold = self.calculate_commit_threshold(entry_id, current_tick)

                    if response_count >= commit_threshold:
                        entry = self.log_entries[entry_id]
                        if not entry.committed:
                            entry.committed = True
                            entry.commit_tick = current_tick
                            logging.debug(f"Tick {current_tick} committing entry {entry_id} with {response_count} responses (threshold: {commit_threshold})")

        self.update_perception(current_tick)
         # Update trust based on observed latency
        for node_id in self.nodes:
            self._update_trust(node_id)
    
class HRaft(Leader):
    def __init__(self, F: int, ec_timeout: float = 1.1, entry_delays: dict = None):
        super().__init__(F)
        self.ec_timeout = ec_timeout
        self.entry_delays = entry_delays or defaultdict(dict)

        self.entry_metadata: Dict[int, dict] = {}
        self.degraded_mode = False


        # HRaft-specific logger
        self.logger = logging.getLogger('hraft')
        if not self.logger.handlers:
            handler = logging.FileHandler('hraft.log', mode='w')
            handler.setFormatter(logging.Formatter('%(message)s'))
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.DEBUG)
            self.logger.propagate = False
    #Initially disseminate one shard to all nodes
    def start_entry(self, tick: float, entry_id: int,):
        entry = LogEntry(entry_id, tick)
        self.log_entries[entry_id] = entry
        entry.phase = "DISSEMINATING"
        self.logger.debug(f"Tick {tick}: Starting entry {entry_id} dissemination")

        self.entry_metadata[entry_id] = {
            'redistributed_for': set(),
            'disseminated_tick': tick,
            'node_send_tick': {node_id: tick for node_id in range(self.N)},
            'saviors': set(),           # nodes that received redistributed shards
            'savior_acks': set(),
            'total_sent': 0,
            'recipients': set()       # saviors that have acked back
        }
        meta = self.entry_metadata[entry_id]
        for node_id in self.nodes:
            delay = self.entry_delays[entry_id][node_id]
            ack_tick = self.nodes[node_id].receive_shard(entry_id, self.fragment_size, tick, delay)
            self.logger.debug(f"Tick {tick}: Sent entry {entry_id} to node {node_id}, ack_tick={ack_tick}")
            if ack_tick > 0:
                self.pending_responses[entry_id][node_id] = ack_tick
                meta['total_sent'] += self.fragment_size
                meta['recipients'].add(node_id) 

        self.entry_bandwidth[entry_id] = meta['total_sent'] / len(meta['recipients'])

    #For entries that EC timeout redistribute missing shards, simplified for latency recording sake,
    #  just send one fragment since no bandwidth associated latency in sim
    def redistribute_missing(self, entry_id: int, tick: float):
        meta = self.entry_metadata[entry_id]

        self.received_responses[entry_id].clear()
        self.pending_responses[entry_id].clear()

        meta['redistributed'] = True
        meta['redistributed_tick'] = tick

        for node_id in self.nodes:
            delay = self.entry_delays[entry_id][node_id]
            ack_tick = self.nodes[node_id].receive_shard(entry_id, self.fragment_size, tick, delay)
            if ack_tick > 0:
                self.pending_responses[entry_id][node_id] = ack_tick
                meta['total_sent'] += self.fragment_size
                meta['recipients'].add(node_id)
                self.entry_bandwidth[entry_id] = self.fragment_size


    def collect_responses(self, current_tick: float):
        for entry_id in list(self.pending_responses.keys()):
            acks_due = self.pending_responses[entry_id]
            meta = self.entry_metadata.get(entry_id, {})
            disseminated_tick = meta.get('disseminated_tick', 0)
            self.entry_metadata[entry_id]['latest_ack_tick'] = self.entry_metadata[entry_id].get('latest_ack_tick', 0.0)
            for node_id in list(acks_due.keys()):
                ack_tick = acks_due[node_id]
                if current_tick >= acks_due[node_id]:
                    self.received_responses[entry_id].add(node_id)
                    del acks_due[node_id]
                    self.entry_metadata[entry_id]['latest_ack_tick'] = max(self.entry_metadata[entry_id]['latest_ack_tick'], ack_tick)
                    self.last_response_tick[node_id] = current_tick
                    if node_id not in self.perceived_nodes_online:
                        self.perceived_nodes_online.add(node_id)
            self.entry_metadata[entry_id]['latest_ack_tick'] = self.entry_metadata[entry_id].get('latest_ack_tick', 0.0)
            timed_out = round(current_tick - disseminated_tick, 6) >= self.ec_timeout

            #if timed out, redistribute, update timed out
            if (timed_out
                    and not meta.get('redistributed', False)
                    and not meta.get('timeout_checked', False)
                    and not self.log_entries[entry_id].committed):
                #not enough responses
                if len(self.received_responses[entry_id]) < self.N:
                    self.redistribute_missing(entry_id, current_tick)
                    #update timeout counter
                    if entry_id not in self.timed_out_entries:
                        self.timeout_count += 1
                        self.timed_out_entries.add(entry_id)
                    self.logger.debug(f"Tick {current_tick}: Entry {entry_id} EC timeout, "
                                    f"only {len(self.received_responses[entry_id])}/{self.N} "
                                    f"responses, redistributing")
                    continue
                else:
                    self.logger.debug(f"Tick {current_tick}: Entry {entry_id} EC timeout, "
                                    f"but all nodes responded, no redistribution needed")
                    meta['timeout_checked'] = True
            #n if no redistribution, otherwise check F+1 nodes responded to redistribution
            commit_threshold = self.N if not meta.get('redistributed', False) else self.k

            entry = self.log_entries[entry_id]
            if not entry.committed and len(self.received_responses[entry_id]) >= commit_threshold:
                entry.committed = True
                entry.commit_tick = self.entry_metadata[entry_id]['latest_ack_tick']
                self.logger.debug(f"Tick {current_tick}: Committing entry {entry_id} with "
                                f"{len(self.received_responses[entry_id])} responses "
                                f"(threshold: {commit_threshold}) with latency {entry.get_latency()}")

        self.update_perception(current_tick)

    def handle_node_reconnection(self, node_id: int, tick: float):
        self.nodes[node_id].set_online(True)
        self.perceived_nodes_online.add(node_id)
        self.last_response_tick[node_id] = tick
        for entry_id, meta in self.entry_metadata.items():
            entry = self.log_entries.get(entry_id)
            if entry and not entry.committed:
                delay = self.entry_delays[entry_id][node_id]
                ack_tick = self.nodes[node_id].receive_shard(
                    entry_id, self.fragment_size, tick, delay
                )
                if ack_tick > 0:
                    self.pending_responses[entry_id][node_id] = ack_tick


class FlexRaftLeader(Leader):
    def __init__(self, F: int, log_size_kb: float = 4.0, ec_timeout: float = 1.1, entry_delays: dict = None):
        super().__init__(F, log_size_kb)
        self.log_history: List[dict] = []
        self.ec_timeout = ec_timeout
        self.last_E = self.N  # global perception starts at N
        self.entry_delays = entry_delays or defaultdict(dict)

        self.multi_timeout_entries = 0

        self.logger = logging.getLogger('flexraft')
        if not self.logger.handlers:
            handler = logging.FileHandler('flexraft.log', mode='w')
            handler.setFormatter(logging.Formatter('%(message)s'))
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.DEBUG)
            self.logger.propagate = False

    def disseminate(self, tick: float, entry_id: int):
        entry = LogEntry(entry_id, tick)
        self.log_entries[entry_id] = entry

        # Use global perception as starting E and encode accordingly
        E = self.last_E
        k = max(1, math.ceil(E / 2))
        shard_size = self.log_size_kb / k

        entry_metadata = {
            'id': entry_id,
            'shard_size_at_node': {i: 0.0 for i in range(self.N)},
            'current_E': E,
            'current_k': k,
            'initial_k': k,
            'disseminated_tick': tick,
            'round': 0,           # how many re-encodings have happened
            'total_sent': 0.0,
            'recipients': set(),
        }
        self.log_history.append(entry_metadata)

        #disseminate to all nodes with calculated shard size, pull delay from pre-generated delays for consistency
        for node_id in self.nodes:
            delay = self.entry_delays[entry_id][node_id]
            ack_tick = self.nodes[node_id].receive_shard(entry_id, shard_size, tick, delay)
            self.logger.debug(f"Tick {tick}: Sent entry {entry_id} to node {node_id}, "
                            f"shard={shard_size:.4f}, ack_tick={ack_tick}")
            if ack_tick > 0:
                self.pending_responses[entry_id][node_id] = ack_tick
                entry_metadata['shard_size_at_node'][node_id] = shard_size
                entry_metadata['total_sent'] += shard_size
                entry_metadata['recipients'].add(node_id)

        self.entry_bandwidth[entry_id] = (
            entry_metadata['total_sent'] / len(entry_metadata['recipients'])
            if entry_metadata['recipients'] else 0
        )

    def _reencode_entry(self, entry_id: int, tick: float):
        """Re-encode a single entry based on how many nodes responded so far."""
        entry_metadata = next(m for m in self.log_history if m['id'] == entry_id)

        # New E is however many nodes responded
        actual_responses = len(self.received_responses[entry_id])
        new_E = max(actual_responses, self.k)
        new_k = max(1, math.ceil(new_E / 2))
        new_shard_size = self.log_size_kb / new_k

        self.logger.debug(f"Tick {tick}: Entry {entry_id} re-encoding round "
                         f"{entry_metadata['round'] + 1}: E={new_E}, k={new_k}, "
                         f"shard={new_shard_size:.4f} "
                         f"(had {actual_responses} responses)")

        # Clear existing acks — commit only on fresh re-encoded acks
        self.received_responses[entry_id].clear()
        self.pending_responses[entry_id].clear()

        entry_metadata['current_E'] = new_E
        entry_metadata['current_k'] = new_k
        entry_metadata['disseminated_tick'] = tick
        entry_metadata['round'] += 1

        # Update global perception
        self.last_E = new_E

        # Resend to all perceived online nodes with new shard size
        for node_id in self.nodes:
            #Use next dissemination delay for extra round
            delay = self.entry_delays[entry_id + 1][node_id]
            ack_tick = self.nodes[node_id].receive_shard(entry_id, new_shard_size, tick, delay)
            self.logger.debug(f"Tick {tick}: Re-sent entry {entry_id} to node {node_id}, "
                            f"ack_tick={ack_tick}")
            if ack_tick > 0:
                self.pending_responses[entry_id][node_id] = ack_tick
                entry_metadata['shard_size_at_node'][node_id] = new_shard_size
                entry_metadata['total_sent'] += new_shard_size
                entry_metadata['recipients'].add(node_id)

        self.entry_bandwidth[entry_id] = (
            entry_metadata['total_sent'] / len(entry_metadata['recipients'])
            if entry_metadata['recipients'] else 0
        )

    def collect_responses(self, current_tick: float):
        for entry_id in list(self.pending_responses.keys()):
            acks_due = self.pending_responses[entry_id]
            entry_metadata = next((m for m in self.log_history if m['id'] == entry_id), None)
            if entry_metadata is None:
                continue
            entry_metadata['latest_ack_tick'] = entry_metadata.get('latest_ack_tick', 0.0)
            for node_id in list(acks_due.keys()):
                ack_tick = acks_due[node_id]
                if current_tick >= ack_tick:
                    self.received_responses[entry_id].add(node_id)
                    del acks_due[node_id]
                    entry_metadata['latest_ack_tick'] = max(entry_metadata['latest_ack_tick'], ack_tick)
                    self.last_response_tick[node_id] = current_tick
                    if node_id not in self.perceived_nodes_online:
                        self.perceived_nodes_online.add(node_id)
            latest_ack_tick = entry_metadata.get('latest_ack_tick', 0.0)
            disseminated_tick = entry_metadata['disseminated_tick']
            timed_out = round(current_tick - disseminated_tick, 6) >= self.ec_timeout
            actual_responses = len(self.received_responses[entry_id])
            current_E = entry_metadata['current_E']
            entry = self.log_entries[entry_id]


            if timed_out and not entry.committed:
                if actual_responses < current_E:
                    # Didn't get enough responses — re-encode and resend

                    if entry_id not in self.timed_out_entries:
                        self.timeout_count += 1
                        self.timed_out_entries.add(entry_id)
                    else:
                        self.multi_timeout_entries += 1


                    if actual_responses >= self.k:
                        # Have enough for safety, re-encode 
                        self._reencode_entry(entry_id, current_tick)
                        continue  # skip commit check this tick
                    else:
                        # Not even k responses — still re-encode but log warning
                        self.logger.debug(f"Tick {current_tick}: Entry {entry_id} only "
                                         f"{actual_responses} responses, below k={self.k}")
                        self._reencode_entry(entry_id, current_tick)
                        continue
                else:
                    # Got all E responses in time — update global perception
                    self.last_E = current_E

            elif not timed_out and not entry.committed and actual_responses > current_E:
                # More nodes online than expected — update global perception for future entries
                new_E = min(actual_responses, len(self.perceived_nodes_online))
                self.last_E = new_E
                self.logger.debug(f"Tick {current_tick}: Entry {entry_id} saw {actual_responses} "
                                f"responses, updating global E {current_E}->{new_E} for future entries")
            # Commit when we have current_E responses
            if not entry.committed and actual_responses >= current_E:
                entry.committed = True
                entry.commit_tick = latest_ack_tick
                self.logger.debug(f"Tick {current_tick}: Committed entry {entry_id} "
                                 f"with {actual_responses} responses "
                                 f"(E={current_E}, round={entry_metadata['round']}) "
                                 f"latency={entry.get_latency():.4f}ms")

        self.update_perception(current_tick)

    def catchup_node(self, node_id: int, tick: float):
        node = self.nodes[node_id]
        for entry_metadata in self.log_history:
            entry_id = entry_metadata['id']
            if entry_metadata['shard_size_at_node'].get(node_id, 0) == 0:
                shard_size = entry_metadata['shard_size_at_node'].get(
                    next(iter(entry_metadata['recipients']), None), self.log_size_kb
                )
                node.catch_up(entry_id, shard_size)
                entry_metadata['shard_size_at_node'][node_id] = shard_size

class Simulation:
    def __init__(self, leader: Leader, events: Dict[float, Dict[float, bool]]):
        self.leader = leader
        self.events = events
        self.stats = []
        self.all_latencies = []
        self._fired_events = set()

    #duration/tick size is how many entries can get disseminated (entries get disseminated once per tick per client) 
    def run(self, duration_ms: float = 100.0, tick_size_ms: float = 0.1, 
        stop_dissemination_ms: float = 100.0, clients: int = 1):
        next_entry_id = 0
        pending_queue = []
        tick = 0.0

        while tick < duration_ms:

            #For partition
            for event_time in list(self.events.keys()):
                if tick >= event_time and event_time not in self._fired_events:
                    self._fired_events.add(event_time)
                    for node_id, status in self.events[event_time].items():
                        if status:
                            self.leader.handle_node_reconnection(node_id, tick)
                        else:
                            self.leader.handle_node_failure(node_id, tick)

            # Each client submits one entry per tick
            if tick <= stop_dissemination_ms:
                for _ in range(clients):
                    pending_queue.append(next_entry_id)
                    next_entry_id += 1

            # Leader processes one entry per tick from queue
            if pending_queue:
                entry_id = pending_queue.pop(0)
                if hasattr(self.leader, 'start_entry'):
                    self.leader.start_entry(tick, entry_id)
                else:
                    self.leader.disseminate(tick, entry_id)


            self.leader.collect_responses(tick)
            tick = round(tick + tick_size_ms, 6)
            
            # 5. Track newly committed entries
            newly_committed = [
                e for e in self.leader.log_entries.values() 
                if e.committed and e.commit_tick == tick
            ]
            #get bandiwdth and latency of committed entries
            for entry in newly_committed:
                latency = entry.get_latency()
                
                is_ec = True
                if isinstance(self.leader, CRaftLeader):
                    if entry.id < len(self.leader.log_history):
                        is_ec = self.leader.log_history[entry.id].get('is_ec', True)
                elif isinstance(self.leader, HRaft):
                    meta = self.leader.entry_metadata.get(entry.id, {})
                    is_ec = len(meta.get('redistributed_for', set())) == 0
                elif isinstance(self.leader, FlexRaftLeader):
                    # EC if committed on first round with no re-encoding
                    meta = self.leader.log_history[entry.id]
                    is_ec = meta.get('round', 0) == 0
                elif isinstance(self.leader, IRaftLeader):
                    if entry.id in self.leader.actual_responders:
                        actual_count = len(self.leader.actual_responders[entry.id])
                        is_ec = (actual_count == self.leader.N)
                    else:
                        is_ec = False
                
                self.all_latencies.append({
                    'tick': tick,
                    'entry_id': entry.id,
                    'latency': latency,
                    'is_ec': is_ec,
                    'bandwidth': self.leader.entry_bandwidth.get(entry.id, 0)
                })
            
            # 6. Calculate statistics
            committed = [e for e in self.leader.log_entries.values() if e.committed]
            latencies = [e.get_latency() for e in committed if e.get_latency() is not None]
            avg_lat = sum(latencies) / len(latencies) if latencies else 0
            
            recent_committed = sorted(committed, key=lambda e: e.commit_tick, reverse=True)[:10]
            recent_latencies = [e.get_latency() for e in recent_committed]
            recent_avg_lat = sum(recent_latencies) / len(recent_latencies) if recent_latencies else 0
            
            self.stats.append({
                'tick': tick,
                'total_storage': self.leader.get_cluster_storage(),
                'live_storage': self.leader.get_live_storage(),
                'nodes_up': len(self.leader.get_online_nodes()),
                'perceived_nodes': len(self.leader.perceived_nodes_online),
                'committed_count': len(committed),
                'avg_latency': avg_lat,
                'recent_latency': recent_avg_lat
            })


def save_latencies_csv(craft_latencies, adaptive_latencies, h_raft_latencies, flex_latencies):
    all_data = []
    for protocol_name, latency_data in [
        ('Full Replication Fallback',    craft_latencies),
        ('eAID',      adaptive_latencies),
        ('Endangered Fragment Resharing',    h_raft_latencies),
        ('Proactive Encoding', flex_latencies),
    ]:
        for d in latency_data:
            all_data.append({
                'protocol':         protocol_name,
                'entry_id':         d['entry_id'],
                'created_tick':     d['created_tick'],
                'commit_tick':      d['commit_tick'],
                'latency':          d['latency'],
                'initial_k':        d.get('initial_k'),
                'final_shard_size': d.get('final_shard_size'),
                'total_bandwidth':  d.get('total_bandwidth'),
            })

    with open('latencies.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'protocol', 'entry_id', 'created_tick', 'commit_tick',
            'latency', 'initial_k', 'final_shard_size', 'total_bandwidth'
        ])
        writer.writeheader()
        writer.writerows(all_data)

    print("Saved latencies.csv")



def run_comparison_heatmap():
    F = 5
    scenario_events = {
        # 15.0: {2: False, 1: False, 0: False},
        # 60.0: {2: True, 1: True, 0: True}
    }
    entry_delays = generate_entry_delays(num_entries= 1000, N=2*F+1)

    
    craft_leader = CRaftLeader(F=F, entry_delays=entry_delays)
    craft_sim = Simulation(craft_leader, scenario_events)
    craft_sim.run()
    
    flex_leader = FlexRaftLeader(F=F, entry_delays=entry_delays)
    flex_sim = Simulation(flex_leader, scenario_events)
    flex_sim.run()

    adaptive_leader = eAID(F=F, entry_delays=entry_delays)
    adaptive_sim = Simulation(adaptive_leader, scenario_events)
    adaptive_sim.run()

    hraft_leader = HRaft(F=F, entry_delays=entry_delays)
    hraft_sim = Simulation(hraft_leader, scenario_events)
    hraft_sim.run()

    # smart_leader = SmartIRaftLeader(F=F, heartbeat_interval=0.7, latency_threshold = 0.7, trust_window=10, min_trusted=2)
    # smart_sim = Simulation(smart_leader, scenario_events)
    # smart_sim.run()
    
    def get_latency_data(sim):
        latencies = []
        for entry_id, entry in sorted(sim.leader.log_entries.items()):
            if entry.committed:
                record = {
                    'entry_id': entry_id,
                    'created_tick': entry.created_tick,
                    'commit_tick': entry.commit_tick,
                    'latency': entry.get_latency(),
                    'initial_k': None,
                    'final_shard_size': None,
                    'total_bandwidth': None,
                }

                if isinstance(sim.leader, FlexRaftLeader):
                    meta = next((m for m in sim.leader.log_history if m['id'] == entry_id), {})
                    record['initial_k'] = meta.get('initial_k', sim.leader.k)
                    record['final_shard_size'] = sim.leader.log_size_kb / meta.get('current_k', sim.leader.k)
                    record['total_bandwidth'] = meta.get('total_sent', 0) / len(meta['recipients']) if meta.get('recipients') else 0

                elif isinstance(sim.leader, eAID):
                    if entry_id < len(sim.leader.log_history):
                        meta = sim.leader.log_history[entry_id]
                        record['initial_k'] = meta.get('initial_fragments')
                        # final shard size is what's currently stored at each node (post-optimization)
                        shard_sizes = [s for s in meta['shard_size_at_node'].values() if s > 0]
                        record['final_shard_size'] = sum(shard_sizes) / len(shard_sizes) if shard_sizes else 0
                        # bandwidth: sum of all transmissions (initial + re-encodings) averaged per node
                        record['total_bandwidth'] = sim.leader.entry_bandwidth.get(entry_id, 0)

                elif isinstance(sim.leader, CRaftLeader):
                    meta = next((m for m in sim.leader.log_history if m['id'] == entry_id), {})
                    retransmitted = meta.get('is_ec', False) == False
                    record['final_shard_size'] = sim.leader.log_size_kb if retransmitted else sim.leader.fragment_size
                    # bandwidth: fragment_size initial send + log_size_kb retransmit if it happened
                    record['total_bandwidth'] = (sim.leader.fragment_size + sim.leader.log_size_kb) if retransmitted else sim.leader.fragment_size

                elif isinstance(sim.leader, HRaft):
                    meta = sim.leader.entry_metadata.get(entry_id, {})
                    record['final_shard_size'] = sim.leader.fragment_size
                    # total_sent is sum across all nodes, average per node
                    total = meta.get('total_sent', sim.leader.fragment_size)
                    recipients = len(meta.get('recipients', set())) or sim.leader.N
                    record['total_bandwidth'] = total / recipients

                latencies.append(record)
        return latencies
    
    craft_latencies = get_latency_data(craft_sim)
    adaptive_latencies = get_latency_data(adaptive_sim)
    h_raft_latencies = get_latency_data(hraft_sim)
    flex_latencies = get_latency_data(flex_sim)
    # smart_latencies = get_latency_data(smart_sim)

    save_latencies_csv(craft_latencies, adaptive_latencies, h_raft_latencies, flex_latencies)
    
    fig, ax = plt.subplots(1, 1, figsize=(16, 6))
    
    from matplotlib.colors import LinearSegmentedColormap
    colors = ['yellow', 'orange', 'red', 'darkred']
    cmap = LinearSegmentedColormap.from_list('latency', colors, N=100)
    
    all_latencies = (
        [d['latency'] for d in craft_latencies] +
        [d['latency'] for d in adaptive_latencies] +
        [d['latency'] for d in h_raft_latencies] +
        [d['latency'] for d in flex_latencies]
        # [d['latency'] for d in smart_latencies]
    )
    vmin = min(all_latencies)
    vmax = max(all_latencies)
    
    row_height = 0.8

    #TO ADJUST NAMING
    protocols = [
        ('FlexRaft-ID', flex_latencies, 3),
        ('Full Replication Fallback',    craft_latencies, 2),
        ('eAID',    adaptive_latencies, 0),
        ('Endangered Fragment Resharing',    h_raft_latencies, 1),
        # ('Smart Rafture', smart_latencies, 4)
    ]
    
    for protocol_name, latency_data, row in protocols:
        for data in latency_data:
            entry_id = data['entry_id']
            latency = data['latency']
            color = cmap((latency - vmin) / (vmax - vmin))
            ax.plot([entry_id, entry_id], [row, row + row_height],
                   color=color, linewidth=2, solid_capstyle='butt')
    
    # ax.axvspan(150, 600, color='gray', alpha=0.2, zorder=-1)
    
    ax.set_xlabel('Entry ID', fontsize=12)
    ax.set_ylabel('Protocol', fontsize=12)
    # label at middle of each row band
    ax.set_yticks([r + row_height / 2 for _, _, r in protocols])
    ax.set_yticklabels([name for name, _, _ in protocols])
    ax.set_ylim(-0.2, max(r for _, _, r in protocols) + row_height + 0.2)  # 4 rows, each 1 unit tall
    ax.set_xlim(0, max(d['entry_id'] for d in craft_latencies + adaptive_latencies + h_raft_latencies + flex_latencies))
    ax.grid(True, alpha=0.3, axis='x')
    
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(vmin=vmin, vmax=vmax))
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, orientation='vertical', pad=0.02)
    cbar.mappable.set_clim(vmin, 3.3)
    ticks = [1.0, 1.5, 2.0, 2.5, 3.0, 3.3]
    cbar.set_ticks(ticks)
    cbar.set_label('Latency (ms)', fontsize=12)
    
    plt.tight_layout()
    plt.show()
    
    print("\n=== LATENCY SUMMARY ===")
    for protocol_name, latency_data in [
        ('C-Raft', craft_latencies),
        ('Adaptive I-Raft', adaptive_latencies),
        ('H-Raft', h_raft_latencies),
        ('Flex-Raft', flex_latencies),
        # ('Smart-I-Raft', smart_latencies)
    ]:
        lats = [d['latency'] for d in latency_data]
        print(f"{protocol_name:15} min={min(lats):4.1f}  max={max(lats):4.1f}  avg={sum(lats)/len(lats):4.1f}")
        

    for name, leader in [
        ('C-Raft', craft_leader),
        ('Adaptive', adaptive_leader),
        ('H-Raft', hraft_leader),
        ('Flex-Raft', flex_leader),
    ]:
        committed = sum(1 for e in leader.log_entries.values() if e.committed)
        print(f"{name:<15} timeouts={leader.timeout_count}")

    print(f"{'Flex-Raft':<15} multi-timeouts={flex_leader.multi_timeout_entries}")



def get_shard_latency_data(sim):
    result = []
    leader = sim.leader

    for entry_id, entry in sorted(leader.log_entries.items()):
        if not entry.committed:
            continue
        latency = entry.get_latency()
        initial_shard_size = 0.0
        skip = False

        if isinstance(leader, CRaftLeader):
            meta = next((m for m in leader.log_history if m['id'] == entry_id), {})
            if meta.get('retransmitted', False):
                skip = True
            else:
                initial_shard_size = leader.fragment_size

        elif isinstance(leader, HRaft):
            initial_shard_size = leader.fragment_size

        elif isinstance(leader, FlexRaftLeader):
            meta = next((m for m in leader.log_history if m['id'] == entry_id), {})
            if meta.get('round', 0) > 0:
                skip = True
            else:
                k = meta.get('current_k', leader.k)
                initial_shard_size = leader.log_size_kb / k

        elif isinstance(leader, eAID):
            meta = leader.log_history[entry_id] if entry_id < len(leader.log_history) else {}
            if meta.get('round', 0) > 0:
                skip = True
            else:
                initial_shard_size = meta.get('fragments_sent', 1) * leader.fragment_size

        if not skip:
            result.append({'initial_shard_size': initial_shard_size, 'latency': latency})
    return result

def graph_all_scatter():
    import pandas as pd
    import matplotlib.pyplot as plt

    df = pd.read_csv('latencies.csv')
    fragment_size = 4 / 6

    configs = [
        ('Full Replication Fallback',    '#27AE60',  lambda d: d['total_bandwidth']),
        ('eAID',      '#8E44AD',   lambda d: d['total_bandwidth']),
        ('Proactive Encoding', '#2980B9', lambda d: d['total_bandwidth']),
        ('Endangered Fragment Resharing',    '#D35400',   lambda d: d['total_bandwidth'])
    ]

    fig, ax = plt.subplots(figsize=(10, 6))

    for protocol, color, size_fn in configs:
        data = df[df['protocol'] == protocol]
        ax.scatter(
            size_fn(data),
            data['latency'],
            label=protocol, color=color, alpha=0.4, s=15
        )

    ax.set_xlabel("Total Bandwidth Used (KB)")
    ax.set_ylabel("Commit Latency (ms)")
    ax.set_title("Bandwidth Used vs Latency (All Protocols)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()


def graph_all_scatter_mean():
    import pandas as pd
    import matplotlib.pyplot as plt

    df = pd.read_csv('latencies.csv')
    fragment_size = 4 / 6

    configs = [
        ('C-Raft',    'red',  lambda d: [fragment_size] * len(d)),
        ('eAID',      'blue',   lambda d: d['initial_k'] * fragment_size),
        ('Flex-Raft', 'purple', lambda d: 4 / d['initial_k']),
    ]

    fig, ax = plt.subplots(figsize=(10, 6))

    for protocol, color, size_fn in configs:
        data = df[df['protocol'] == protocol].copy()
        data['shard_size'] = size_fn(data)
        means = data.groupby('shard_size')['latency'].mean().reset_index()
        ax.scatter(
            means['shard_size'],
            means['latency'],
            label=protocol, color=color, s=60
        )

    # eAID post-pruning: overall mean latency plotted at fragment_size (4/6)
    ax.axvline(
        x=fragment_size * 2,
        color='blue', linestyle='--', linewidth=1.5,
        label=f'eAID post-pruning size'
    )

    ax.set_xlabel("Dissemination Shard Size (KB)")
    ax.set_ylabel("Mean Commit Latency (ms)")
    ax.set_title("Shard Size vs Mean Latency (All Protocols)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    run_comparison_heatmap()