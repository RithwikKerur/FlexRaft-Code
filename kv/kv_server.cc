#include "kv_server.h"

#include <chrono>
#include <cstdlib>
#include <mutex>
#include <thread>

#include "RCF/RecursionLimiter.hpp"
#include "RCF/ThreadLibrary.hpp"
#include "client.h"
#include "kv_format.h"
#include "log_entry.h"
#include "raft_node.h"
#include "raft_struct.h"
#include "raft_type.h"
#include "rpc.h"
#include "storage_engine.h"
#include "type.h"
#include "util.h"
#include <cstdio>


namespace kv {
KvServer *KvServer::NewKvServer(const KvServerConfig &config) {
  auto kv_server = new KvServer();
  kv_server->channel_ = Channel::NewChannel(100000);
  kv_server->db_ = StorageEngine::Default(config.storage_engine_name);
  kv_server->id_ = config.raft_node_config.node_id_me;

  // Pass channel as a Rsm into raft
  auto raft_config = config.raft_node_config;
  raft_config.rsm = kv_server->channel_;
  kv_server->raft_ = new raft::RaftNode(raft_config);

  kv_server->exit_ = false;
  return kv_server;
}

KvServer *KvServer::NewKvServer(const KvClusterConfig &config, raft::raft_node_id_t node_id) {
  auto raft_cluster_config = ConstructRaftClusterConfig(config);
  assert(config.count(node_id) > 0);
  auto kv_node_config = config.at(node_id);
  auto raft_config = raft::RaftNode::NodeConfig{node_id, raft_cluster_config,
                                                kv_node_config.raft_log_filename, nullptr};
  auto kv_server = KvServer::NewKvServer({raft_config, kv_node_config.kv_dbname});
  // Register the RPC clients to it
  for (const auto &[id, conf] : config) {
    if (id == node_id) {
      continue;
    }
    kv_server->kv_peers_.insert({id, new rpc::KvServerRPCClient(conf.kv_rpc_addr, id)});
  }
  return kv_server;
}

void KvServer::Start() {
  raft_->Start();
  startApplyKvRequestCommandsThread();
}

// A server receives a request from outside world(e.g. A client or a mock
// client) and it should deal with this request properly
void KvServer::DealWithRequest(const Request *request, Response *resp) {
  if (IsDisconnected()) {
    resp->err = kRequestExecTimeout;
    return;
  }
  LOG(raft::util::kRaft, "S%d Deals with Req(From C%d) key = %s value = %s", id_, request->client_id,
      request->key.c_str(), request->value.c_str());

  resp->type = request->type;
  resp->client_id = request->client_id;
  resp->sequence = request->sequence;
  resp->raft_term = raft_->getRaftState()->CurrentTerm();
  resp->reply_server_id = id_;

  switch (request->type) {
    case kDetectLeader:
      resp->err = raft_->IsLeader() ? kOk : kNotALeader;
      LOG(raft::util::kRaft, "S%d reply DetectLeader term:%d err:%s", Id(), resp->raft_term,
          ToString(resp->err).c_str());
      return;
    case kAbort:
      resp->err = raft_->IsLeader() ? kOk : kNotALeader;
      if (raft_->IsLeader()) {
        abort();
      }
    case kPut:
    case kDelete: {
      auto size = GetRawBytesSizeForRequest(*request);
      auto data = new char[size + 12];
      RequestToRawBytes(*request, data);

      // find the start offset, it must contain the request Header and the key
      // content
      int start_offset = RequestHdrSize() + sizeof(int) + request->key.size();

      LOG(raft::util::kRaft, "S%d propose request startoffset(%d)", id_, start_offset);

      // Construct a raft command
      raft::util::Timer commit_timer;
      commit_timer.Reset();
      LOG(raft::util::kRaft, "Command Data size: %d", size);
      auto cmd = raft::CommandData{start_offset, raft::Slice(data, size)};
      
      auto pr = raft_->Propose(cmd);

      // Loop until the propose entry to be applied
      raft::util::Timer timer;
      timer.Reset();
      KvRequestApplyResult ar;
      while (timer.ElapseMilliseconds() <= 5000) {
        // Check if applied
        if (CheckEntryCommitted(pr, &ar)) {
          resp->err = ar.err;
          resp->value = ar.value;
          resp->apply_elapse_time = ar.elapse_time;
          // Calculate the time elapsed for commit
          resp->commit_elapse_time = commit_timer.ElapseMicroseconds() - resp->apply_elapse_time;
          // resp->commit_elapse_time = raft_->CommitLatency(pr.propose_index);
          LOG(raft::util::kRaft, "S%d ApplyResult value=%s", id_, resp->value.c_str());
          return;
        }
      }
      // Otherwise timesout
      resp->err = kRequestExecTimeout;
      return;
    }

    case kGet: {
      ExecuteGetOperation(request, resp);
      return;
    }
  }
}

void TruncateRequestFragments(std::string &raw_data, int target_num_shards) {
  // 1. Basic Safety Checks
  if (raw_data.size() < 12) return; // Not enough data for header

  const char *ptr = raw_data.data();
  size_t total_size = raw_data.size();
  
  // Start scanning AFTER the 12-byte header (k, m, id)
  size_t current_offset = 12;
  int shards_found = 0;

  // 2. Loop through slices to find the cut-off point
  for (int i = 0; i < target_num_shards; ++i) {
    // Check if there is enough space to read the Length Integer (4 bytes)
    if (current_offset + sizeof(int) > total_size) {
      break; 
    }

    // Read the length of the current slice
    int slice_len = *reinterpret_cast<const int *>(ptr + current_offset);

    // Calculate end of this slice (Offset + LengthHeader + DataLength)
    size_t next_offset = current_offset + sizeof(int) + slice_len;

    // Check bounds
    if (next_offset > total_size) {
      break; 
    }

    // Advance
    current_offset = next_offset;
    shards_found++;
  }

  // 3. Truncate the string
  // If target_num_shards was smaller than existing, this chops off the extra bytes.
  // If target_num_shards was larger, this does nothing (keeps all existing data).
  raw_data.resize(current_offset);
  std::printf("Updated Request in-place: New K=%d, Size=%zu\n", shards_found, raw_data.size());
}

// Check if a particular propose has been committed and set the ApplyResult
// struct if it has been committed
bool KvServer::CheckEntryCommitted(const raft::ProposeResult &pr, KvRequestApplyResult *apply) {
  // Not committed entry
  std::scoped_lock<std::mutex> lck(map_mutex_);
  if (applied_cmds_.count(pr.propose_index) == 0) {
    return false;
  }

  auto ar = applied_cmds_[pr.propose_index];
  apply->raft_term = ar.raft_term;
  if (ar.raft_term != pr.propose_term) {
    apply->err = kEntryDeleted;
    apply->value = "";
    apply->elapse_time = ar.elapse_time;
  } else {
    apply->err = ar.err;
    apply->value = ar.value;
    apply->elapse_time = ar.elapse_time;
  }
  return true;
}

std::string KvServer::IndexToKey(int index) {
    char buffer[16]; 
    std::snprintf(buffer, sizeof(buffer), "key-0%d", index);
    return std::string(buffer);
}

void KvServer::ApplyRequestCommandThread(KvServer *server) {
  raft::util::Timer elapse_timer;
  while (!server->exit_.load()) {
    raft::LogEntry ent;
    if (!server->channel_->TryPop(ent)) {
      continue;
    }
    LOG(raft::util::kRaft, "S%d Pop Ent From Raft I%d T%d", server->Id(), ent.Index(), ent.Term());
    LOG(raft::util::kRaft,"Command Size: %d \n", ent.CommandLength());
    LOG(raft::util::kRaft,"Command Size2: %d \n", ent.CommandData().size());
    elapse_timer.Reset();

    // Apply this entry to state machine(i.e. Storage Engine)

    // DEBUG: Check LogEntry's CommandData BEFORE RaftEntryToRequest
    const char* apply_cmd_data = ent.CommandData().data();
    int apply_cmd_size = ent.CommandData().size();
    int apply_start = ent.StartOffset();
    int apply_check_start = apply_start + 16;
    //printf("DEBUG [ApplyLogEntry]: BEFORE RaftEntryToRequest - CommandData size=%d, start_offset=%d, checking from byte %d\n",
          // apply_cmd_size, apply_start, apply_check_start);

           /*
    bool found_before_garbage = false;
    for (int i = apply_check_start; i < apply_cmd_size; i++) {
      if (apply_cmd_data[i] != 0) {
        if (!found_before_garbage) {
          //printf("DEBUG [ApplyLogEntry]: BEFORE conversion - FIRST non-zero byte at position %d: 0x%02X\n",
                 i, (unsigned char)apply_cmd_data[i]);
          found_before_garbage = true;
        }
      }
    }
    if (!found_before_garbage) {
      //printf("DEBUG [ApplyLogEntry]: BEFORE conversion - All bytes after start_offset+16 are zero (as expected)\n");
    }*/

    Request req;
    // RawBytesToRequest(ent.CommandData().data(), &req);
    RaftEntryToRequest(ent, &req, server->Id(), server->ClusterServerNum());

    // DEBUG: Check for garbage bytes in req.value after RaftEntryToRequest
    /*
    printf("DEBUG [ApplyLogEntry]: After RaftEntryToRequest, req.value size=%zu\n", req.value.size());
    if (req.value.size() > 16) {
      bool found_apply_garbage = false;
      for (size_t i = 17; i < req.value.size(); i++) {
        if (req.value[i] != 0) {
          if (!found_apply_garbage) {
            printf("DEBUG [ApplyLogEntry]: FIRST non-zero byte at position %zu: 0x%02X\n", i, (unsigned char)req.value[i]);
            found_apply_garbage = true;
          }
        }
      }
      if (!found_apply_garbage) {
        printf("DEBUG [ApplyLogEntry]: All bytes after position 16 are zero (as expected)\n");
      }
    } 

    std::printf("S%d Apply request(key = %s value = %s) to db\n",
            server->Id(),
            req.key.c_str(),
            req.value.size() > 16 ? req.value.c_str() + 16 : "Empty/HeaderOnly");
    std::printf("Server Threshold1 %d Threshold2 %d Flag %d \n", server->channel_->GetThreshold1(), server->channel_->GetThreshold2(), server->channel_->GetFlag());
*/
    std::string get_value;
    KvRequestApplyResult ar = {ent.Term(), kOk, std::string("")};
    switch (req.type) {
      case kPut: {
        server->db_->Put(req.key, req.value);
        ar.elapse_time = elapse_timer.ElapseMicroseconds();
        break;
      }
      case kDelete: {
        server->db_->Delete(req.key);
        ar.elapse_time = elapse_timer.ElapseMicroseconds();
        break;
      }
      // NOTE: Get will not go through this path since no raft entry will be
      // generated for Get operation
      case kGet:
      default:
        assert(0);
    }

    LOG(raft::util::kRaft, "S%d Apply request(%s) to db Done, APPLY I%d", server->Id(),
        ToString(req).c_str(), server->LastApplyIndex());

    if (server->channel_->GetFlag()) {
      // 1. Snapshot the new values atomically so they don't change while we loop
      int new_t1 = server->channel_->GetThreshold1();
      int new_t2 = server->channel_->GetThreshold2();
      bool keyNotFound = false;

      // --- PROCESS THRESHOLD 1 ---
      // Start at last + 1 so we don't re-process the previously finished index
      for (int i = server->last_threshold_1 + 1; i <= new_t1; i++) {
        std::string index = server->IndexToKey(i); // Ensure correct scope
        std::string value;
        
        std::printf("T1 Update: Looking up key %s\n", index.c_str());
        
        bool found = server->db_->Get(index, &value);
        if (found) {
          size_t old_size = value.size();
          
          // Modify 'value' in-place
          TruncateRequestFragments(value, 2);
          
          bool success = server->db_->Put(index, value); 
          
          if (!success) {
              std::cerr << "CRITICAL ERROR: Failed to write key " << index << " to DB!" << std::endl;
          } else {
              std::printf("Successfully stored %s (%zu bytes)\n", index.c_str(), value.size());
              // Update local tracker
              server->last_threshold_1 = i;
          }
        } else {
          std::printf("Key %s not found during T1 update.\n", index.c_str());
          keyNotFound = true;
          break;
        }
      }
      

      // --- PROCESS THRESHOLD 2 ---
      for (int i = server->last_threshold_2 + 1; i <= new_t2; i++) {
        std::string index = server->IndexToKey(i);
        std::string value;
        
        std::printf("T2 Update: Looking up key %s\n", index.c_str());

        bool found = server->db_->Get(index, &value);
        if (found) {
          size_t old_size = value.size();
          
          // Modify 'value' in-place
          TruncateRequestFragments(value, 1);
          
          bool success = server->db_->Put(index, value); 
          
          if (!success) {
              std::cerr << "CRITICAL ERROR: Failed to write key " << index << " to DB!" << std::endl;
          } else {
              std::printf("Successfully stored %s (%zu bytes)\n", index.c_str(), value.size());
              server->last_threshold_2 = i;
          }
        } else {
          std::printf("Key %s not found during T2 update.\n", index.c_str());
          keyNotFound= true;
          break;
        }
      }
      if (!keyNotFound){
        server->channel_->SetFlag(false); 
      }
    }
    server->applied_index_ = ent.Index();

    
    // Add the apply result into map
    std::scoped_lock<std::mutex> lck(server->map_mutex_);
    server->applied_cmds_.insert({ent.Index(), ar});
  }
}



void KvServer::ExecuteGetOperation(const Request *request, Response *resp) {
  auto read_index = this->raft_->LastIndex();
  LOG(raft::util::kRaft, "S%d Execute Get Operation, ReadIndex=%d", id_, read_index);

  resp->read_index = read_index;

  // spin until the entry has been applied
  raft::util::Timer timer;
  timer.Reset();
  while (LastApplyIndex() < read_index) {
    if (timer.ElapseMilliseconds() >= 500) {
      LOG(raft::util::kRaft, "S%d Execute Get Operation Timeout, ReadIndex=%d", id_, read_index);
      resp->err = kRequestExecTimeout;
      return;
    }
    LOG(raft::util::kRaft, "S%d Execute Get Operation(ApplyIndex:%d) ReadIndex%d", id_,
        LastApplyIndex(), read_index);

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  auto succ = db_->Get(request->key, &(resp->value));
  if (!succ) {
    resp->err = kKeyNotExist;
    return;
  }

  // The key is successfully found, however, the get value might be a fragment.
  // In such case, the leader issues GetValue RPC to all servers to retrive the
  // fragments back.
  auto format = KvServiceClient::DecodeString(&(resp->value));

  // k = 1 means this this the full entry value, simply returns Ok
  if (format.k == 1) {
    resp->err = kOk;
    return;
  }

  // Otherwise, a value gathering task should be established and executed to get
  // the full entry value.
  const std::string& raw_val = resp->value;

  // Basic validation
  if (raw_val.size() < 12) {
      LOG(raft::util::kRaft, "[S%d] Error: Local value too short", Id());
      return; 
  }

  const char* ptr = raw_val.data();

  // 2. Read 'k' and 'm' manually (Bytes 0-8)
  int k = *reinterpret_cast<const int*>(ptr);
  int m = *reinterpret_cast<const int*>(ptr + 4);
  // We ignore bytes 8-11 (the stored frag_id) because we recalculate it below

  // 3. Prepare the Input Map
  raft::Encoder::EncodingResults input;

  // 4. Parsing Loop (Identical to DoValueGatheringTask)
  size_t current_offset = 12; // Skip the 12-byte header
  int internal_index = 0;     // Start fragment counter at 0

  while (current_offset < raw_val.size()) {
      // Safety: Check if we can read the length integer
      if (current_offset + sizeof(int) > raw_val.size()) break;

      // Read the length of this specific shard
      int slice_len = *reinterpret_cast<const int*>(ptr + current_offset);
      current_offset += sizeof(int);

      // Safety: Check if we can read the payload
      if (current_offset + slice_len > raw_val.size()) break;

      // 5. CALCULATE FRAGMENT ID
      // This ensures the ID matches the global matrix row, not just the local index.
      // Formula: (ServerID * k) + internal_index
      auto frag_id = static_cast<raft::raft_frag_id_t>((Id() * k) + internal_index);

      // 6. Deep Copy & Insert
      // We CREATE a std::string to force a new memory allocation. 
      // This is critical because DoValueGatheringTask will try to 'delete[]' this pointer later.
      std::string chunk_data(ptr + current_offset, slice_len);
      input.insert({frag_id, raft::Slice(chunk_data)});

      LOG(raft::util::kRaft, "[S%d] Loaded Local Shard: GlobalID=%d (Internal=%d), Size=%d", 
          Id(), frag_id, internal_index, slice_len);

      current_offset += slice_len;
      internal_index++;
  }

  // 7. Create and Execute the Task
  // We pass 'input', which now contains safely allocated, correctly ID'd shards
  ValueGatheringTask task{request->key, resp->read_index, resp->reply_server_id, &input, k, m};
  ValueGatheringTaskResults res{&(resp->value), kOk};

  DoValueGatheringTask(&task, &res);

  std::string insert_full_entry;
  insert_full_entry.reserve(12 + sizeof(int) + res.value->size());

  // 2. Append the 12-byte Header (safely, without reinterpret_cast issues)
  int header_ints[3] = {1, 0, 0};
  insert_full_entry.append(reinterpret_cast<const char*>(header_ints), sizeof(header_ints));

  // 3. Append the Prefix Length (The size of the value)
  // (Assuming you want the length of the string as the prefix)
  int val_size = static_cast<int>(res.value->size());
  insert_full_entry.append(reinterpret_cast<const char*>(&val_size), sizeof(val_size));

  // 4. Append the Actual Value
  insert_full_entry.append(*res.value);

  // DEBUG: Check final insert_full_entry for garbage bytes after position 16
  /*
  printf("DEBUG [ExecuteGetOperation]: Final insert_full_entry size=%zu\n", insert_full_entry.size());
  if (insert_full_entry.size() > 16) {
    bool found_final_garbage = false;
    for (size_t i = 17; i < insert_full_entry.size(); i++) {
      if (insert_full_entry[i] != 0) {
        if (!found_final_garbage) {
          printf("DEBUG [ExecuteGetOperation]: FIRST non-zero byte at position %zu: 0x%02X\n",
                 i, (unsigned char)insert_full_entry[i]);
          found_final_garbage = true;
        }
      }
    }
    if (!found_final_garbage) {
      printf("DEBUG [ExecuteGetOperation]: All bytes after position 16 are zero (as expected)\n");
    }
  }
  */
  // 5. Use it
  resp->value = insert_full_entry;
  resp->err = kOk;
  db_->Put(request->key, insert_full_entry);

  return;
}

/*
void KvServer::DoValueGatheringTask(ValueGatheringTask *task, ValueGatheringTaskResults *res) {
  LOG(raft::util::kRaft, "[S%d] Start running ValueGatheringTask, k=%d, m=%d", Id(), task->k,
      task->m);
  std::atomic<bool> gather_value_done = false;

  // Use lock to prevent concurrent callback function running
  std::mutex mtx;
  auto call_back = [=, &gather_value_done, &mtx](const GetValueResponse &resp) {
    LOG(raft::util::kRaft, "[S%d] Recv GetValue Response from S%d", Id(), resp.reply_server_id);
    if (resp.err != kOk) {
      return;
    }

    std::scoped_lock<std::mutex> lck(mtx);
    auto fmt = KvServiceClient::DecodeString(const_cast<std::string *>(&resp.value));
    LOG(raft::util::kRaft, "[S%d] DecodeString: k=%d, m=%d, fragid=%d", Id(), fmt.k, fmt.m,
        fmt.frag_id);

    // Get a full entry of value
    if (fmt.k == 1 && fmt.m == 0) {
      GetKeyFromPrefixLengthFormat(fmt.frag.data(), res->value);
      res->err = kOk;
      gather_value_done.store(true);
      LOG(raft::util::kRaft, "[S%d] Get Full Entry, value=%s", Id(), res->value->c_str());
      return;
    } else {
      // Get a fragment of value
      if (fmt.k == task->k && fmt.m == task->m) {
        task->decode_input->insert({fmt.frag_id, raft::Slice::Copy(fmt.frag)});
        LOG(raft::util::kRaft, "[S%d] Add Fragment%d in ValueGatheringTask", Id(), fmt.frag_id);
      }

      // The gather value task is not done, and there is enough fragments to
      // decode the entry
      if (!gather_value_done.load() && task->decode_input->size() >= task->k) {
        raft::Encoder encoder;
        raft::Slice results;
        auto stat = encoder.DecodeSlice(*(task->decode_input), task->k, task->m, &results);
        if (stat) {
          GetKeyFromPrefixLengthFormat(results.data(), res->value);
          res->err = kOk;
          gather_value_done.store(true);
          LOG(raft::util::kRaft, "[S%d] Decode Value Succ", Id());
        } else {
          res->err = kKVDecodeFail;
          LOG(raft::util::kRaft, "[S%d] Decode Value Fail", Id());
        }
      }
    }
  };

  auto clear_gather_ctx = [=]() {
    for (auto &[_, frag] : *(task->decode_input)) {
      delete[] frag.data();
    }
  };

  auto get_req = GetValueRequest{task->key, task->read_index};
  for (auto &[id, server] : kv_peers_) {
    if (id == task->replied_id) {
      continue;
    }
    auto stub = reinterpret_cast<rpc::KvServerRPCClient *>(server);
    stub->SetRPCTimeOutMs(1000);
    auto resp = stub->GetValue(get_req);
    if (resp.err == kOk) {
      call_back(resp);
    }
    if (gather_value_done.load()) {
      break;
    }
    // Send in an async way
    // stub->GetValue(get_req, call_back);
  }

  raft::util::Timer timer;
  timer.Reset();
  while (timer.ElapseMilliseconds() <= 1000) {
    if (gather_value_done.load() == true) {
      clear_gather_ctx();
      return;
    } else {
      // sleepMs(100);
    }
  }
  //  Set the error code
  if (res->err == kOk) {
    res->err = kRequestExecTimeout;
  }
  clear_gather_ctx();
}*/

void KvServer::DoValueGatheringTask(ValueGatheringTask *task, ValueGatheringTaskResults *res) {

  LOG(raft::util::kRaft, "[S%d] Start running ValueGatheringTask, k=%d, m=%d", Id(), task->k,
      task->m);
  std::atomic<bool> gather_value_done = false;

  // Use lock to prevent concurrent callback function running
  std::mutex mtx;

  // Store responses to keep them alive during processing
  std::vector<GetValueResponse> responses;

  // --------------------------------------------------------------------------
  // Callback: Handles responses containing potentially MULTIPLE shards
  // --------------------------------------------------------------------------
  auto call_back = [=, &gather_value_done, &mtx](const GetValueResponse &resp) {
    LOG(raft::util::kRaft, "[S%d] Recv GetValue Response from S%d", Id(), resp.reply_server_id);
    if (resp.err != kOk) {
      return;
    }

    std::scoped_lock<std::mutex> lck(mtx);

    // 1. Fast Exit if already finished
    if (gather_value_done.load()) {
      return;
    }

    // 2. Parse the Raw Response
    const std::string& raw_val = resp.value;
    
    // Basic header validation (4 bytes k + 4 bytes m + 4 bytes padding/other = 12)
    if (raw_val.size() < 12) {
         LOG(raft::util::kRaft, "[S%d] Error: Response too short from S%d", Id(), resp.reply_server_id);
         return;
    }

    const char* ptr = raw_val.data();
    size_t current_offset = 12; // Skip the 12-byte header
    int internal_index = 0;     // Start fragment counting at 0

    // Loop through the buffer to extract ALL shards stored in this response
    while (current_offset < raw_val.size()) {
        if (current_offset + sizeof(int) > raw_val.size()) break;
        
        // Read slice length
        int slice_len = *reinterpret_cast<const int*>(ptr + current_offset);
        current_offset += sizeof(int);
        
        if (current_offset + slice_len > raw_val.size()) break;

        // Construct Fragment ID: internal index + server ID offset
        auto frag_id = static_cast<raft::raft_frag_id_t>(internal_index + (resp.reply_server_id*task->k));
        LOG(raft::util::kRaft, 
            "[S%d] Parsing S%d: internal_idx=%d, k=%d -> Assigned FragID=%d", 
            Id(), resp.reply_server_id, internal_index, task->k, frag_id);

        // Store if new
        if (task->decode_input->find(frag_id) == task->decode_input->end()) {
            // Allocate persistent memory for this fragment
            char* persistent_data = new char[slice_len];
            std::memcpy(persistent_data, ptr + current_offset, slice_len);

            // Create Slice from the persistent buffer
            task->decode_input->insert({frag_id, raft::Slice(persistent_data, slice_len)});        
            LOG(raft::util::kRaft, "[S%d] Add Fragment%d in ValueGatheringTask with size %d", Id(), frag_id, slice_len);
        }

        current_offset += slice_len;
        internal_index++;
    }

    // 3. Check for Reconstruction
    // We use task->k because the encoding scheme is fixed/known
    if (!gather_value_done.load() && static_cast<int>(task->decode_input->size()) >= task->k) {
        raft::Encoder encoder;
        raft::Slice results;
        
        // Attempt decode with the fixed k/m parameters
        auto stat = encoder.DecodeSlice(*(task->decode_input), task->k, 45, &results);
        
        if (stat) {
          // DEBUG: Show raw decoded buffer
          printf("DEBUG [DoValueGatheringTask]: Decode SUCCESS, results.size()=%zu\n", results.size());

          // 1. Point to the raw decoded buffer
          const char* ptr = results.data();

          // 2. Read the embedded length (The "True" Length)
          // Your format is: [4-byte Length] [Actual String Data]
          int true_data_len = *reinterpret_cast<const int*>(ptr);
          // 3. Safety Check: Ensure the length is sane
          // It must be positive and fit within the decoded buffer (minus the 4-byte header)
          if (true_data_len > 0 && static_cast<size_t>(true_data_len) <= results.size() - sizeof(int)) {

              // 4. Extract ONLY the valid bytes (skipping the 4-byte header)
              res->value->assign(ptr + sizeof(int), true_data_len);

              res->err = kOk;
              gather_value_done.store(true);

              // DEBUG: Check decoded value for garbage bytes after position 16
              printf("DEBUG [DoValueGatheringTask]: After decode, value size=%zu\n", res->value->size());
              if (res->value->size() > 16) {
                bool found_decode_garbage = false;
                for (size_t i = 16; i < res->value->size(); i++) {
                  if ((*res->value)[i] != 0) {
                    if (!found_decode_garbage) {
                      printf("DEBUG [DoValueGatheringTask]: FIRST non-zero byte at position %zu: 0x%02X\n",
                             i, (unsigned char)(*res->value)[i]);
                      found_decode_garbage = true;
                    }
                  }
                }
                if (!found_decode_garbage) {
                  printf("DEBUG [DoValueGatheringTask]: All bytes after position 16 are zero (as expected)\n");
                }
              }

              // (Optional) Debug Log showing clean vs raw
              LOG(raft::util::kRaft, "Successfully Decoded");

          } else {
              // Handle corruption (Header says length is 5000 but buffer is only 10 bytes)
              res->err = kKVDecodeFail; 
              LOG(raft::util::kRaft, "[S%d] Decode Success but Header Invalid (Len: %d, Buff: %d)", 
                  Id(), true_data_len, results.size());
          }
        }
    }
  };

  // Helper to clean up memory
  auto clear_gather_ctx = [=]() {
    for (auto &[_, frag] : *(task->decode_input)) {
      delete[] frag.data();
    }
  };

  // --------------------------------------------------------------------------
  // Scatter: Send Requests to All Peers
  // --------------------------------------------------------------------------
  auto get_req = GetValueRequest{task->key, task->read_index};
  for (auto &[id, server] : kv_peers_) {
    // Skip the node that triggered this task (if applicable)
    if (id == task->replied_id) {
      continue;
    }
    
    auto stub = reinterpret_cast<rpc::KvServerRPCClient *>(server);
    stub->SetRPCTimeOutMs(1000);
    
    // Synchronous call
    auto resp = stub->GetValue(get_req);
    if (resp.err == kOk) {
      // Store the response to keep it alive
      responses.push_back(resp);
      // Pass the stored response (which won't go out of scope)
      call_back(responses.back());
    }
    
    // Optimization: Stop asking peers if we successfully decoded the value
    if (gather_value_done.load()) {
      break;
    }
  }

  // --------------------------------------------------------------------------
  // Timeout / Wait Logic
  // --------------------------------------------------------------------------
  raft::util::Timer timer;
  timer.Reset();
  while (timer.ElapseMilliseconds() <= 1000) {
    if (gather_value_done.load() == true) {
      clear_gather_ctx();
      return;
    } else {
      // sleepMs(100);
    }
  }
  
  // Set the error code if we timed out
  if (res->err == kOk) {
    res->err = kRequestExecTimeout;
  }
  clear_gather_ctx();
}
}  // namespace kv
