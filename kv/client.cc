#include "client.h"

#include "config.h"
#include "encoder.h"
#include "kv_format.h"
#include "type.h"
#include "util.h"
namespace kv {
KvServiceClient::KvServiceClient(const KvClusterConfig &config, uint32_t client_id)
    : client_id_(client_id) {
  for (const auto &[id, conf] : config) {
    servers_.insert({id, new rpc::KvServerRPCClient(conf.kv_rpc_addr, id)});
  }
  curr_leader_ = kNoDetectLeader;
  curr_leader_term_ = 0;
}

KvServiceClient::~KvServiceClient() {
  for (auto [id, ptr] : servers_) {
    delete ptr;
  }
}

Response KvServiceClient::WaitUntilRequestDone(const Request &request) {
  raft::util::Timer timer;
  timer.Reset();
  LOG(raft::util::kRaft, "[C%d] Dealing With Req (%s)", ClientId(), ToString(request).c_str());
  while (timer.ElapseMilliseconds() < kKVRequestTimesoutCnt * 1000) {
    if (curr_leader_ == kNoDetectLeader && DetectCurrentLeader() == kNoDetectLeader) {
      LOG(raft::util::kRaft, "Detect No Leader");
      sleepMs(300);
      continue;
    }
    LOG(raft::util::kRaft, "[C%d] Send Req (key = %s value = %s) to S%d", ClientId(), request.key.c_str(), request.value.c_str(),
        curr_leader_);
    auto resp = GetRPCStub(curr_leader_)->DealWithRequest(request);
    switch (resp.err) {
      case kOk:
      case kKeyNotExist:
        return resp;

      case kEntryDeleted:
      // The leader might be separated from the cluster
      case kRequestExecTimeout:
      case kNotALeader:
      case kRPCCallFailed:
        LOG(raft::util::kRaft, "[C%d] Recv Resp(err=%s), Fallback to Nonleader", ClientId(),
            ToString(resp.err).c_str());
        curr_leader_ = kNoDetectLeader;
        curr_leader_term_ = 0;
        break;
        // default:
        // assert(false);
    }

    // The abort command only executes once
    if (request.type == kAbort) {
      return resp;
    }
  }
  // Timeout
  Response resp;
  resp.err = kRequestExecTimeout;
  return resp;
}

OperationResults KvServiceClient::Put(const std::string &key, const std::string &value) {
  Request request = {kPut, ClientId(), 0, key, value};
  auto resp = WaitUntilRequestDone(request);
  return OperationResults{resp.err, resp.apply_elapse_time, resp.commit_elapse_time};
}

OperationResults KvServiceClient::Abort() {
  Request request = {kAbort, ClientId(), 0, "", ""};
  auto resp = WaitUntilRequestDone(request);
  return OperationResults{resp.err, 0, 0};
}

OperationResults KvServiceClient::Get(const std::string &key, std::string *value) {
  Request request = {kGet, ClientId(), 0, key, std::string("")};
  auto resp = WaitUntilRequestDone(request);

  LOG(raft::util::kRaft, "[C%d] Recv Resp from S%d", ClientId(), resp.reply_server_id);

  if (resp.err != kOk) {
    return OperationResults{resp.err, resp.apply_elapse_time};
  }

    LOG(raft::util::kRaft, "Client Value %s\n", resp.value.c_str()+16);


  // Decoding the response byte array for further information: we may need to
  // collect other fragments
  auto format = DecodeString(&resp.value);


  if (format.k == 1) {
    GetKeyFromPrefixLengthFormat(format.frag.data(), value);
    LOG(raft::util::kRaft, "Client Value %s\n", (*value).c_str());
    return OperationResults{kOk, 0};
  }

  LOG(raft::util::kRaft, "[Get Partial Value: k=%d m=%d readindex=%d], start collecting", format.k,
      format.m, resp.read_index);

  // Initiate a GatherValue task
  int k = format.k, m = format.m;
  raft::Encoder::EncodingResults input;
  input.insert({format.frag_id, raft::Slice::Copy(format.frag)});
  LOG(raft::util::kRaft, "[C%d] Add Fragment of Frag%d from S%d", ClientId(), format.frag_id,
      resp.reply_server_id);

  GatherValueTask task{key, resp.read_index, resp.reply_server_id, &input, k, m};
  GatherValueTaskResults res{value, kOk};

  DoGatherValueTask(&task, &res);
  return OperationResults{res.err, 0};
}

OperationResults KvServiceClient::Delete(const std::string &key) {
  Request request = {kDelete, ClientId(), 0, key, ""};
  auto resp = WaitUntilRequestDone(request);
  return OperationResults{resp.err, resp.apply_elapse_time};
}

raft::raft_node_id_t KvServiceClient::DetectCurrentLeader() {
  for (auto &[id, stub] : servers_) {
    if (stub == nullptr) {
      continue;
    }
    Request detect_request = {kDetectLeader, ClientId(), 0, "", ""};
    auto resp = GetRPCStub(id)->DealWithRequest(detect_request);
    if (resp.err == kOk) {
      if (resp.raft_term > curr_leader_term_) {
        curr_leader_ = id;
        curr_leader_term_ = resp.raft_term;
      }
    }
  }
  return curr_leader_;
}

/*
void KvServiceClient::DoGatherValueTask(const GatherValueTask *task, GatherValueTaskResults *res) {
  LOG(raft::util::kRaft, "[C%d] Start running Gather Value Task, k=%d, m=%d", ClientId(), task->k,
      task->m);
  std::atomic<bool> gather_value_done = false;

  // Use lock to prevent concurrent callback function running
  std::mutex mtx;

  auto call_back = [=, &gather_value_done, &mtx](const GetValueResponse &resp) {
    LOG(raft::util::kRaft, "[C%d] Recv GetValue Response from S%d", ClientId(),
        resp.reply_server_id);
    if (resp.err != kOk) {
      return;
    }
    std::scoped_lock<std::mutex> lck(mtx);

    auto fmt = DecodeString(const_cast<std::string *>(&resp.value));
    LOG(raft::util::kRaft, "[C%d] Decode Value: k=%d, m=%d, fragid=%d", ClientId(), fmt.k, fmt.m,
        fmt.frag_id)

    // This is an full entry
    if (fmt.k == 1 && fmt.m == 0) {
      GetKeyFromPrefixLengthFormat(fmt.frag.data(), res->value);
      res->err = kOk;
      gather_value_done.store(true);
      LOG(raft::util::kRaft, "[C%d] Get Full Entry, value=%s", ClientId(), res->value->c_str());
      return;
    } else {
      // Collect a fragment
      if (fmt.k == task->k && fmt.m == task->m) {
        task->decode_input->insert({fmt.frag_id, raft::Slice::Copy(fmt.frag)});
        LOG(raft::util::kRaft, "[C%d] Add Fragment%d", ClientId(), fmt.frag_id);
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
          LOG(raft::util::kRaft, "[C%d] Decode Value Succ", ClientId());
        } else {
          res->err = kKVDecodeFail;
          LOG(raft::util::kRaft, "[C%d] Decode Value Fail", ClientId());
        }
      }
    }
  };

  auto clear_gather_ctx = [=]() {
    for (auto &[_, frag] : *(task->decode_input)) {
      delete[] frag.data();
    }
  };

  // issues parallel GetValue RPC to all nodes and decode the value when
  // receiving at least F response
  auto get_req = GetValueRequest{task->key, task->read_index};
  for (auto &[id, server] : servers_) {
    if (id != task->replied_id) {
      // GetRPCStub(id)->GetValue(get_req, call_back);
      GetRPCStub(id)->SetRPCTimeOutMs(1000);
      // Sync call for simplicity
      auto resp = GetRPCStub(id)->GetValue(get_req);
      if (resp.err == kOk) {
        call_back(resp);
      }
    }
  }

  raft::util::Timer timer;
  timer.Reset();
  while (timer.ElapseMilliseconds() <= 1000) {
    if (gather_value_done.load() == true) {
      clear_gather_ctx();
      return;
    } else {
      sleepMs(100);
    }
  }
  //  Set the error code
  if (res->err == kOk) {
    res->err = kRequestExecTimeout;
  }
  clear_gather_ctx();
}*/

void KvServiceClient::DoGatherValueTask(const GatherValueTask *task, GatherValueTaskResults *res) {
  LOG(raft::util::kRaft, "[C%d] Start Gather Value (Fixed Encoding): k=%d, m=%d", ClientId(), task->k, task->m);

  std::atomic<bool> gather_value_done = false;
  std::mutex mtx;

  // --------------------------------------------------------------------------
  // Callback: Handles responses containing potentially MULTIPLE shards
  // --------------------------------------------------------------------------
  auto call_back = [=, &gather_value_done, &mtx](const GetValueResponse &resp) {
    if (resp.err != kOk) {
      return;
    }

    std::scoped_lock<std::mutex> lck(mtx);

    // 1. Fast Exit if already finished
    if (gather_value_done.load()) {
        return;
    }

    // 2. Parse the Raw Response (Logic from CollectShardsFromValue)
    const std::string& raw_val = resp.value;
    
    // Basic header validation (4 bytes k + 4 bytes m + 4 bytes padding/other = 12)
    if (raw_val.size() < 12) {
        LOG(raft::util::kRaft, "[C%d] Error: Response too short from S%d", ClientId(), resp.reply_server_id);
        return;
    }

    const char* ptr = raw_val.data();
    size_t current_offset = 12; // Skip the 12-byte header

    // Internal index within this server's shards (starts at 0)
    int internal_index = 0;

    // Loop through the buffer to extract ALL shards stored in this response
    while (current_offset < raw_val.size()) {
        if (current_offset + sizeof(int) > raw_val.size()) break;

        // Read slice length
        int slice_len = *reinterpret_cast<const int*>(ptr + current_offset);
        current_offset += sizeof(int);

        if (current_offset + slice_len > raw_val.size()) break;

        // Construct Fragment ID
        // Fragment ID = server_id * k + internal_index
        // This matches the encoding: server i gets fragments [i*k, i*k+1, ..., i*k+(k-1)]
        auto frag_id = static_cast<raft::raft_frag_id_t>(resp.reply_server_id * task->k + internal_index);

        // Store if new
        if (task->decode_input->find(frag_id) == task->decode_input->end()) {
            // Create a safe copy of the data
            std::string chunk_data(ptr + current_offset, slice_len);

            // Insert into the map (Assuming Slice manages its own memory or copies)
            task->decode_input->insert({frag_id, raft::Slice(chunk_data)});

            LOG(raft::util::kRaft, "[C%d] Collected Frag %d (server %d, shard %d) from S%d",
                ClientId(), frag_id, resp.reply_server_id, internal_index, resp.reply_server_id);
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
      auto stat = encoder.DecodeSlice(*(task->decode_input), task->k, task->m, &results);
      
      if (stat) {
        // Success
        GetKeyFromPrefixLengthFormat(results.data(), res->value);
        res->err = kOk;
        gather_value_done.store(true);
        LOG(raft::util::kRaft, "[C%d] Decode Success! Reconstructed Value Size: %d", ClientId(), results.size());
      } else {
        // We have enough shards but decode failed (mismatched parity/corruption)
        res->err = kKVDecodeFail;
        LOG(raft::util::kRaft, "[C%d] Decode Failed despite having %d shards", ClientId(), task->decode_input->size());
      }
    }
  };

  // Helper to clean up memory
  auto clear_gather_ctx = [=]() {
    for (auto &[_, frag] : *(task->decode_input)) {
        // Depending on your Slice implementation, you might need to free the buffer
        // delete[] frag.data(); 
    }
  };

  // --------------------------------------------------------------------------
  // Scatter: Send Requests to All Nodes
  // --------------------------------------------------------------------------
  auto get_req = GetValueRequest{task->key, task->read_index};
  
  for (auto &[id, server] : servers_) {
    // Optimization: Skip the node we already got a reply from (if passed in task)
    if (id != task->replied_id) {
      GetRPCStub(id)->SetRPCTimeOutMs(1000);
      
      // Synchronous call (as per your original code structure)
      auto resp = GetRPCStub(id)->GetValue(get_req);
      if (resp.err == kOk) {
        call_back(resp);
      }
      
      // Optimization: If we finished mid-loop, stop querying other servers
      if (gather_value_done.load()) break;
    }
  }

  // --------------------------------------------------------------------------
  // Timeout / Wait Logic
  // --------------------------------------------------------------------------
  raft::util::Timer timer;
  timer.Reset();
  
  // Wait up to 1 second if still running (relevant if using async calls)
  while (timer.ElapseMilliseconds() <= 1000) {
    if (gather_value_done.load() == true) {
      clear_gather_ctx();
      return;
    } else {
      sleepMs(100);
    }
  }

  // Handle Timeout
  if (res->err == kOk) { 
    // If err is still kOk but we are here, it means we never set it to 'done'
    res->err = kRequestExecTimeout;
  }
  
  clear_gather_ctx();
}

}  // namespace kv
