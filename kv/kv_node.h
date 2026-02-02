#pragma once
#include <chrono>
#include <thread>
#include "config.h"
#include "kv_server.h"
#include "raft_type.h"
#include "rpc.h"
#include "util.h"
namespace kv {

// A KvServiceNode is basically an adapter that combines the KvServer and
// RPC server
class KvServiceNode {
 public:
  static KvServiceNode *NewKvServiceNode(const KvClusterConfig &config, raft::raft_node_id_t id);
  KvServiceNode() = default;
  ~KvServiceNode();

  KvServiceNode(const KvClusterConfig &config, raft::raft_node_id_t id);
  void InitServiceNodeState();
  void StartServiceNode();
  void StopServiceNode();

  // This is for debug and test
  void Disconnect() {
    LOG(raft::util::kRaft, "S%d Disconnect", id_);
    rpc_server_->Stop();
    kv_server_->Disconnect();
  }

  void Reconnect() {
    LOG(raft::util::kRaft, "S%d Reconnect", id_);
    kv_server_->Reconnect();
    // Stop first in case it's already running, then restart
    rpc_server_->Stop();
    // Wait for the OS to release the port before restarting
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    rpc_server_->Start();
  }

  bool IsDisconnected() const { return kv_server_->IsDisconnected(); }

  bool IsLeader() const { return kv_server_->IsLeader(); }

 private:
  KvClusterConfig config_;
  raft::raft_node_id_t id_;
  KvServer *kv_server_;
  rpc::KvServerRPCServer *rpc_server_;
};
}  // namespace kv
