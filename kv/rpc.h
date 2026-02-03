#pragma once
#include <functional>
#include <thread>
#include <chrono>

#include "RCF/ClientStub.hpp"
#include "RCF/Exception.hpp"
#include "RCF/Future.hpp"
#include "RCF/InitDeinit.hpp"
#include "RCF/RCF.hpp"
#include "RCF/RcfFwd.hpp"
#include "RCF/RcfMethodGen.hpp"
#include "RCF/RcfServer.hpp"
#include "RCF/TcpEndpoint.hpp"
#include "kv_server.h"
#include "raft_type.h"
#include "type.h"
#include "util.h"
namespace kv {
class KvServer;
namespace rpc {

// Define the RPC return value and parameter
RCF_BEGIN(I_KvServerRPCService, "I_KvServerRPCService")
RCF_METHOD_R1(Response, DealWithRequest, const Request &)
RCF_METHOD_R1(GetValueResponse, GetValue, const GetValueRequest &)
RCF_END(I_KvServerRPCService)

class KvServerRPCService {
 public:
  KvServerRPCService() = default;
  KvServerRPCService(KvServer *server) : server_(server) {}
  Response DealWithRequest(const Request &req) {
    Response resp;
    server_->DealWithRequest(&req, &resp);
    return resp;
  }

  GetValueResponse GetValue(const GetValueRequest &request) {
    LOG(raft::util::kRaft, "S%d recv GetValue Request: readIndex=%d", server_->Id(),
        request.read_index);
    raft::util::Timer timer;
    timer.Reset();
    // Spin until the entries before read index have been applied into the DB
    // NOTE: Added timeout to prevent infinite spin which can cause use-after-free
    // when RCF session times out while we're still waiting
    while (server_->LastApplyIndex() < request.read_index) {
      if (timer.ElapseMilliseconds() >= 3000) {
        LOG(raft::util::kRaft, "S%d GetValue timeout waiting for readIndex=%d", server_->Id(),
            request.read_index);
        return GetValueResponse{std::string(""), kRequestExecTimeout, server_->Id()};
      }
      std::this_thread::yield();
    }
    std::string value;
    auto found = server_->DB()->Get(request.key, &value);
    LOG(raft::util::kRaft, "S%d make GetValue Response", server_->Id());
    if (found) {
      return GetValueResponse{std::move(value), kOk, server_->Id()};
    } else {
      return GetValueResponse{std::string(""), kKeyNotExist, server_->Id()};
    }
  }

  void SetKvServer(KvServer *server) { server_ = server; }

 private:
  KvServer *server_;
};

// RPC client issues a DealWithRequest RPC to specified KvNode by
// simply call "DealWithRequest()". The call is synchronized and might be
// blocked. We need a timeout to solve this problem.
//
// Each KvServerRPCClient object responds to a KvNode
class KvServerRPCClient {
 public:
  using ClientPtr = std::shared_ptr<RcfClient<I_KvServerRPCService>>;
  KvServerRPCClient(const NetAddress &net_addr, raft::raft_node_id_t id)
    : address_(net_addr),
      id_(id),
      client_stub_(RCF::TcpEndpoint(net_addr.ip, net_addr.port)),
      rcf_init_() {
  client_stub_.getClientStub().getTransport().setMaxIncomingMessageLength(
      raft::rpc::config::kMaxMessageLength);
  client_stub_.getClientStub().getTransport().setMaxOutgoingMessageLength(
      raft::rpc::config::kMaxMessageLength);
  // ADD THIS: Set timeout slightly longer than server's 5000ms wait
  client_stub_.getClientStub().setRemoteCallTimeoutMs(6000);
}

  Response DealWithRequest(const Request &request);

  void GetValue(const GetValueRequest &request, std::function<void(const GetValueResponse &)> cb);

  void onGetValueComplete(RCF::Future<GetValueResponse> ret,
                          std::function<void(const GetValueResponse &)> cb);

  GetValueResponse GetValue(const GetValueRequest &request);

  // Set timeout for this RPC call, a typical value might be 300ms?
  void SetRPCTimeOutMs(int cnt) { client_stub_.getClientStub().setRemoteCallTimeoutMs(cnt); }

 private:
  RCF::RcfInit rcf_init_;
  NetAddress address_;
  raft::raft_node_id_t id_;
  RcfClient<I_KvServerRPCService> client_stub_;
};

// Server side of a KvNode, the server calls Start() to continue receive
// RPC request from client and deal with it.
class KvServerRPCServer {
 public:
  KvServerRPCServer(const NetAddress &net_addr, raft::raft_node_id_t id, KvServerRPCService service)
      : address_(net_addr),
        id_(id),
        server_(RCF::TcpEndpoint(net_addr.ip, net_addr.port)),
        service_(service) {
    LOG(raft::util::kRaft, "S%d RPC init with (ip=%s port=%d)", id_, net_addr.ip.c_str(),
        net_addr.port);
  }
  KvServerRPCServer() = default;

  void Start() {
    printf("[DEBUG] S%d KvServerRPC: Attempting to bind to %s:%d\n",
           id_, address_.ip.c_str(), address_.port);
    fflush(stdout);

    server_.getServerTransport().setMaxIncomingMessageLength(raft::rpc::config::kMaxMessageLength);
    server_.bind<I_KvServerRPCService>(service_);

    // Retry binding a few times in case of transient port conflicts
    const int max_retries = 3;
    for (int attempt = 1; attempt <= max_retries; ++attempt) {
      try {
        server_.start();
        printf("[DEBUG] S%d KvServerRPC: Successfully started on port %d\n", id_, address_.port);
        fflush(stdout);
        return;  // Success
      } catch (const RCF::Exception& e) {
        printf("[ERROR] S%d KvServerRPC: Failed to start on port %d (attempt %d/%d): %s\n",
               id_, address_.port, attempt, max_retries, e.getErrorMessage().c_str());
        fflush(stdout);
        if (attempt < max_retries) {
          printf("[DEBUG] S%d Retrying in 2 seconds...\n", id_);
          fflush(stdout);
          std::this_thread::sleep_for(std::chrono::seconds(2));
        } else {
          printf("[FATAL] S%d KvServerRPC: Cannot bind to port %d after %d attempts. Exiting.\n",
                 id_, address_.port, max_retries);
          fflush(stdout);
          std::exit(1);  // Exit cleanly instead of throwing uncaught exception
        }
      }
    }
  }

  void Stop() {
    LOG(raft::util::kRaft, "S%d stop RPC server", id_);
    server_.stop();
  }

  void SetServiceContext(KvServer *server) { service_.SetKvServer(server); }

 private:
  RCF::RcfInit rcf_init_;
  NetAddress address_;
  raft::raft_node_id_t id_;
  RCF::RcfServer server_;
  KvServerRPCService service_;
};

}  // namespace rpc
}  // namespace kv
