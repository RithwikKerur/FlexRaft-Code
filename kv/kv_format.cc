#include "kv_format.h"

#include <cstddef>
#include <cstring>

#include "raft_type.h"
#include "type.h"
#include "util.h"
#include <cstdio>
#include "log_entry.h"

namespace kv {
size_t GetRawBytesSizeForRequest(const Request &request) {
  size_t hdr_size = RequestHdrSize();
  size_t key_size = sizeof(int) + request.key.size();
  size_t val_size = sizeof(int) + request.value.size();
  printf("hrd_size %d  key_size %d  val_size %d", hdr_size, key_size, val_size);
  return hdr_size + key_size + val_size;
}

void RequestToRawBytes(const Request &request, char *bytes) {
  std::memcpy(bytes, &request, RequestHdrSize());
  bytes = MakePrefixLengthKey(request.key, bytes + RequestHdrSize());
  MakePrefixLengthKey(request.value, bytes);
  printf("bytes %s", bytes);
}

void RawBytesToRequest(char *bytes, Request *request) {
  std::memcpy(request, bytes, RequestHdrSize());
  bytes = GetKeyFromPrefixLengthFormat(bytes + RequestHdrSize(), &(request->key));
  GetKeyFromPrefixLengthFormat(bytes, &(request->value));
}

void RaftEntryToRequest(const raft::LogEntry &ent, Request *request, raft::raft_node_id_t server_id,
                        int server_num) {
  if (ent.Type() == raft::kNormal) {
    auto bytes = ent.CommandData().data();
    std::memcpy(request, bytes, RequestHdrSize());

    bytes = GetKeyFromPrefixLengthFormat(bytes + RequestHdrSize(), &(request->key));

    char tmp_data[12];
    *reinterpret_cast<int *>(tmp_data) = 1;
    *reinterpret_cast<int *>(tmp_data + 4) = 0;
    *reinterpret_cast<int *>(tmp_data + 8) = 0;

    for (int i = 0; i < 12; ++i) {
      request->value.push_back(tmp_data[i]);
    }

    std::printf( "RaftEnt To Request: k=%d,m=%d,frag_id=%d", 1, 0, 0);

    // value would be the prefix length key format
    auto remaining_size = ent.CommandData().size() - (bytes - ent.CommandData().data());
    request->value.append(bytes, remaining_size);
  } else {
    // construct the header and key
    std::memcpy(request, ent.NotEncodedSlice().data(), RequestHdrSize());
    auto key_data = ent.NotEncodedSlice().data() + RequestHdrSize();
    GetKeyFromPrefixLengthFormat(key_data, &(request->key));

    std::printf("Server key data %s\n\n", request->key.c_str());

    // Construct the value, in the following format:
    // k, m, fragment_id, value_contents
    request->value.reserve(sizeof(int) * 3 + ent.GetFragmentsSize());

    char tmp_data[12];
    int k = ent.GetChunkInfo().GetK();
    int m = server_num - k;
    *reinterpret_cast<int *>(tmp_data) = k;
    *reinterpret_cast<int *>(tmp_data + 4) = m;
    *reinterpret_cast<int *>(tmp_data + 8) = static_cast<int>(server_id);

    std::printf(" Encoded RaftEnt To Request: k=%d,m=%d,frag_id=%d\n", k, m, server_id);

    for (int i = 0; i < 12; ++i) {
      request->value.push_back(tmp_data[i]);
    }


    // Append the value contents
    auto fragment_slices = ent.FragmentSlice();
    std::printf("Fragments %d\n", fragment_slices.size());

    for (const auto& slice : fragment_slices) {
      int len = static_cast<int>(slice.size());
      request->value.append(reinterpret_cast<const char*>(&len), sizeof(int));
      request->value.append(slice.data(), slice.size());
    }
    std::printf("Fragment Size %d \n", ent.GetFragmentsSize());

  }
}

void RaftEntryToRequest(const raft::LogEntry &ent, Request *request, raft::raft_node_id_t server_id,
                        int server_num, int numShards) {
    // construct the header and key
    std::memcpy(request, ent.NotEncodedSlice().data(), RequestHdrSize());
    auto key_data = ent.NotEncodedSlice().data() + RequestHdrSize();
    GetKeyFromPrefixLengthFormat(key_data, &(request->key));

    // Construct the value, in the following format:
    // k, m, fragment_id, value_contents
    request->value.reserve(sizeof(int) * 3 + ent.GetFragmentsSize());

    char tmp_data[12];
    int k = ent.GetChunkInfo().GetK();
    int m = server_num - k;
    *reinterpret_cast<int *>(tmp_data) = k;
    *reinterpret_cast<int *>(tmp_data + 4) = m;
    *reinterpret_cast<int *>(tmp_data + 8) = static_cast<int>(server_id);

    std::printf(" Encoded RaftEnt To Request: k=%d,m=%d,frag_id=%d\n", k, m, server_id);

    for (int i = 0; i < 12; ++i) {
      request->value.push_back(tmp_data[i]);
    }

    // Append the value contents
    const auto& slices = ent.FragmentSlice();
    size_t limit = std::min(slices.size(), static_cast<size_t>(numShards));
    std::printf("Reducing shard amount for %s to %d\n", request->key.c_str(), limit);
    
    for (size_t i = 0; i < limit; ++i) {
      const auto& slice = slices[i];
      int len = static_cast<int>(slice.size());
      request->value.append(reinterpret_cast<const char*>(&len), sizeof(int));

      // B. Write the Data
      request->value.append(slice.data(), len);
    }

}



void RaftEntryToRequest(const raft::LogEntry &ent, Request *request) {
  if (ent.Type() == raft::kNormal) {
    auto bytes = ent.CommandData().data();
    std::memcpy(request, bytes, RequestHdrSize());

    bytes = GetKeyFromPrefixLengthFormat(bytes + RequestHdrSize(), &(request->key));

    char tmp_data[12];
    *reinterpret_cast<int *>(tmp_data) = 1;
    *reinterpret_cast<int *>(tmp_data + 4) = 0;
    *reinterpret_cast<int *>(tmp_data + 8) = 0;

    for (int i = 0; i < 12; ++i) {
      request->value.push_back(tmp_data[i]);
    }

    std::printf( "RaftEnt To Request: k=%d,m=%d,frag_id=%d", 1, 0, 0);

    // value would be the prefix length key format
    auto remaining_size = ent.CommandData().size() - (bytes - ent.CommandData().data());
    request->value.append(bytes, remaining_size);
  } else {
    // construct the header and key
    std::memcpy(request, ent.NotEncodedSlice().data(), RequestHdrSize());
    auto key_data = ent.NotEncodedSlice().data() + RequestHdrSize();
    GetKeyFromPrefixLengthFormat(key_data, &(request->key));

    // Construct the value, in the following format:
    // k, m, fragment_id, value_contents
    request->value.reserve(sizeof(int) * 3 + ent.GetFragmentsSize());

    char tmp_data[12];
    // *reinterpret_cast<int *>(tmp_data) = ent.GetVersion().GetK();
    // *reinterpret_cast<int *>(tmp_data + 4) = ent.GetVersion().GetM();
    // *reinterpret_cast<int *>(tmp_data + 8) =
    // ent.GetVersion().GetFragmentId();
    //
    // LOG(raft::util::kRaft, "RaftEnt To Request: k=%d,m=%d,frag_id=%d",
    //     ent.GetVersion().GetK(), ent.GetVersion().GetM(),
    //     ent.GetVersion().GetFragmentId());

    std::printf(" Encoded RaftEnt To Request: ");

    for (int i = 0; i < 12; ++i) {
      request->value.push_back(tmp_data[i]);
    }

    // Append the value contents
    for (const auto& slice : ent.FragmentSlice()) {
      request->value.append(slice.data(), slice.size());
  }  }
}

}  // namespace kv
