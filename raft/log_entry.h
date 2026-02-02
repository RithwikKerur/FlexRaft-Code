#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdio>
#include <string>
#include <vector>
#include <memory>
#include "SF/Archive.hpp"
#include "raft_type.h"

namespace raft {

class Serializer;

class Slice {
 public:
  static Slice Copy(const Slice &slice) {
    auto data = new char[slice.size()];
    std::memcpy(data, slice.data(), slice.size());
    return Slice(data, slice.size(), true);
  }

 public:
  // Non-owning constructor (view into existing memory)
  Slice(char *data, size_t size) : data_(data), size_(size), owned_data_(nullptr) {}

  // Owning constructor (takes ownership of memory)
  Slice(char *data, size_t size, bool owns) : data_(data), size_(size) {
    if (owns && data != nullptr) {
      owned_data_ = std::shared_ptr<char[]>(data);
    }
  }

  // String constructor (creates owned copy)
  Slice(const std::string &s) : size_(s.size()) {
    auto data = new char[s.size()];
    std::memcpy(data, s.c_str(), size_);
    data_ = data;
    owned_data_ = std::shared_ptr<char[]>(data);
  }

  Slice() = default;
  Slice(const Slice &) = default;
  Slice &operator=(const Slice &) = default;

  auto data() const -> char * { return data_; }
  auto size() const -> size_t { return size_; }
  auto valid() const -> bool { return data_ != nullptr && size_ > 0; }
  auto toString() const -> std::string { return std::string(data_, size_); }

  // Require both slice are valid
  auto compare(const Slice &slice) const -> int {
    assert(valid() && slice.valid());
    auto cmp_len = std::min(size(), slice.size());
    auto cmp_res = std::memcmp(data(), slice.data(), cmp_len);
    if (cmp_res != 0 || size() == slice.size()) {
      return cmp_res;
    }
    return size() > slice.size() ? 1 : -1;
  }

 private:
  char *data_ = nullptr;
  size_t size_ = 0;
  std::shared_ptr<char[]> owned_data_;  // Manages owned memory with reference counting
};

class Stripe;
class LogEntry {
  friend class Serializer;

 public:
  LogEntry() = default;
  LogEntry &operator=(const LogEntry &) = default;

  auto Index() const -> raft_index_t { return index; }
  void SetIndex(raft_index_t index) { this->index = index; }

  auto Term() const -> raft_term_t { return term; }
  void SetTerm(raft_term_t term) { this->term = term; }

  auto Type() const -> raft_entry_type { return type; }
  void SetType(raft_entry_type type) { this->type = type; }

  auto GetChunkInfo() const -> ChunkInfo { return chunk_info; }
  void SetChunkInfo(const ChunkInfo &chunk_info) { this->chunk_info = chunk_info; }

  auto StartOffset() const -> int { return start_fragment_offset; }
  void SetStartOffset(int off) { start_fragment_offset = off; }

  auto CommandData() const -> const Slice& {
    static const Slice empty_slice;
    return Type() == kNormal ? command_data_ : empty_slice;
  }
  auto CommandLength() const -> int { return command_size_; }
  void SetCommandLength(int size) { command_size_ = size; }

  void SetCommandData(const Slice &slice) {
    command_data_ = slice;
    command_size_ = slice.size();
  }

  auto NotEncodedSlice() const -> const Slice& {
    return Type() == kNormal ? CommandData() : not_encoded_slice_;
  }
  void SetNotEncodedSlice(const Slice &slice) { not_encoded_slice_ = slice; }

  auto FragmentSlice() const -> const std::vector<Slice>& {
    static const std::vector<Slice> empty_vec;
    return Type() == kNormal ? empty_vec : fragment_slices;
  }
  void SetFragmentSlice(const std::vector<Slice> &slice) { fragment_slices = slice; }

  auto GetFragmentsSize() const -> size_t {
    size_t total_size = 0;
    for(const Slice& s: fragment_slices){
      total_size+= s.size();
    }
    return total_size;
  }


  // Serialization function required by RCF
  // void serialize(SF::Archive &ar);
  //
  // Dump some important information
  std::string ToString() const {
    char buf[256];
    sprintf(buf,
            "LogEntry{term=%d, index=%d, type=%s, chunkinfo=%s, "
            "commandlen=%d, start_off=%d}",
            Term(), Index(), EntryTypeToString(Type()), chunk_info.ToString().c_str(),
            CommandLength(), StartOffset());

    return std::string(buf);
  }

 private:
  // These three attributes are allocated when creating a command
  raft_term_t term;
  raft_index_t index;
  raft_entry_type type;  // Full entry or fragments

  // Information of this chunk that is contained in this raft entry
  ChunkInfo chunk_info;

  // [REQUIRE] specified by user, indicating the start offset of command
  // data for encoding
  int start_fragment_offset;
  int command_size_;

  Slice command_data_;       // Spcified by user, valid iff type = normal
  Slice not_encoded_slice_;  // Command data not being encoded
  std::vector<Slice> fragment_slices;     // Fragments of encoded data
};

auto operator==(const LogEntry &lhs, const LogEntry &rhs) -> bool;
}  // namespace raft
