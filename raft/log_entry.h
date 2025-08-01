#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdio>
#include <string>

#include "../RCF/include/SF/Archive.hpp"
#include "raft_type.h"

namespace raft {

class Serializer;

class Slice {
 public:
  static Slice Copy(const Slice &slice) {
    auto data = new char[slice.size() + 12];
    std::memcpy(data, slice.data(), slice.size());
    return Slice(data, slice.size());
  }

 public:
  Slice(char *data, size_t size) : data_(data), size_(size) {}
  Slice(const std::string &s) : data_(new char[s.size()]), size_(s.size()) {
    std::memcpy(data_, s.c_str(), size_);
  }

  Slice() = default;
  Slice(const Slice& other)
    : data_(nullptr), size_(other.size_) // Initialize members
{
    if (size_ > 0) {
        data_ = new char[size_]; // Allocate NEW memory
        std::memcpy(data_, other.data_, size_); // Copy the CONTENT
    }
}
  auto data() const -> char * { return data_; }
  auto size() const -> size_t { return size_; }
  auto valid() const -> bool { return data_ != nullptr && size_ > 0; }
  auto toString() const -> std::string { return std::string(data_, size_); }

  // Require both slice are valid
  auto compare(const Slice &slice) -> int {
    assert(valid() && slice.valid());
    auto cmp_len = std::min(size(), slice.size());
    auto cmp_res = std::memcmp(data(), slice.data(), cmp_len);
    if (cmp_res != 0 || size() == slice.size()) {
      return cmp_res;
    }
    return size() > slice.size() ? 1 : -1;
  }
  auto operator==(const Slice& other) const {
    // First, if the sizes are different, they can't be equal.
    if (this->size() != other.size()) {
      return false;
    }
  
    // If both are empty, they are equal.
    if (this->size() == 0) {
      return true;
    }
  
    // Otherwise, compare the memory content byte-for-byte.
    return std::memcmp(this->data(), other.data(), this->size()) == 0;
  }

  auto operator=(const Slice& other) {
    // Handle self-assignment (e.g., mySlice = mySlice;)
    if (this == &other) {
        return *this;
    }
    Slice temp(other);
    std::swap(this->data_, temp.data_);
    std::swap(this->size_, temp.size_);

    return *this;
}

 private:
  char *data_ = nullptr;
  size_t size_ = 0;
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

  auto CommandData() const -> Slice { return Type() == kNormal ? command_data_ : Slice(); }
  auto CommandLength() const -> int { return command_size_; }
  void SetCommandLength(int size) { command_size_ = size; }

  void SetCommandData(const Slice &slice) {
    command_data_ = slice;
    command_size_ = slice.size();
  }

  auto NotEncodedSlice() const -> Slice {
    return Type() == kNormal ? CommandData() : not_encoded_slice_;
  }
  void SetNotEncodedSlice(const Slice &slice) { not_encoded_slice_ = slice; }

  auto FragmentSlice() const -> Slice { return Type() == kNormal ? Slice() : fragment_slice_; }
  void SetFragmentSlice(const Slice &slice) { fragment_slice_ = slice; }
  void SetExtraFragment(const Slice &slice) { extra_fragment_ = slice; }
  auto ExtraFragment() const -> Slice { return Type() == kNormal ? Slice() : extra_fragment_; }

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
  Slice fragment_slice_;     // Fragments of encoded data
  Slice extra_fragment_;
};

auto operator==(const LogEntry &lhs, const LogEntry &rhs) -> bool;
}  // namespace raft
