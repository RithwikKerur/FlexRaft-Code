#include "serializer.h"

#include <cstring>

#include "RCF/ByteBuffer.hpp"
#include "log_entry.h"
#include "raft_struct.h"
#include "raft_type.h"

namespace raft {
Serializer Serializer::NewSerializer() { return Serializer(); }

char *Serializer::serialize_logentry_helper(const LogEntry *entry, char *dst) {
  // Serialize basic fields individually instead of copying entire struct
  // to avoid copying invalid pointers from Slice and vector objects
  
  // Serialize basic data members
  std::memcpy(dst, &entry->term, sizeof(raft_term_t));
  dst += sizeof(raft_term_t);
  
  std::memcpy(dst, &entry->index, sizeof(raft_index_t));
  dst += sizeof(raft_index_t);
  
  std::memcpy(dst, &entry->type, sizeof(raft_entry_type));
  dst += sizeof(raft_entry_type);
  
  std::memcpy(dst, &entry->chunk_info, sizeof(ChunkInfo));
  dst += sizeof(ChunkInfo);
  
  std::memcpy(dst, &entry->start_fragment_offset, sizeof(int));
  dst += sizeof(int);
  
  std::memcpy(dst, &entry->command_size_, sizeof(int));
  dst += sizeof(int);
  
  // Serialize the slice data
  dst = PutPrefixLengthSlice(entry->NotEncodedSlice(), dst);
  dst = PutPrefixLengthSlices(entry->FragmentSlice(), dst);
  return dst;
}

const char *Serializer::deserialize_logentry_helper(const char *src, LogEntry *entry) {
  // Deserialize basic fields individually instead of copying entire struct
  // to avoid copying invalid pointers from Slice and vector objects
  
  // Deserialize basic data members
  std::memcpy(&entry->term, src, sizeof(raft_term_t));
  src += sizeof(raft_term_t);
  
  std::memcpy(&entry->index, src, sizeof(raft_index_t));
  src += sizeof(raft_index_t);
  
  std::memcpy(&entry->type, src, sizeof(raft_entry_type));
  src += sizeof(raft_entry_type);
  
  std::memcpy(&entry->chunk_info, src, sizeof(ChunkInfo));
  src += sizeof(ChunkInfo);
  
  std::memcpy(&entry->start_fragment_offset, src, sizeof(int));
  src += sizeof(int);
  
  std::memcpy(&entry->command_size_, src, sizeof(int));
  src += sizeof(int);
  
  Slice not_encoded;
  std::vector<Slice> fragment_slices;
  
  // Parse the not-encoded slice (single slice)
  src = ParsePrefixLengthSlice(src, &not_encoded);
  
  // Parse the fragment slices (vector of slices)
  src = ParsePrefixLengthSlices(src, &fragment_slices);

  entry->SetNotEncodedSlice(not_encoded);
  entry->SetFragmentSlice(fragment_slices);

  if (entry->Type() == kNormal) {
    entry->SetCommandData(not_encoded);
  }
  return src;
}


const char *Serializer::deserialize_logentry_withbound(const char *src, size_t len,
                                                       LogEntry *entry) {
                                                        /*
  if (len < sizeof(LogEntry)) {
    return nullptr;
  }
  std::memcpy(entry, src, sizeof(LogEntry));
  src += sizeof(LogEntry);
  len -= sizeof(LogEntry);
  Slice not_encoded, frag;
  auto tmp_src = src;
  src = ParsePrefixLengthSliceWithBound(src, len, &not_encoded);
  if (src == nullptr) return nullptr;
  len -= (src - tmp_src);
  src = ParsePrefixLengthSliceWithBound(src, len, &frag);
  if (src == nullptr) return nullptr;

  entry->SetNotEncodedSlice(not_encoded);
  entry->SetFragmentSlice(frag);

  if (entry->Type() == kNormal) {
    entry->SetCommandData(not_encoded);
  } */
  return src;
}

void Serializer::Serialize(const LogEntry *entry, RCF::ByteBuffer *buffer) {
  serialize_logentry_helper(entry, buffer->getPtr());
}

void Serializer::Deserialize(const RCF::ByteBuffer *buffer, LogEntry *entry) {
  deserialize_logentry_helper(buffer->getPtr(), entry);
}

void Serializer::Serialize(const RequestVoteArgs *args, RCF::ByteBuffer *buffer) {
  auto dst = buffer->getPtr();
  std::memcpy(dst, args, sizeof(RequestVoteArgs));
}

void Serializer::Deserialize(const RCF::ByteBuffer *buffer, RequestVoteArgs *args) {
  auto src = buffer->getPtr();
  std::memcpy(args, src, sizeof(RequestVoteArgs));
}

void Serializer::Serialize(const RequestVoteReply *reply, RCF::ByteBuffer *buffer) {
  auto dst = buffer->getPtr();
  std::memcpy(dst, reply, sizeof(RequestVoteReply));
}

void Serializer::Deserialize(const RCF::ByteBuffer *buffer, RequestVoteReply *reply) {
  auto src = buffer->getPtr();
  std::memcpy(reply, src, sizeof(RequestVoteReply));
}

void Serializer::Serialize(const AppendEntriesArgs *args, RCF::ByteBuffer *buffer) {
  assert(args->entry_cnt == args->entries.size());
  auto dst = buffer->getPtr();
  std::memcpy(dst, args, kAppendEntriesArgsHdrSize);
  dst += kAppendEntriesArgsHdrSize;
  for (const auto &ent : args->entries) {
    dst = serialize_logentry_helper(&ent, dst);
  }
}

void Serializer::Deserialize(const RCF::ByteBuffer *buffer, AppendEntriesArgs *args) {
  const char *src = buffer->getPtr();
  auto buffer_size = buffer->getLength();

  // Validate buffer size
  if (buffer_size < kAppendEntriesArgsHdrSize) {
    // Buffer too small - cannot deserialize, set safe defaults
    printf("ERROR: AppendEntriesArgs buffer too small: %zu < %zu\n", buffer_size, kAppendEntriesArgsHdrSize);
    args->term = 0;
    args->leader_id = 0;
    args->prev_log_index = 0;
    args->prev_log_term = 0;
    args->leader_commit = 0;
    args->entry_cnt = 0;
    return;
  }

  std::memcpy(args, src, kAppendEntriesArgsHdrSize);
  src += kAppendEntriesArgsHdrSize;

  // Detect and log corruption
  if (static_cast<int64_t>(args->term) < 0 || args->term > 2000000000) {
    printf("ERROR: Corrupted AppendEntriesArgs - term=%u (0x%08x) buffer_size=%zu\n",
           args->term, args->term, buffer_size);
    // Reset to safe values
    args->term = 0;
    args->leader_id = 0;
    args->entry_cnt = 0;
    return;
  }

  // Validate entry_cnt
  if (args->entry_cnt < 0 || args->entry_cnt > 10000) {
    printf("ERROR: Corrupted entry_cnt=%lld in AppendEntriesArgs\n", (long long)args->entry_cnt);
    args->entry_cnt = 0;
    return;
  }

  args->entries.reserve(args->entry_cnt);
  const char *buffer_end = buffer->getPtr() + buffer_size;

  for (decltype(args->entry_cnt) i = 0; i < args->entry_cnt; ++i) {
    // Check if we have enough buffer space remaining
    if (src >= buffer_end) {
      printf("ERROR: Buffer overflow in AppendEntriesArgs - ran out of buffer at entry %lld/%lld\n",
             (long long)i, (long long)args->entry_cnt);
      args->entry_cnt = i;  // Truncate to entries we successfully parsed
      return;
    }

    LogEntry ent;
    const char *new_src = deserialize_logentry_helper(src, &ent);

    // Validate that deserialization advanced the pointer reasonably
    if (new_src <= src || new_src > buffer_end) {
      printf("ERROR: Invalid deserialization - pointer moved from %p to %p (buffer_end=%p)\n",
             (void*)src, (void*)new_src, (void*)buffer_end);
      args->entry_cnt = i;
      return;
    }

    src = new_src;
    args->entries.push_back(ent);
  }
}

void Serializer::Serialize(const AppendEntriesReply *reply, RCF::ByteBuffer *buffer) {
  auto dst = buffer->getPtr();

  // Log what we're about to serialize
  if (static_cast<int64_t>(reply->term) < 0 || reply->term > 2000000000) {
    printf("SENDER ERROR: Serializing corrupted AppendEntriesReply - term=%u (0x%08x) success=%d expect_index=%u reply_id=%u chunk_cnt=%d\n",
           reply->term, reply->term, reply->success, reply->expect_index, reply->reply_id, reply->chunk_info_cnt);
  }

  // std::memcpy(dst, reply, sizeof(AppendEntriesReply));
  std::memcpy(dst, reply, kAppendEntriesReplyHdrSize);
  dst += kAppendEntriesReplyHdrSize;
  for (const auto &chunk_info : reply->chunk_infos) {
    std::memcpy(dst, &chunk_info, sizeof(ChunkInfo));
    dst += sizeof(ChunkInfo);
  }
}

void Serializer::Deserialize(const RCF::ByteBuffer *buffer, AppendEntriesReply *reply) {
  auto src = buffer->getPtr();
  auto buffer_size = buffer->getLength();

  // Validate buffer size
  if (buffer_size < kAppendEntriesReplyHdrSize) {
    printf("ERROR: AppendEntriesReply buffer too small: %zu < %zu\n", buffer_size, kAppendEntriesReplyHdrSize);
    reply->term = 0;
    reply->success = 0;
    reply->expect_index = 0;
    reply->reply_id = 0;
    reply->padding = 0;
    reply->chunk_info_cnt = 0;
    return;
  }

  std::memcpy(reply, src, kAppendEntriesReplyHdrSize);
  src += kAppendEntriesReplyHdrSize;

  // Detect term corruption
  if (static_cast<int64_t>(reply->term) < 0 || reply->term > 2000000000) {
    printf("ERROR: Corrupted AppendEntriesReply - term=%u (0x%08x) success=%d expect_index=%u reply_id=%u\n",
           reply->term, reply->term, reply->success, reply->expect_index, reply->reply_id);
    reply->term = 0;
    reply->success = 0;
    reply->chunk_info_cnt = 0;
    return;
  }

  // Validate chunk_info_cnt before using it
  if (reply->chunk_info_cnt < 0 || reply->chunk_info_cnt > 1000) {
    printf("ERROR: Corrupted chunk_info_cnt=%d in AppendEntriesReply\n", reply->chunk_info_cnt);
    reply->chunk_info_cnt = 0;
    return;
  }

  // Validate remaining buffer size
  size_t required_size = kAppendEntriesReplyHdrSize + (reply->chunk_info_cnt * sizeof(ChunkInfo));
  if (buffer_size < required_size) {
    printf("ERROR: AppendEntriesReply buffer size mismatch: have %zu, need %zu for %d chunks\n",
           buffer_size, required_size, reply->chunk_info_cnt);
    reply->chunk_info_cnt = 0;
    return;
  }

  for (int i = 0; i < reply->chunk_info_cnt; ++i) {
    ChunkInfo ci;
    std::memcpy(&ci, src, sizeof(ChunkInfo));
    src += sizeof(ChunkInfo);
    reply->chunk_infos.push_back(ci);
  }
}

void Serializer::Serialize(const RequestFragmentsArgs *args, RCF::ByteBuffer *buffer) {
  auto dst = buffer->getPtr();
  std::memcpy(dst, args, sizeof(RequestFragmentsArgs));
}

void Serializer::Deserialize(const RCF::ByteBuffer *buffer, RequestFragmentsArgs *args) {
  auto src = buffer->getPtr();
  std::memcpy(args, src, sizeof(RequestFragmentsArgs));
}

void Serializer::Serialize(const RequestFragmentsReply *reply, RCF::ByteBuffer *buffer) {
  assert(reply->entry_cnt == reply->fragments.size());
  auto dst = buffer->getPtr();
  std::memcpy(dst, reply, kRequestFragmentsReplyHdrSize);
  dst += kRequestFragmentsReplyHdrSize;
  for (const auto &ent : reply->fragments) {
    dst = serialize_logentry_helper(&ent, dst);
  }
}

void Serializer::Deserialize(const RCF::ByteBuffer *buffer, RequestFragmentsReply *reply) {
  const char *src = buffer->getPtr();
  auto buffer_size = buffer->getLength();

  // Validate buffer size
  if (buffer_size < kRequestFragmentsReplyHdrSize) {
    printf("ERROR: RequestFragmentsReply buffer too small: %zu < %zu\n", buffer_size, kRequestFragmentsReplyHdrSize);
    reply->entry_cnt = 0;
    return;
  }

  std::memcpy(reply, src, kRequestFragmentsReplyHdrSize);
  src += kRequestFragmentsReplyHdrSize;

  // Validate entry_cnt
  if (reply->entry_cnt < 0 || reply->entry_cnt > 10000) {
    printf("ERROR: Corrupted entry_cnt=%d in RequestFragmentsReply\n", reply->entry_cnt);
    reply->entry_cnt = 0;
    return;
  }

  reply->fragments.reserve(reply->entry_cnt);
  const char *buffer_end = buffer->getPtr() + buffer_size;

  for (decltype(reply->entry_cnt) i = 0; i < reply->entry_cnt; ++i) {
    // Check if we have enough buffer space remaining
    if (src >= buffer_end) {
      printf("ERROR: Buffer overflow in RequestFragmentsReply - ran out of buffer at entry %d/%d\n",
             i, reply->entry_cnt);
      reply->entry_cnt = i;
      return;
    }

    LogEntry ent;
    const char *new_src = deserialize_logentry_helper(src, &ent);

    // Validate that deserialization advanced the pointer reasonably
    if (new_src <= src || new_src > buffer_end) {
      printf("ERROR: Invalid deserialization in RequestFragmentsReply - pointer moved from %p to %p (buffer_end=%p)\n",
             (void*)src, (void*)new_src, (void*)buffer_end);
      reply->entry_cnt = i;
      return;
    }

    src = new_src;
    reply->fragments.push_back(ent);
  }
}

char *Serializer::PutPrefixLengthSlice(const Slice &slice, char *buf) {
  *reinterpret_cast<int *>(buf) = slice.size();
  //printf("Serializing size %d\n", slice.size());
  buf += sizeof(int);
  std::memcpy(buf, slice.data(), slice.size());
  return buf + slice.size();
}

char *Serializer::PutPrefixLengthSlices(const std::vector<Slice> &slices, char *buf) {
  // First, serialize the number of slices in the vector
  *reinterpret_cast<int *>(buf) = slices.size();
  //printf("Serializing %zu slices\n", slices.size());
  buf += sizeof(int);
  
  // Then serialize each slice with its own length prefix
  for (const auto &slice : slices) {
    // Write the size of this slice
    *reinterpret_cast<int *>(buf) = slice.size();
    //printf("Serializing slice size %zu\n", slice.size());
    buf += sizeof(int);
    
    // Write the slice data
    std::memcpy(buf, slice.data(), slice.size());
    buf += slice.size();
  }
  
  return buf;
}


const char *Serializer::ParsePrefixLengthSlice(const char *buf, Slice *slice) {
  int size = *reinterpret_cast<const int *>(buf);
  //printf("DeSerializing size %d\n", size);

  // Validate size before allocating
  if (size < 0 || size > 100*1024*1024) {  // Max 100MB per slice
    printf("ERROR: Invalid slice size=%d in ParsePrefixLengthSlice\n", size);
    *slice = Slice(nullptr, 0);
    return buf + sizeof(int);  // Skip the size field
  }

  char *data = new char[size];
  buf += sizeof(int);
  std::memcpy(data, buf, size);
  *slice = Slice(data, size);
  return buf + size;
}

const char *Serializer::ParsePrefixLengthSlices(const char *buf, std::vector<Slice> *slices) {
  // Read the number of slices
  int num_slices = *reinterpret_cast<const int *>(buf);
  //printf("Deserializing %zu slices\n", num_slices);
  buf += sizeof(int);

  // Validate num_slices
  if (num_slices < 0 || num_slices > 10000) {
    printf("ERROR: Invalid num_slices=%d in ParsePrefixLengthSlices\n", num_slices);
    slices->clear();
    return buf;
  }

  slices->clear();
  slices->reserve(num_slices);

  // Read each slice
  for (int i = 0; i < num_slices; ++i) {
    // Read slice size
    int slice_size = *reinterpret_cast<const int *>(buf);
    //printf("Deserializing slice size %zu\n", slice_size);
    buf += sizeof(int);

    // Validate slice_size
    if (slice_size < 0 || slice_size > 100*1024*1024) {  // Max 100MB per slice
      printf("ERROR: Invalid slice_size=%d in ParsePrefixLengthSlices at slice %d/%d\n",
             slice_size, i, num_slices);
      // Return what we've parsed so far
      return buf;
    }

    // Create slice from data
    char *data = new char[slice_size];
    std::memcpy(data, buf, slice_size);
    slices->emplace_back(data, slice_size);
    buf += slice_size;
  }

  return buf;
}

const char *Serializer::ParsePrefixLengthSliceWithBound(const char *buf, size_t len, Slice *slice) {
  if (len < sizeof(int)) {
    return nullptr;
  }
  int size = *reinterpret_cast<const int *>(buf);
  if (size + sizeof(int) > len) {  // Beyond range
    return nullptr;
  }
  char *data = new char[size];
  buf += sizeof(int);
  std::memcpy(data, buf, size);
  *slice = Slice(data, size);
  return buf + size;
}

size_t Serializer::getSerializeSize(const LogEntry &entry) {
  // Calculate size based on individual fields, not sizeof(LogEntry)
  size_t ret = 0;
  
  // Basic data members
  ret += sizeof(raft_term_t);           // term
  ret += sizeof(raft_index_t);          // index
  ret += sizeof(raft_entry_type);       // type
  ret += sizeof(ChunkInfo);             // chunk_info
  ret += sizeof(int);                   // start_fragment_offset
  ret += sizeof(int);                   // command_size_
  
  // Slice data with length prefixes
  ret += sizeof(int) + entry.NotEncodedSlice().size();  // not_encoded slice
  
  // Fragment slices with length prefixes
  ret += sizeof(int);  // number of fragment slices
  for (const auto &slice : entry.FragmentSlice()) {
    ret += sizeof(int) + slice.size();  // each slice with its size prefix
  }

  return ret;
}

size_t Serializer::getSerializeSize(const RequestVoteArgs &args) { return sizeof(args); }

size_t Serializer::getSerializeSize(const RequestVoteReply &reply) { return sizeof(reply); }

size_t Serializer::getSerializeSize(const AppendEntriesArgs &args) {
  size_t ret = kAppendEntriesArgsHdrSize;
  for (const auto &ent : args.entries) {
    ret += getSerializeSize(ent);
  }
  return ret;
}

size_t Serializer::getSerializeSize(const AppendEntriesReply &reply) {
  size_t ret = kAppendEntriesReplyHdrSize;
  ret += reply.chunk_info_cnt * sizeof(ChunkInfo);
  return ret;
}

size_t Serializer::getSerializeSize(const RequestFragmentsArgs &args) { return sizeof(args); }

size_t Serializer::getSerializeSize(const RequestFragmentsReply &reply) {
  size_t ret = kRequestFragmentsReplyHdrSize;
  for (const auto &ent : reply.fragments) {
    ret += getSerializeSize(ent);
  }
  return ret;
}

}  // namespace raft
