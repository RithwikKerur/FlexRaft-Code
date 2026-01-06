#include <iostream>
#include "storage.h"
#include "storage_engine.h"
#include "log_entry.h"
#include "raft_type.h"
#include "encoder.h"     // Required for raft::Encoder

void CollectShardsFromValue(const std::string& raw_val, 
                            raft::Encoder::EncodingResults& accumulator, 
                            int& out_k, int& out_m, int db_index) {
    if (raw_val.size() < 12) return;

    const char* ptr = raw_val.data();
    
    // Read Header (k, m) from this specific DB's entry
    // We update the output k/m so the main loop knows the encoding parameters
    out_k = *reinterpret_cast<const int*>(ptr); 
    out_m = *reinterpret_cast<const int*>(ptr + 4);

    size_t current_offset = 12;
    
    // In a sharded setup, each DB usually holds 1 specific fragment.
    // However, your format supports multiple, so we loop to be safe.
    int internal_index = 1; 

    while (current_offset < raw_val.size()) {
        if (current_offset + sizeof(int) > raw_val.size()) break;
        int slice_len = *reinterpret_cast<const int*>(ptr + current_offset);
        current_offset += sizeof(int);
        if (current_offset + slice_len > raw_val.size()) break;

        // 1. Create a std::string copy (Safe for Slice constructor)
        std::string chunk_data(ptr + current_offset, slice_len);
        
        raft::raft_frag_id_t frag_id = static_cast<raft::raft_frag_id_t>(internal_index + db_index) ;
        std::cout << "internal index " << internal_index << "\n" << std::endl;
        // Check if we already have this fragment (from another DB)
        if (accumulator.find(frag_id) == accumulator.end()) {
             accumulator.insert({frag_id, raft::Slice(chunk_data)});
             // Debug print to confirm we are getting multiple shards
             // std::cout << "    Loaded Frag " << frag_id << " from DB " << db_index << "\n";
        }
        
        current_offset += slice_len;
        internal_index++;
    }
}



void ViewEntries(const std::string& filename) {
  raft::FileStorage* storage = raft::FileStorage::Open(filename);

  if (!storage) {
    std::cerr << "Error opening file storage: " << filename << std::endl;
    return;
  }

  std::vector<raft::LogEntry> entries;
  storage->LogEntries(&entries);

  std::cout << "Entries in " << filename << ":\n";
  for (const auto& entry : entries) {
    std::cout << "Index: " << entry.Index()
              << ", Term: " << entry.Term()
              << ", Data Size: " << entry.CommandLength()
              << std::endl;
  }

  raft::FileStorage::Close(storage);
}

void ViewDistributedDBs(const std::string& base_name, int num_dbs) {
  std::vector<kv::StorageEngine*> dbs;
  
  // 1. Open All Databases
  std::cout << "Opening " << num_dbs << " databases..." << std::endl;
  for (int i = 2; i <= num_dbs; ++i) {
      std::string db_name = base_name + std::to_string(i); // e.g., "testdb1"
      auto* db = kv::StorageEngine::NewRocksDBEngine(db_name);
      if (!db) {
          std::cerr << "Failed to open " << db_name << "!" << std::endl;
          return; // cleanup needed in real code
      }
      dbs.push_back(db);
      std::cout << "  - Opened " << db_name << std::endl;
  }

  // 2. Get Keys (Assume DB-1 has the master list of keys)
  std::vector<std::string> keys;
  dbs[0]->GetAllKeys(&keys);
  std::cout << "Found " << keys.size() << " keys. Starting Aggregation & Reconstruction...\n";
  std::cout << "------------------------------------------------------------\n";

  // 3. Process Each Key
  for (const auto &key : keys) {
      raft::Encoder::EncodingResults gathered_shards;
      int k = 0;
      int m = 0;
      
      // A. Query EVERY Database for this key
      for (int i = 0; i < dbs.size(); ++i) {
          std::string raw_val;
          // Note: i is the index in vector (0-4), i+1 is the DB ID (1-5)
          bool found = dbs[i]->Get(key, &raw_val);
          
          if (found) {
            std::cout << "Found key for DB " << i << "\n" << std::endl;
              // Extract the shard(s) from this DB and add to 'gathered_shards'
              // We pass 'i' as the db_index, assuming testdb1 holds frag 0, etc.
              CollectShardsFromValue(raw_val, gathered_shards, k, m, i*2);
          }
      }

      // B. Attempt Reconstruction
      std::cout << "Key: " << std::left << std::setw(15) << key 
                << " | Shards Found: " << gathered_shards.size() << "/" << num_dbs;

      if (static_cast<int>(gathered_shards.size()) >= k && k > 0) {
          raft::Encoder encoder;
          raft::Slice result;
          
          // Decode
          bool success = encoder.DecodeSlice(gathered_shards, 3, 2, &result);
          
          if (success) {
              std::cout << " | [SUCCESS] Reconstructed Size: " << result.size() << " bytes";
              // Optional: delete[] result.data() if Encoder allocates new[]
          } else {
              std::cout << " | [FAIL] Decode Error";
          }
      } else {
           std::cout << " | [FAIL] Not enough shards (Need " << k << ")";
      }
      std::cout << std::endl;

      // C. Cleanup Memory for this Key
      // Since Slice(std::string) allocated 'new char[]', we must delete it
      for (auto& item : gathered_shards) {
          delete[] item.second.data();
      }
  }

  // 4. Cleanup Databases
  for (auto* db : dbs) {
      delete db;
  }
}
/*
void ViewDBEntries(const std::string& filename) {
  kv::StorageEngine *db = kv::StorageEngine::NewRocksDBEngine(filename);

  if (!db) {
    std::cerr << "Failed to open database!" << std::endl;
    return;
  }
  std::cout << "Database opened successfully." << std::endl;

  // 2. Fetch all keys into memory
  std::vector<std::string> keys;
  std::cout << "Scanning for all keys..." << std::endl;
  
  db->GetAllKeys(&keys);

  std::cout << "Found " << keys.size() << " keys. Retrieving sizes..." << std::endl;
  std::cout << "------------------------------------------------" << std::endl;

  // 3. Loop through keys, Get the value, and print size
  size_t total_bytes = 0;
  
  for (const auto &key : keys) {
    std::string value;
    bool found = db->Get(key, &value);

    if (found) {
      std::cout << "Key: " << key 
                << " \t| Value Size: " << value.size() << " bytes" << std::endl;
      total_bytes += value.size();
    } else {
      // This technically shouldn't happen since we just got the key list from the DB
      std::cerr << "Key: " << key << " \t| ERROR: Key not found during Get()" << std::endl;
    }
  }

  std::cout << "------------------------------------------------" << std::endl;
  std::cout << "Total Data Size (Values only): " << total_bytes << " bytes" << std::endl;

  // 4. Cleanup
  delete db;

} */

int main(int argc, char* argv[]) {

  ViewDistributedDBs("/Users/rithwikkerur/Documents/UCSB/data/testdb", 3);
  return 0;
}
