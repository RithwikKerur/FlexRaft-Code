#include <iostream>
#include "storage.h"
#include "storage_engine.h"
#include "log_entry.h"
#include "raft_type.h"

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

}

int main(int argc, char* argv[]) {

  ViewDBEntries("/Users/rithwikkerur/Documents/UCSB/data/testdb3");
  return 0;
}
