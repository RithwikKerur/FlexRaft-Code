#include <iostream>
#include "storage.h"
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

int main(int argc, char* argv[]) {

  ViewEntries("/Users/rithwikkerur/Documents/UCSB/data/raft_log2");
  return 0;
}
