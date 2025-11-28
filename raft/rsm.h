#pragma once
#include "log_entry.h"

namespace raft {
// Rsm is short for Replicate State machine
class Rsm {
 public:
  virtual void ApplyLogEntry(LogEntry entry) = 0;
  virtual int GetThreshold1() {return 0;}
  virtual int GetThreshold2() {return 0;}
  virtual int GetFlag() {return 0;}
  virtual void SetThreshold1(int threshold1) {}
  virtual void SetThreshold2(int threshold2) {}
  virtual void SetFlag(bool flag) {}
  
};
}  // namespace raft
