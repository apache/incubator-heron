#ifndef SRC_CPP_SVCS_STMGR_SRC_UTIL_ROTATING_MAP_H_
#define SRC_CPP_SVCS_STMGR_SRC_UTIL_ROTATING_MAP_H_

#include <unordered_map>
#include <list>

namespace heron {
namespace stmgr {

// Rotating Map maintains a list of unordered maps.
// Every time a rotate is called, it drops the
// last map and intantiates a new map at the head
// of the list. The create operation adds elements
// to the front map of the list. The anchor and remove
// operation do their operations starting from the
// front of the list to the back.
class RotatingMap {
 public:
  // Creates a rotating map with _nbuckets maps
  explicit RotatingMap(sp_int32 _nbuckets);
  ~RotatingMap();

  // Deletes the last map on the list and
  // instantiates a new map at the front of the list
  void rotate();

  // Adds an item to the map at the front of the list
  void create(sp_int64 _key, sp_int64 _value);

  // Starting from the front of the list,
  // it checks if the key already exists and if so
  // xors another entry to that. Returns true if the
  // xor turns zero. False otherwise
  bool anchor(sp_int64 _key, sp_int64 _value);

  // Starting from the front of the list, checks
  // if the key was present in the map and if so
  // removes it. Returns true if the key was removed
  // from some map. False otherwise.
  bool remove(sp_int64 _key);

 private:
  std::list<std::unordered_map<sp_int64, sp_int64>*> buckets_;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_UTIL_ROTATING_MAP_H_
