#include "common.h"
#include <algorithm>
#include <cassert>
#include <regex>
#include <cstring>

void sortAscendingInterval(std::vector<shard_t>& shards) {
  std::sort(
      shards.begin(), shards.end(),
      [](const shard_t& a, const shard_t& b) { return a.lower < b.lower; });
}

size_t size(const shard_t& s) { return s.upper - s.lower + 1; }

bool shard_has_key(const shard_t& s, unsigned int key) { return s.lower <= key && key <= s.upper; }

std::pair<shard_t, shard_t> split_shard(const shard_t& s) {
  // can't get midpoint of size 1 shard
  assert(s.lower < s.upper);
  unsigned int midpoint = s.lower + ((s.upper - s.lower) / 2);
  return std::make_pair<shard_t, shard_t>({s.lower, midpoint},
                                          {midpoint + 1, s.upper});
}

std::vector<shard_t> split_shard(const shard_t& s, size_t num_shards) {
  assert(num_shards > 0);
  assert(s.upper - s.lower > num_shards);
  size_t shard_size = size(s) / num_shards;
  size_t remainder = size(s) % num_shards;
  std::vector<shard_t> shards;

  unsigned int lower = s.lower, upper;
  for(size_t i = 0; i < num_shards; i++) {
    upper = lower + shard_size - (i < remainder ? 0 : 1);
    shards.push_back({lower, upper});
    lower = upper+1;
  }
  assert(upper == s.upper);

  return shards;
}

std::pair<shard_t, shard_t> split_shard_at(const shard_t& s, unsigned int pos) {
  return std::make_pair<shard_t, shard_t>({s.lower, pos}, {pos + 1, s.upper});
}

std::pair<shard_t, std::vector<shard_t>> extract_shard(const shard_t& s,
                                                    const shard_t& s_sub) {
  std::vector<shard_t> shards_remaining;
  if(s.lower+1 <= s_sub.lower)
    shards_remaining.push_back({s.lower, s_sub.lower-1});
  if(s_sub.upper+1 <= s.upper)
    shards_remaining.push_back({s_sub.upper+1, s.upper});
  return std::make_pair(s_sub, shards_remaining);
}

void sortAscendingSize(std::vector<shard_t>& shards) {
  std::sort(
      shards.begin(), shards.end(),
      [](const shard_t& a, const shard_t& b) { return size(a) < size(b); });
}

void sortDescendingSize(std::vector<shard_t>& shards) {
  std::sort(
      shards.begin(), shards.end(),
      [](const shard_t& a, const shard_t& b) { return size(b) < size(a); });
}

size_t shardRangeSize(const std::vector<shard_t>& vec) {
  size_t tot = 0;
  for (const shard_t& s : vec) {
    tot += size(vec);
  }
  return tot;
}

OverlapStatus get_overlap(const shard_t& a, const shard_t& b) {
  if (a.upper < b.lower || b.upper < a.lower) {
    /**
     * A: [-----]
     * B:         [-----]
     */
    return OverlapStatus::NO_OVERLAP;
  } else if (b.lower <= a.lower && a.upper <= b.upper) {
    /**
     * A:    [----]
     * B:  [--------]
     * Note: This also includes the case where the two shards are equal!
     */
    return OverlapStatus::COMPLETELY_CONTAINED;
  } else if (a.lower < b.lower && a.upper > b.upper) {
    /**
     * A: [-------]
     * B:   [---]
     */
    return OverlapStatus::COMPLETELY_CONTAINS;
  } else if (a.lower >= b.lower && a.upper > b.upper) {
    /**
     * A:    [-----]
     * B: [----]
     */
    return OverlapStatus::OVERLAP_START;
  } else if (a.lower < b.lower && a.upper <= b.upper) {
    /**
     * A: [-------]
     * B:    [------]
     */
    return OverlapStatus::OVERLAP_END;
  } else {
    throw std::runtime_error("bad case in get_overlap\n");
  }
}

std::vector<std::string> split(const std::string& s) {
  std::vector<std::string> v;
  std::regex ws_re("\\s+");  // whitespace
  std::copy(std::sregex_token_iterator(s.begin(), s.end(), ws_re, -1),
            std::sregex_token_iterator(), std::back_inserter(v));
  return v;
}

std::vector<std::string> parse_value(std::string val, std::string delim) {
  std::vector<std::string> tokens;

  char *save;
  char *tok = strtok_r((char *)val.c_str(), delim.c_str(), &save);
  while(tok != NULL){
    tokens.push_back(std::string(tok));
    tok = strtok_r(NULL, delim.c_str(), &save);
  }

  return tokens;
}

int extractID(std::string key){
  std::vector<std::string> tokens;

  char *save;
  char *tok = strtok_r((char *)key.c_str(), "_", &save);
  while(tok != NULL){
    tokens.push_back(std::string(tok));
    tok = strtok_r(NULL, "_", &save);
  }
  
  assert(tokens.size() > 1); //illformed key

  return stoi(tokens[1]);
}
