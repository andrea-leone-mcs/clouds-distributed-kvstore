#include "shardmaster.h"
using namespace std;

const shard_t StaticShardmaster::ALL_KEYS_SHARD = {MIN_KEY, MAX_KEY};
const size_t StaticShardmaster::NUM_SHARDS = MAX_KEY - MIN_KEY + 1;

StaticShardmaster::StaticShardmaster() : _mutex(make_unique<mutex>()) { }

void StaticShardmaster::_reassign_shards() {
    vector<shard_t> new_shards = split_shard(ALL_KEYS_SHARD, _server_list.size());
    for (size_t i = 0; i < _server_list.size(); i++) {
        _servers[_server_list[i]].clear();
        _servers[_server_list[i]].push_back(new_shards[i]);
    }
}

/**
 * Based on the server specified in JoinRequest, you should update the
 * shardmaster's internal representation that this server has joined. Remember,
 * you get to choose how to represent everything the shardmaster tracks in
 * shardmaster.h! Be sure to rebalance the shards in equal proportions to all
 * the servers. This function should fail if the server already exists in the
 * configuration.
 *
 * @param context - you can ignore this
 * @param request A message containing the address of a key-value server that's
 * joining
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::Join(::grpc::ServerContext* context,
                                       const ::JoinRequest* request,
                                       Empty* response) {
    string server = request->server();
    lock_guard<mutex> lock(*_mutex);
    if (_servers.find(server) != _servers.end()) {
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Server already exists");
    }
    if (_servers.size() == NUM_SHARDS) {
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "No shards left to give");
    }
    
    _server_list.push_back(server);
    _servers[server] = vector<shard_t>();
    _reassign_shards();
    return ::grpc::Status::OK;
}

/**
 * LeaveRequest will specify a list of servers leaving. This will be very
 * similar to join, wherein you should update the shardmaster's internal
 * representation to reflect the fact the server(s) are leaving. Once that's
 * completed, be sure to rebalance the shards in equal proportions to the
 * remaining servers. If any of the specified servers do not exist in the
 * current configuration, this function should fail.
 *
 * @param context - you can ignore this
 * @param request A message containing a list of server addresses that are
 * leaving
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::Leave(::grpc::ServerContext* context,
                                        const ::LeaveRequest* request,
                                        Empty* response) {
    lock_guard<mutex> lock(*_mutex);
    for (const auto& server : request->servers()) {
        if (_servers.find(server) == _servers.end()) {
            return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Server does not exist");
        }
        _servers.erase(server);
        _server_list.erase(remove(_server_list.begin(), _server_list.end(), server), _server_list.end());
    }

    _reassign_shards();

    return ::grpc::Status::OK;
}

/**
 * Move the specified shard to the target server (passed in MoveRequest) in the
 * shardmaster's internal representation of which server has which shard. Note
 * this does not transfer any actual data in terms of kv-pairs. This function is
 * responsible for just updating the internal representation, meaning whatever
 * you chose as your data structure(s).
 *
 * @param context - you can ignore this
 * @param request A message containing a destination server address and the
 * lower/upper bounds of a shard we're putting on the destination server.
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::Move(::grpc::ServerContext* context,
                                       const ::MoveRequest* request,
                                       Empty* response) {
    // Hint: Take a look at get_overlap in common.{h, cc}
    // Using the function will save you lots of time and effort!
    const string& target_server = request->server();
    const shard_t shard{request->shard().lower(), request->shard().upper()};
    lock_guard<mutex> lock(*_mutex);
    if (_servers.find(target_server) == _servers.end()) {
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Server does not exist");
    }

    for (const auto& server : _server_list) {
        vector<shard_t>& shards = _servers[server];
        vector<shard_t> new_shards;
        for (auto& s : shards) {
            switch(get_overlap(shard, s)) {
                case OverlapStatus::NO_OVERLAP:
                    new_shards.push_back(s);
                    break;
                case OverlapStatus::OVERLAP_START: {
                    pair<shard_t, shard_t> split = split_shard_at(s, shard.lower-1);
                    new_shards.push_back(split.first);
                    break;
                }
                case OverlapStatus::OVERLAP_END: {
                    pair<shard_t, shard_t> split = split_shard_at(s, shard.upper);
                    new_shards.push_back(split.second);
                    break;
                }
                case OverlapStatus::COMPLETELY_CONTAINS:
                    break;
                case OverlapStatus::COMPLETELY_CONTAINED: {
                    pair<shard_t, vector<shard_t>> parts = extract_shard(s, shard);
                    new_shards.insert(new_shards.end(), parts.second.begin(), parts.second.end());
                    break;
                }
            }
        }
        _servers[server] = new_shards;
    }
    _servers[target_server].push_back(shard);
    sortAscendingInterval(_servers[target_server]);

    return ::grpc::Status::OK;
}

/**
 * When this function is called, you should store the current servers and their
 * corresponding shards in QueryResponse. Take a look at
 * 'protos/shardmaster.proto' to see how to set QueryResponse correctly. Note
 * that its a list of ConfigEntry, which is a struct that has a server's address
 * and a list of the shards its currently responsible for.
 *
 * @param context - you can ignore this
 * @param request An empty message, as we don't need to send any data
 * @param response A message that specifies which shards are on which servers
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::Query(::grpc::ServerContext* context,
                                        const StaticShardmaster::Empty* request,
                                        ::QueryResponse* response) {
    lock_guard<mutex> lock(*_mutex);
    for (const auto& server : _server_list) {
        ConfigEntry* entry = response->add_config();
        entry->set_server(server);
        for (const auto& shard : _servers[server]) {
            Shard* response_shard = entry->add_shards();
            response_shard->set_lower(shard.lower);
            response_shard->set_upper(shard.upper);
        }
    }
    
    return ::grpc::Status::OK;
}
