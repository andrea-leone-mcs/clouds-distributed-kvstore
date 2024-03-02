#include <grpcpp/grpcpp.h>
#include <iostream>

#include "shardkv.h"
using namespace std;

bool ShardkvServer::_manages_key(const string& key) {
    return key == "all_users" || _server_of(key) == shardmanager_address;
}

string ShardkvServer::_server_of(const string& key) {
    unsigned int ikey = extractID(key);
    return (--_keys_assignments.upper_bound(shard_t{ikey, ikey}))->second;
    /*string target_server = find_if(servers_shards.begin(), servers_shards.end(),
                                [&key](const auto& p) -> bool {
                                    return any_of(p.second.begin(), p.second.end(),
                                        [&key](shard_t s) -> bool { return shard_has_key(s, extractID(key)); });
                                })->first;
    return target_server;*/
}

bool ShardkvServer::_key_is_for_user(const std::string& key) {
    return key.front() == 'u' && key.back() != 's';
}

bool ShardkvServer::_key_is_for_post(const std::string& key) {
    return key.front() == 'p';
}

string ShardkvServer::_remove_user(std::string& users, const std::string& user) {
    vector<string> tokens = parse_value(users, ",");
    string new_value = "";
    for (const auto& t : tokens)
        if (t != user)
            new_value += t + ",";
    return new_value;
}

/**
 * This method is analogous to a hashmap lookup. A key is supplied in the
 * request and if its value can be found, we should either set the appropriate
 * field in the response Otherwise, we should return an error. An error should
 * also be returned if the server is not responsible for the specified key
 *
 * @param context - you can ignore this
 * @param request a message containing a key
 * @param response we store the value for the specified key here
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvServer::Get(::grpc::ServerContext* context,
                                  const ::GetRequest* request,
                                  ::GetResponse* response) {
    string key = request->key();

    lock_guard<mutex> lock(*_mutex);
    if (!_manages_key(key))
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Not responsible for key");
    if (_database.find(key) == _database.end())
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Key not found");
    response->set_data(_database[key]);
    return ::grpc::Status::OK;
}

/**
 * Insert the given key-value mapping into our store such that future gets will
 * retrieve it
 * If the item already exists, you must replace its previous value.
 * This function should error if the server is not responsible for the specified
 * key.
 *
 * @param context - you can ignore this
 * @param request A message containing a key-value pair
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvServer::Put(::grpc::ServerContext* context,
                                  const ::PutRequest* request,
                                  Empty* response) {
    string key = request->key();
    string value = request->data();
    string user = request->user();
    
    lock_guard<mutex> lock(*_mutex);

    if (_stub_to_backup != nullptr) {
        // cerr<<address<<" sending put to backup "<<_backup_address<<endl;
        ::grpc::ClientContext cc;
        Empty put_response;
        ::grpc::Status result;
        do {
            result = _stub_to_backup->Put(&cc, *request, &put_response);
        } while(!result.ok());
    }

    if (!_manages_key(key))
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Not responsible for key");
    _database[key] = value;
    if (_key_is_for_user(key)) {
        _database["all_users"] += key + ",";
    } else if(_key_is_for_post(key)) {
        _authors[key] = user;
        string responsible = _server_of(user);
        string user_id_posts_key = user + "_posts";
        if(responsible == shardmanager_address) {
            vector<string> tokens = parse_value(_database[user_id_posts_key], ",");
            if (count(tokens.begin(), tokens.end(), key) == 0)
                _database[user_id_posts_key] += key + ",";
        } else {
            // create a stub for the target server
            auto stub = Shardkv::NewStub(grpc::CreateChannel(responsible, grpc::InsecureChannelCredentials()));
            bool done = false;
            // keep trying to move the key until it succeeds
            while (!done) {
                ::grpc::ClientContext cc;
                AppendRequest append_request;
                append_request.set_key(user_id_posts_key);
                append_request.set_data(key);
                Empty append_response;
                auto put_result = stub->Append(&cc, append_request, &append_response);
                done = put_result.ok();
                if (!done)
                    cerr<<"Append "<<key<<" to "<<responsible<<" failed, retrying ..."<<endl;
            }
        }
    } else {
        cerr << "PUT Warning: key " << key << " is not for a user or a post" << endl;
    }
    return ::grpc::Status::OK;
}

/**
 * Appends the data in the request to whatever data the specified key maps to.
 * If the key is not mapped to anything, this method should be equivalent to a
 * put for the specified key and value. If the server is not responsible for the
 * specified key, this function should fail.
 *
 * @param context - you can ignore this
 * @param request A message containing a key-value pair
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>"
 */
::grpc::Status ShardkvServer::Append(::grpc::ServerContext* context,
                                     const ::AppendRequest* request,
                                     Empty* response) {
    string key = request->key();
    string value = request->data();

    lock_guard<mutex> lock(*_mutex);
    if (!_manages_key(key))
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Not responsible for key");
    if (_database.find(key) != _database.end() || !(_key_is_for_post(key) || _key_is_for_user(key))) {
        if (key.back() == 's') {
            vector<string> tokens = parse_value(_database[key], ",");
            // if(tokens.size()) cerr<<tokens[0]<<" | value = "<<value<<endl;
            if (count(tokens.begin(), tokens.end(), value) == 0)
                _database[key] += value + ",";
        } else
            _database[key] += value;
        return ::grpc::Status::OK;
    }
    ::grpc::ServerContext sc;
    PutRequest put_request;
    Empty resp;
    put_request.set_key(key);
    put_request.set_data(value);
    return Put(&sc, &put_request, &resp);
}

/**
 * Deletes the key-value pair associated with this key from the server.
 * If this server does not contain the requested key, do nothing and return
 * the error specified
 *
 * @param context - you can ignore this
 * @param request A message containing the key to be removed
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvServer::Delete(::grpc::ServerContext* context,
                                           const ::DeleteRequest* request,
                                           Empty* response) {
    string key = request->key();

    lock_guard<mutex> lock(*_mutex);
    if (!_manages_key(key))
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Not responsible for key");
    if (_database.find(key) == _database.end())
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Key not found");

    _database.erase(key);
    if (_key_is_for_user(key)) {
        // remove the user key from the "all_users" key
        _database["all_users"] = _remove_user(_database["all_users"], key);
    }
    return ::grpc::Status::OK;
}

/**
 * This method is called in a separate thread on periodic intervals (see the
 * constructor in shardkv.h for how this is done). It should query the shardmaster
 * for an updated configuration of how shards are distributed. You should then
 * find this server in that configuration and look at the shards associated with
 * it. These are the shards that the shardmaster deems this server responsible
 * for. Check that every key you have stored on this server is one that the
 * server is actually responsible for according to the shardmaster. If this
 * server is no longer responsible for a key, you should find the server that
 * is, and call the Put RPC in order to transfer the key/value pair to that
 * server. You should not let the Put RPC fail. That is, the RPC should be
 * continually retried until success. After the put RPC succeeds, delete the
 * key/value pair from this server's storage. Think about concurrency issues like
 * potential deadlock as you write this function!
 *
 * @param stub a grpc stub for the shardmaster, which we use to invoke the Query
 * method!
 */
void ShardkvServer::QueryShardmaster(Shardmaster::Stub* stub) {
    ::grpc::ClientContext cc;
    Empty request;
    QueryResponse response;
    // query shardmaster for updated configuration (should always succeed)
    auto query_result = stub->Query(&cc, request, &response);
    if(!query_result.ok()) {
        cerr<<"Query to shardmaster failed with error: "<<query_result.error_message()<<endl;
        cerr<<"Primary = "<<_is_primary<<endl;
        return;
    }
    assert(query_result.ok());

    // build a map with the results of the query
    vector<pair<shard_t, string>> shards_servers;
    for (const auto& e : response.config())
        for (const auto& s : e.shards())
            shards_servers.push_back({{s.lower(), s.upper()}, e.server()});


    // lock the mutex to avoid concurrent access to the database
    unique_lock<mutex> lock(*_mutex);
    // update the keys assignments
    _keys_assignments = map{shards_servers.begin(), shards_servers.end()};
    if (!_is_primary) {
        // if this is a backup server, it should not redistribute keys
        return;
    }

    // find keys that need to be redistributed and to which server
    // this is saved in a map
    unordered_map<string, vector<string>> keys_to_redostribute;
    for(auto& [k, v] : _database) {
        if (!_manages_key(k)) {
            string target_server = _server_of(k);
            if (keys_to_redostribute.find(target_server) == keys_to_redostribute.end())
                keys_to_redostribute[target_server] = vector<string>();
            keys_to_redostribute[target_server].push_back(k);
        }
    }
    if(keys_to_redostribute.empty())
        return;

    // build all the channels and put requests to redistribute keys to the target servers
    vector<pair<unique_ptr<Shardkv::Stub>, vector<PutRequest>>> stubs_requests;
    for (auto& [server, keys] : keys_to_redostribute) {
        auto stub = Shardkv::NewStub(grpc::CreateChannel(server, grpc::InsecureChannelCredentials()));
        vector<PutRequest> put_requests;
        for (auto& k : keys) {
            PutRequest put_request;
            put_request.set_key(k);
            put_request.set_data(_database[k]);
            if(_key_is_for_post(k))
                put_request.set_user(_authors[k]);
            put_requests.push_back(put_request);
        }
        stubs_requests.push_back({move(stub), move(put_requests)});
    }
    // cerr<<"lock releasing"<<endl;
    lock.unlock();

    // send the requests to the target servers
    for (auto& [stub, requests] : stubs_requests) {
        for (auto& request : requests) {
            // keep trying to move the key until it succeeds
            ::grpc::Status put_result;
            do {
                ::grpc::ClientContext cc;
                Empty put_response;
                put_result = stub->Put(&cc, request, &put_response);
            } while(!put_result.ok());
        }
    }

    lock.lock();
    for (auto& [server, keys] : keys_to_redostribute) {
        for (auto& k : keys) {
            _database.erase(k);
            _authors.erase(k);
            if(_key_is_for_user(k))
                _database["all_users"] = _remove_user(_database["all_users"], k);
        }
    }
}


/**
 * This method is called in a separate thread on periodic intervals (see the
 * constructor in shardkv.h for how this is done).
 * BASIC LOGIC - PART 2
 * It pings the shardmanager to signal the it is alive and available to receive Get, Put, Append and Delete RPCs.
 * The first time it pings the sharmanager, it will  receive the name of the shardmaster to contact (by means of a QuerySharmaster).
 *
 * PART 3
 *
 *
 * @param stub a grpc stub for the shardmaster, which we use to invoke the Query
 * method!
 * */
void ShardkvServer::PingShardmanager(Shardkv::Stub* stub) {
    ::grpc::ClientContext cc;
    PingRequest request;
    PingResponse response;

    unique_lock<mutex> lock(*_mutex);
    request.set_viewnumber(_viewnumber);
    request.set_server(address);
    lock.unlock();
    stub->Ping(&cc, request, &response);
    lock.lock();
    shardmaster_address = response.shardmaster();
    // cerr<<address<<" PINGED "<<_viewnumber<<endl;
    _is_primary = response.primary() == address;
    bool is_backup = !_is_primary && response.backup() == address;
    if (_is_primary && !response.backup().empty()) {
        if (response.backup() != _backup_address) {
            cerr<<address<<" creating channel towards "<<response.backup()<<endl;
            _backup_address = response.backup();
            _stub_to_backup = Shardkv::NewStub(grpc::CreateChannel(response.backup(), grpc::InsecureChannelCredentials()));
        }
    } else if (_stub_to_backup != nullptr || _backup_address != "") {
        cerr<<address<<" deleting channel towards "<<_backup_address<<endl;
        _stub_to_backup = nullptr;
        _backup_address = "";
    }
    if (is_backup && _viewnumber == 0) {
        unique_ptr<Shardkv::Stub> stub = Shardkv::NewStub(grpc::CreateChannel(response.primary(), grpc::InsecureChannelCredentials()));        
        ::grpc::ClientContext cc;
        DumpResponse response;
        Empty request;
        lock.unlock();
        ::grpc::Status status = stub->Dump(&cc, request, &response);
        lock.lock();
        if (status.ok()) {
            for( const auto& kv : response.database() )
                this->_database.insert({kv.first, kv.second});
        } else{
            cerr<<"Transfer FAILED"<<endl;
        }
    }
    _viewnumber = response.id();
}


/**
 * PART 3 ONLY
 *
 * This method is called by a backup server when it joins the system for the firt time or after it crashed and restarted.
 * It allows the server to receive a snapshot of all key-value pairs stored by the primary server.
 *
 * @param context - you can ignore this
 * @param request An empty message
 * @param response the whole database
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvServer::Dump(::grpc::ServerContext* context, const Empty* request, ::DumpResponse* response) {
    lock_guard<mutex> lock(*_mutex);
    response->mutable_database()->insert(_database.begin(), _database.end());
    return ::grpc::Status::OK;
}
