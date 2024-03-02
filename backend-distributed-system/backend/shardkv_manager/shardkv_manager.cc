#include <grpcpp/grpcpp.h>
#include <iostream>
#include <algorithm>

#include "shardkv_manager.h"
using namespace std;

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
::grpc::Status ShardkvManager::Get(::grpc::ServerContext* context,
                                  const ::GetRequest* request,
                                  ::GetResponse* response) {
    lock_guard<mutex> lock(*_mutex);
    if( _primary_stub == nullptr){
        return ::grpc::Status(::grpc::StatusCode::UNAVAILABLE, "No primary server");
    }
    ::grpc::ClientContext cc;
    cerr<<"Forwarding get to "<<_current_primary()<<endl;
    return _primary_stub->Get(&cc, *request, response);
}

/**
 * Insert the given key-value mapping into our store such that future gets will
 * retrieve it
 * If the item already exists, you must r eplace its previous value.
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
::grpc::Status ShardkvManager::Put(::grpc::ServerContext* context,
                                  const ::PutRequest* request,
                                  Empty* response) {
    lock_guard<mutex> lock(*_mutex);
    if( _primary_stub == nullptr){
        return ::grpc::Status(::grpc::StatusCode::UNAVAILABLE, "No primary server");
    }
    ::grpc::ClientContext cc;
    // cerr<<address<<" forwarding put to "<<_primary_address<<endl;
    ::grpc::Status result_status = _primary_stub->Put(&cc, *request, response);
    if (!result_status.ok()){
        //cerr<<"shardmaster "<<address<<" received error from primary "<<_current_primary()<<"  >>  "<<result_status.error_message()<<" | key was "<<request->key()<<endl;
    }
    return result_status;
}

/**
 * Appends the data in the request to whatever data the specified key maps to.
 * If the key is not mapped to anything, this method should be equivalent to a
 * put for the specified key and value. If the server is not responsible for the
 * specified key, this function should fail.
 *
 * @param context - you can ignore this
 * @param request A message containngi a key-value pair
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>"
 */
::grpc::Status ShardkvManager::Append(::grpc::ServerContext* context,
                                     const ::AppendRequest* request,
                                     Empty* response) {
    lock_guard<mutex> lock(*_mutex);
    if( _primary_stub == nullptr){
        return ::grpc::Status(::grpc::StatusCode::UNAVAILABLE, "No primary server");
    }
    ::grpc::ClientContext cc;
    return _primary_stub->Append(&cc, *request, response);
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
::grpc::Status ShardkvManager::Delete(::grpc::ServerContext* context,
                                           const ::DeleteRequest* request,
                                           Empty* response) {
    lock_guard<mutex> lock(*_mutex);
    if( _primary_stub == nullptr) {
        return ::grpc::Status(::grpc::StatusCode::UNAVAILABLE, "No primary server");
    }
    ::grpc::ClientContext cc;
    return _primary_stub->Delete(&cc, *request, response);
}

/**
 * In part 2, this function get address of the server sending the Ping request, who became the primary server to which the
 * shardmanager will forward Get, Put, Append and Delete requests. It answer with the name of the shardmaster containeing
 * the information about the distribution.
 *
 * @param context - you can ignore this
 * @param request A message containing the name of the server sending the request, the number of the view acknowledged
 * @param response The current view and the name of the shardmaster
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvManager::Ping(::grpc::ServerContext* context, const PingRequest* request,
                                       ::PingResponse* response){
    size_t view_number = request->viewnumber();
    string server_name = request->server();

    lock_guard<mutex> lock(*_mutex);
    response->set_shardmaster(sm_address);

    if (_current == 0) {
        // first time ping is called
        _views.push_back({server_name, ""});
        _current++;
        _primary_stub = Shardkv::NewStub(grpc::CreateChannel(server_name, grpc::InsecureChannelCredentials()));
        response->set_primary(_current_primary());
        response->set_backup(_current_backup());
    } else if (server_name == _current_primary()) {
        // ping from primary
        // update acknowledged view
        // if(_acknowledged < view_number)
        //     cerr<<address<<" ACKED "<<view_number<<endl;
        if(view_number > _acknowledged)
            _acknowledged = view_number;
        // cerr<<address<<" ACKED = "<<_acknowledged<<" CUR = "<<_current<<" VN = "<<view_number<<endl;
        if (_acknowledged == _current) {
            // update current view if there are more recent views to acknowledge
            if (_current < _latest()) {
                _current++;
            }
            // send current view
            response->set_primary(_current_primary());
            response->set_backup(_current_backup());
        } else {
            // means that primary died and backup took over
            response->set_primary(_current_primary());
            response->set_backup(_current_backup());
        }
    } else if (_current_backup().empty()) {
        // ping from idle server while there is no backup
        if (_current < _latest()) {
            // if there are more recent views to acknowledge put the backup in that view
            _views[_latest()][1] = server_name;
        } else {
            // there are no more recent views to acknowledge, create a new view with the backup
            _views.push_back({_current_primary(), server_name});
        }
        // still send the last current view
        response->set_primary(_current_primary());
        response->set_backup(_current_backup());
    } else if (server_name == _current_backup()) {
        // ping from backup
        // still send the last current view
        response->set_primary(_current_primary());
        response->set_backup(_current_backup());
    } else {
        // ping from an idle server
        if (find(_views[_latest()].begin(), _views[_latest()].end(), server_name) != _views[_latest()].end()) {
            // if not added yet
            if (_current < _latest()) {
                // if there are more recent views to acknowledge put the idle server in that view
                _views[_latest()].push_back(server_name);
            } else {
                // there are no more recent views to acknowledge, create a new view with the idle server
                _views.push_back({_current_primary(), _current_backup(), server_name});
            }
        }
        response->set_primary(_current_primary());
        response->set_backup(_current_backup());
    }
    response->set_id(_current);
    // cerr<<"RES FOR "<<server_name<<" = "<<_current<<endl;
    _last_ping[server_name].Push(chrono::high_resolution_clock::now());
    return ::grpc::Status(::grpc::StatusCode::OK, sm_address);
}

