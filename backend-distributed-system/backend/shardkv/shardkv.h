#ifndef SHARDING_SHARDKV_H
#define SHARDING_SHARDKV_H

#include <grpcpp/grpcpp.h>
#include <thread>
#include "../common/common.h"
#include <unordered_map>
#include <mutex>
#include <iostream>
#include <fstream>
#include <unordered_set>
#include <map>

#include "../build/shardkv.grpc.pb.h"
#include "../build/shardmaster.grpc.pb.h"

class ShardkvServer : public Shardkv::Service {
  using Empty = google::protobuf::Empty;

 public:
  explicit ShardkvServer(std::string addr, const std::string& shardmanager_addr)
      : address(std::move(addr)),
        shardmanager_address(shardmanager_addr),
        _mutex(std::make_shared<std::mutex>()) {

    // This thread will query the shardmaster every 100 milliseconds for updates
    std::thread query(
            [this]() {
                // TODO: Assignment 2 Implement the QueryShardmaster(...) function
                std::chrono::milliseconds timespan(100);
                while (shardmaster_address.empty()) {
                    std::this_thread::sleep_for(timespan);
                }
                auto stub = Shardmaster::NewStub(
                        grpc::CreateChannel(shardmaster_address, grpc::InsecureChannelCredentials()));
                while (true) {
                    this->QueryShardmaster(stub.get());
                    std::this_thread::sleep_for(timespan);
                }
            }
            );
    // we detach the thread so we don't have to wait for it to terminate later
    query.detach();


    std::thread heartbeat(
        [this](const std::string sm_addr) {
            // TODO: Assignment 2 - Implement PingShardmanager(...) function
            // TODO: Assignment 3 - Extends PingShardmanager(...) function as described in the instructions
            std::chrono::milliseconds timespan(100);
            auto stub = Shardkv::NewStub(
                    grpc::CreateChannel(sm_addr, grpc::InsecureChannelCredentials()));
            while (true) {
                PingShardmanager(stub.get());
                std::this_thread::sleep_for(timespan);
            }
        },
        shardmanager_addr);
    // we detach the thread so we don't have to wait for it to terminate later
    heartbeat.detach();
  };


  // TODO implement these three methods, should be fairly similar to your simple_shardkv
  ::grpc::Status Get(::grpc::ServerContext* context,
                     const ::GetRequest* request,
                     ::GetResponse* response) override;
  ::grpc::Status Put(::grpc::ServerContext* context,
                     const ::PutRequest* request, Empty* response) override;
  ::grpc::Status Append(::grpc::ServerContext* context,
                        const ::AppendRequest* request,
                        Empty* response) override;
  ::grpc::Status Delete(::grpc::ServerContext* context,
                        const ::DeleteRequest* request,
                        Empty* response) override;
    ::grpc::Status Dump(::grpc::ServerContext* context,
                        const ::google::protobuf::Empty* request,
                        ::DumpResponse* response);

  // TODO this will be called in a separate thread, here is where you want to
  // query the shardmaster for configuration updates and respond to changes
  // appropriately (i.e. transferring keys, no longer serving keys, etc.)
  void QueryShardmaster(Shardmaster::Stub* stub);

  // TODO this will be called in a separate thread, here is where you want to
  // ping the shardmanager to get updates about the sharmaster (part 2) and the views changes (part 3)
  void PingShardmanager(Shardkv::Stub* stub);

 private:
  // address we're running on (hostname:port)
  const std::string address;
  // address of shardmanager passed as constructor's parameter
  std::string shardmanager_address;
  // address of shardmaster sent by the shardmanager
  std::string shardmaster_address;

  // TODO add any fields you want here!
  // to manage concurrent access to resources
  std::shared_ptr<std::mutex> _mutex;
  // map of key value pairs
  std::unordered_map<std::string, std::string> _database;
  // vector of shards mapping keys assignments
  std::map<shard_t, std::string> _keys_assignments;
  // map of posts to users ids
  std::unordered_map<std::string, std::string> _authors;
  // last view number
  std::size_t _viewnumber;
  bool _is_primary;
  std::string _backup_address;
  std::unique_ptr<Shardkv::Stub> _stub_to_backup;


  // tell if this server manages a key
  bool _manages_key(const std::string& key);
  // get the server which is in charge of managing a key
  std::string _server_of(const std::string& key);

  bool _key_is_for_user(const std::string& key);
  bool _key_is_for_post(const std::string& key);
  std::string _remove_user(std::string& users, const std::string& user);
};

#endif  // SHARDING_SHARDKV_H
