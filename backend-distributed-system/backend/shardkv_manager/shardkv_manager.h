#ifndef SHARDING_SHARDKV_MANAGER_H
#define SHARDING_SHARDKV_MANAGER_H

#include <grpcpp/grpcpp.h>
#include <thread>
#include "../common/common.h"
#include <unordered_map>
#include <mutex>
#include <iostream>
#include <fstream>

#include "../build/shardkv.grpc.pb.h"
#include "../build/shardmaster.grpc.pb.h"


class PingInterval {
    std::chrono::time_point<std::chrono::system_clock> time;
public:
    std::uint64_t GetPingInterval(){
        return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()-time.time_since_epoch()).count();
    };
    void Push(std::chrono::time_point<std::chrono::system_clock> t){
        time = t;
    }
};

class ShardkvManager : public Shardkv::Service {
  using Empty = google::protobuf::Empty;

 public:
  explicit ShardkvManager(std::string addr, const std::string& shardmaster_addr)
      : address(std::move(addr)),
        sm_address(shardmaster_addr),
        _mutex(std::make_shared<std::mutex>()),
        _views{{"",""}},
        _acknowledged{0} {
      // TODO: Part 3
      // This thread will check for last shardkv server ping and update the view accordingly if needed
      std::thread heartbeatChecker(
              [this]() {
                  std::chrono::milliseconds timespan(1000);
                  const std::uint64_t dead_interval = 3000;
                  while (true) {
                      std::this_thread::sleep_for(timespan);
                      std::lock_guard<std::mutex> lock(*this->_mutex);
                      if (_current_primary().empty()) continue;
                      bool primary_dead = !_current_primary().empty() && _last_ping[_current_primary()].GetPingInterval() > dead_interval;
                      bool backup_dead = !_current_backup().empty() && _last_ping[_current_backup()].GetPingInterval() > dead_interval;
                      if (primary_dead) {
                        std::cerr<<"Primary "<<_current_primary()<<" dead at "<<_last_ping[_current_primary()].GetPingInterval()<<std::endl;
                        assert(_current == _acknowledged);
                        _views.push_back({_views.back().begin()+1, _views.back().end()});
                        if(_views.back().size() == 1)
                            _views.back().push_back("");
                        _current++;
                        _primary_stub = Shardkv::NewStub(grpc::CreateChannel(_current_primary(), grpc::InsecureChannelCredentials()));
                      } else if (backup_dead) {
                        std::cerr<<"Backup "<<_current_backup()<<" dead at "<<_last_ping[_current_backup()].GetPingInterval()<<std::endl;
                        assert(_current == _acknowledged);
                        _views.push_back(_views.back());
                        _views.back().erase(_views.back().begin()+1);
                        if(_views.back().size() == 1)
                            _views.back().push_back("");
                        _current++;
                      }
                  }
              });
      // We detach the thread so we don't have to wait for it to terminate later
      heartbeatChecker.detach();
  };

  // TODO implement these three methods
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
  ::grpc::Status Ping(::grpc::ServerContext* context, const PingRequest* request,
                        ::PingResponse* response) override;

 private:
    // address we're running on (hostname:port)
    const std::string address;

    // shardmaster address
    std::string sm_address;

    // TODO add any fields you want here!
    
    // stub for primary server
    std::unique_ptr<Shardkv::Stub> _primary_stub;
    // mutex for primary server
    std::shared_ptr<std::mutex> _mutex;
    // vector of views (each view is a vector like [primary, backup, idle0, idle1, ...])
    std::vector<std::vector<std::string>> _views;
    // currently returned view
    std::size_t _current;
    // last acknowledged view
    std::size_t _acknowledged;
    // map of last ping time for each server
    std::unordered_map<std::string, PingInterval> _last_ping;

    inline const std::string& _current_primary() { return _views[_current][0]; }
    inline const std::string& _current_backup() { return _views[_current][1]; }
    inline const std::string& _acked_primary() { return _views[_acknowledged][0]; }
    inline const std::string& _acked_backup() { return _views[_acknowledged][1]; }
    inline const std::size_t _latest() { return _views.size() - 1; }
    inline const std::string& _latest_primary() { return _views.back()[0]; }
    inline const std::string& _latest_backup() { return _views.back()[1]; }
};
#endif  // SHARDING_SHARDKV_MANAGER_H
