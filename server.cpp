#include "kv.pb.h"
#include "log.h"
#include "protocol.h"
#include "rpc.h"

#include <array>
#include <cstdio>
#include <cstring>
#include <csignal>
#include <cerrno>
#include <sstream>
#include <string>
#include <fstream>
#include <thread>
#include <chrono>
#include <unordered_map>

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdio.h>
#include <unistd.h>

#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>

static_assert(EAGAIN
== EWOULDBLOCK);

using namespace NLogging;
using namespace NProtocol;
using namespace NRpc;

namespace {

////////////////////////////////////////////////////////////////////////////////

    constexpr int max_events = 32;

////////////////////////////////////////////////////////////////////////////////

    auto create_and_bind(std::string const &port) {
        struct addrinfo hints;

        memset(&hints, 0, sizeof(struct addrinfo));
        hints.ai_family = AF_UNSPEC; /* Return IPv4 and IPv6 choices */
        hints.ai_socktype = SOCK_STREAM; /* TCP */
        hints.ai_flags = AI_PASSIVE; /* All interfaces */

        struct addrinfo *result;
        int sockt = getaddrinfo(nullptr, port.c_str(), &hints, &result);
        if (sockt != 0) {
            LOG_ERROR("getaddrinfo failed");
            return -1;
        }

        struct addrinfo *rp = nullptr;
        int socketfd = 0;
        for (rp = result; rp != nullptr; rp = rp->ai_next) {
            socketfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
            if (socketfd == -1) {
                continue;
            }

            sockt = bind(socketfd, rp->ai_addr, rp->ai_addrlen);
            if (sockt == 0) {
                break;
            }

            close(socketfd);
        }

        if (rp == nullptr) {
            LOG_ERROR("bind failed");
            return -1;
        }

        freeaddrinfo(result);

        return socketfd;
    }

////////////////////////////////////////////////////////////////////////////////

    auto make_socket_nonblocking(int socketfd) {
        int flags = fcntl(socketfd, F_GETFL, 0);
        if (flags == -1) {
            LOG_ERROR("fcntl failed (F_GETFL)");
            return false;
        }

        flags |= O_NONBLOCK;
        int s = fcntl(socketfd, F_SETFL, flags);
        if (s == -1) {
            LOG_ERROR("fcntl failed (F_SETFL)");
            return false;
        }

        return true;
    }

////////////////////////////////////////////////////////////////////////////////

    SocketStatePtr invalid_state()
    {
        return std::make_shared<SocketState>();
    }

    SocketStatePtr accept_connection(
            int socketfd,
            struct epoll_event &event,
            int epollfd) {
        struct sockaddr in_addr;
        socklen_t in_len = sizeof(in_addr);
        int infd = accept(socketfd, &in_addr, &in_len);
        if (infd == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return nullptr;
            } else {
                LOG_PERROR("accept failed with error");
                return invalid_state();
            }
        }

        std::string hbuf(NI_MAXHOST, '\0');
        std::string sbuf(NI_MAXSERV, '\0');
        auto ret = getnameinfo(
                &in_addr, in_len,
                const_cast<char *>(hbuf.data()), hbuf.size(),
                const_cast<char *>(sbuf.data()), sbuf.size(),
                NI_NUMERICHOST | NI_NUMERICSERV);

        if (ret == 0) {
            LOG_INFO_S("accepted connection on fd " << infd
                                                    << "(host=" << hbuf << ", port=" << sbuf << ")");
        }

        if (!make_socket_nonblocking(infd)) {
            LOG_PERROR("make_socket_nonblocking failed");
            return invalid_state();
        }

        event.data.fd = infd;
        event.events = EPOLLIN | EPOLLOUT | EPOLLET;
        if (epoll_ctl(epollfd, EPOLL_CTL_ADD, infd, &event) == -1) {
            LOG_PERROR("epoll_ctl failed");
            return invalid_state();
        }

        auto state = std::make_shared<SocketState>();
        state->fd = infd;
        return state;
    }

}   // namespace

////////////////////////////////////////////////////////////////////////////////

constexpr uint64_t
PARTITION_COUNT = 1; // must be power of 2
constexpr uint64_t
PARTITION_SIZE = UINT64_MAX / PARTITION_COUNT;
constexpr uint64_t
P = 53;

volatile std::sig_atomic_t running = 1;

inline uint64_t Hash(const std::string &str) {
    uint64_t res = 0;
    for (const auto &ch: str) {
        res += ch;
        res *= P;
    }
    return res;
}

inline uint64_t Hash(std::string &&str) {
    uint64_t res = 0;
    for (auto &&ch: str) {
        res *= P;
        res += (uint64_t)
        ch;
    }
    return res;
}

inline uint64_t GetPartitionId(uint64_t
val) {
return val /
PARTITION_SIZE;
}

class TFileHashMap {
public:
    TFileHashMap() = default;

    void Init(const std::string &fileNameTemplate, int32_t id) {
        PartitionId = id;
        CurrentOutId = 0;
        std::string key;
        uint64_t value;
        In.open(fileNameTemplate + std::to_string(PartitionId) + "_" + std::to_string(CurrentOutId),
                std::ios_base::in);
        if (In >> key >> value) {
            Storage[0][key] = value;
            Storage[1][key] = value;
        } else {
            CurrentOutId ^= 1;
            In.close();
            In.open(fileNameTemplate + std::to_string(PartitionId) + "_" + std::to_string(CurrentOutId),
                    std::ios_base::in);
        }
        while (In >> key >> value) {
            Storage[0][key] = value;
            Storage[1][key] = value;
        }
        In.close();

        LogPutsIn.open(fileNameTemplate + std::to_string(PartitionId) + "_" + std::to_string(CurrentOutId) + "_log",
                std::ios_base::in);
        while (LogPutsIn >> key >> value) {
            Storage[0][key] = value;
            Storage[1][key] = value;
        }
        LogPutsIn.close();

        LogOut[CurrentOutId].open(fileNameTemplate + std::to_string(PartitionId) + "_" + std::to_string(CurrentOutId) + "_log",
                               std::ios_base::out | std::ios_base::trunc);
        LogOut[CurrentOutId ^ 1].open(
                fileNameTemplate + std::to_string(PartitionId) + "_" + std::to_string(CurrentOutId ^ 1) + "_log",
                std::ios_base::out | std::ios_base::trunc);

        Out[CurrentOutId].open(fileNameTemplate + std::to_string(PartitionId) + "_" + std::to_string(CurrentOutId),
                               std::ios_base::out | std::ios_base::trunc);
        Out[CurrentOutId ^ 1].open(
                fileNameTemplate + std::to_string(PartitionId) + "_" + std::to_string(CurrentOutId ^ 1),
                std::ios_base::out | std::ios_base::trunc);
        for (auto &entry: Storage[CurrentOutId]) {
            Out[CurrentOutId] << entry.first << " " << entry.second << " ";
        }

        Flusher = std::thread([&]() {
            while (running) {
                std::this_thread::sleep_for(std::chrono::milliseconds(2000));
                int32_t newOutId = CurrentOutId ^ 1;
                CurrentOutId = newOutId;
                int32_t oldOutId = CurrentOutId ^ 1;

                for (auto &entry: Storage[oldOutId]) {
                    Out[CurrentOutId] << entry.first << " " << entry.second << " ";
                    Out[CurrentOutId].flush();
                }

                LogOut[oldOutId].close();
                LogOut[oldOutId].open(fileNameTemplate + std::to_string(PartitionId) + "_" + std::to_string(oldOutId) + "_log",
                                   std::ios_base::out | std::ios_base::trunc);

                Out[oldOutId].close();
                Out[oldOutId].open(fileNameTemplate + std::to_string(PartitionId) + "_" + std::to_string(oldOutId),
                                   std::ios_base::out | std::ios_base::trunc);
            }
        });
    }

    void Put(const std::string &key, uint64_t value) {
        Storage[CurrentOutId][key] = value;
        LogOut[CurrentOutId] << key << " " << value << "\n";
        LogOut[CurrentOutId].flush();
    }

    bool Find(const std::string &key, uint64_t *value) {
        auto otherVersion = CurrentOutId ^ 1;
        if (Storage[CurrentOutId].find(key) == Storage[CurrentOutId].end()
            && Storage[otherVersion].find(key) == Storage[otherVersion].end()) {
            return false;
        }
        if (Storage[CurrentOutId].count(key)) {
            *value = Storage[CurrentOutId][key];
        } else {
            *value = Storage[otherVersion][key];
        }
        return true;
    }

    void Close() {
        Out[0].close();
        Out[1].close();
        LogOut[0].flush();
        LogOut[1].flush();
    }

    void Flush() {
        Flusher.join();
        Close();
    }
private:
    std::unordered_map <std::string, uint64_t> Storage[2];
    std::ifstream In;
    std::ifstream LogPutsIn;
    std::ofstream Out[2];
    std::ofstream LogOut[2];
    int32_t CurrentOutId;
    int32_t PartitionId;
    std::thread Flusher;
};

class TConcurrentHashMap {
public:
    explicit TConcurrentHashMap() {
        uint64_t total = 0;
        for (int32_t i = 0; i < PARTITION_COUNT; ++i) {
            Maps[i].Init(MAP_NAME_TEMPLATE, i);
            auto lastSize = 0;
            total += lastSize;
            LOG_INFO(std::to_string(lastSize) + " elements got from cold start at shard_id=" + std::to_string(i));
        }
        LOG_INFO("Total: " + std::to_string(total) + " elements");
    }

    ~TConcurrentHashMap() {
        uint64_t totalSize = 0;
        for (uint64_t i = 0; i < PARTITION_COUNT; ++i) {
            Maps[i].Flush();
            Maps[i].Close();
            totalSize += 0;
        }
        LOG_INFO("Before destructor: " + std::to_string(totalSize) + " elements");
    }

    void Put(const std::string &key, uint64_t value) {
        Maps[GetId(key)].Put(key, value);
    }

    bool Find(const std::string &key, uint64_t *value) {
        return Maps[GetId(key)].Find(key, value);
    }

private:
    uint64_t GetId(const std::string &key) {
        return GetPartitionId(Hash(key));
    }

    uint64_t GetId(std::string &&key) {
        return GetPartitionId(Hash(key));
    }

    std::string MAP_NAME_TEMPLATE = "bin_map_";

    TFileHashMap Maps[PARTITION_COUNT];
};

class TKeyValueStorage {
public:
    explicit TKeyValueStorage() {
        f = fopen(PATH.c_str(), "ab+");
    }

    ~TKeyValueStorage() {
        fclose(f);
    }

    void Put(const std::string &key, const std::string &value) {
        uint64_t sz = value.size();
        uint64_t offset = ftell(f);
        fwrite(&sz, sizeof(uint64_t), 1, f);
        fwrite(value.c_str(), sizeof(char), sz, f);
        Map.Put(key, offset);
    }

    bool Get(const std::string &key, std::string *value) {
        uint64_t offset = 0;
        if (Map.Find(key, &offset)) {
            *value = ReadValue(offset);
            return true;
        }
        return false;
    }

private:
    std::string ReadValue(uint64_t offset) {
        fseek(f, offset, SEEK_SET);
        uint64_t sz;
        fread(&sz, sizeof(uint64_t), 1, f);
        std::string ret(sz, 0);
        fread(&ret[0], sizeof(char), sz, f);
        fseek(f, 0, SEEK_END);
        return ret;
    }

    const std::string PATH = "values.bin";
    FILE *f;
    TConcurrentHashMap Map;
};

void signal_handler(int) {
    running = 0;
}

int main(int argc, const char **argv) {
    signal(SIGINT, signal_handler);

    if (argc < 2) {
        return 1;
    }

    /*
     * socket creation and epoll boilerplate
     * TODO extract into struct Bootstrap
     */

    auto socketfd = ::create_and_bind(argv[1]);
    if (socketfd == -1) {
        return 1;
    }

    if (!::make_socket_nonblocking(socketfd)) {
        return 1;
    }

    if (listen(socketfd, SOMAXCONN) == -1) {
        LOG_ERROR("listen failed");
        return 1;
    }

    int epollfd = epoll_create1(0);
    if (epollfd == -1) {
        LOG_ERROR("epoll_create1 failed");
        return 1;
    }

    struct epoll_event event;
    event.data.fd = socketfd;
    event.events = EPOLLIN | EPOLLET;
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, socketfd, &event) == -1) {
        LOG_ERROR("epoll_ctl failed");
        return 1;
    }

    /*
     * handler function
     */

//    TConcurrentHashMap concurrentHashMap;
    TKeyValueStorage db;

//    auto handle_get = [&](const std::string &request) {
//        NProto::TGetRequest get_request;
//        if (!get_request.ParseFromArray(request.data(), request.size())) {
//            // TODO proper handling
//
//            abort();
//        }
//
//        LOG_DEBUG_S("get_request: " << get_request.ShortDebugString());
//
//        NProto::TGetResponse get_response;
//        get_response.set_request_id(get_request.request_id());
//
//        uint64_t offset;
//        if (concurrentHashMap.Find(get_request.key(), &offset)) {
//            get_response.set_offset(offset);
//        }
//
//        std::stringstream response;
//        serialize_header(GET_RESPONSE, get_response.ByteSizeLong(), response);
//        get_response.SerializeToOstream(&response);
//
//        return response.str();
//    };
//
//    auto handle_put = [&](const std::string &request) {
//        NProto::TPutRequest put_request;
//        if (!put_request.ParseFromArray(request.data(), request.size())) {
//            // TODO proper handling
//
//            abort();
//        }
//
//        LOG_DEBUG_S("put_request: " << put_request.ShortDebugString());
//
//        concurrentHashMap.Put(put_request.key(), put_request.offset());
//
//        NProto::TPutResponse put_response;
//        put_response.set_request_id(put_request.request_id());
//
//        std::stringstream response;
//        serialize_header(PUT_RESPONSE, put_response.ByteSizeLong(), response);
//        put_response.SerializeToOstream(&response);
//
//        return response.str();
//    };

    auto handle_insert = [&](const std::string &request) {
        NProto::TInsertRequest insert_request;
        if (!insert_request.ParseFromArray(request.data(), request.size())) {
            // TODO proper handling

            abort();
        }

        LOG_DEBUG_S("insert_request: " << insert_request.ShortDebugString());

        db.Put(insert_request.key(), insert_request.val());

        NProto::TInsertResponse insert_response;
        insert_response.set_request_id(insert_request.request_id());

        std::stringstream response;
        serialize_header(INSERT_RESPONSE, insert_response.ByteSizeLong(), response);
        insert_response.SerializeToOstream(&response);

        return response.str();
    };

    auto handle_get_inserted = [&](const std::string &request) {
        NProto::TGetInsertedRequest get_inserted_request;
        if (!get_inserted_request.ParseFromArray(request.data(), request.size())) {
            // TODO proper handling

            abort();
        }

        LOG_DEBUG_S("get_inserted_request: " << get_inserted_request.ShortDebugString());

        NProto::TGetInsertedResponse get_inserted_response;
        get_inserted_response.set_request_id(get_inserted_request.request_id());

        std::string value;
        if (db.Get(get_inserted_request.key(), &value)) {
            get_inserted_response.set_value(value);
        }

        std::stringstream response;
        serialize_header(GET_INSERTED_RESPONSE, get_inserted_response.ByteSizeLong(), response);
        get_inserted_response.SerializeToOstream(&response);

        return response.str();
    };

    Handler handler = [&](char request_type, const std::string &request) {
        switch (request_type) {
//            case PUT_REQUEST:
//                return handle_put(request);
//            case GET_REQUEST:
//                return handle_get(request);
            case INSERT_REQUEST:
                return handle_insert(request);
            case GET_INSERTED_REQUEST:
                return handle_get_inserted(request);
        }

        // TODO proper handling

        abort();
        return std::string();
    };

    /*
     * rpc state and event loop
     * TODO extract into struct Rpc
     */

    std::array<struct epoll_event, ::max_events> events;
    std::unordered_map<int, SocketStatePtr> states;

    auto finalize = [&](int fd) {
        LOG_INFO_S("close " << fd);

        close(fd);
        states.erase(fd);
    };

    while (running) {
        const auto n = epoll_wait(epollfd, events.data(), ::max_events, -1);

        {
            LOG_INFO_S("got " << n << " events");
        }

        for (int i = 0; i < n; ++i) {
            const auto fd = events[i].data.fd;

            if (events[i].events & EPOLLERR
                || events[i].events & EPOLLHUP
                || !(events[i].events & (EPOLLIN | EPOLLOUT))) {
                LOG_ERROR_S("epoll event error on fd " << fd);

                finalize(fd);

                continue;
            }

            if (socketfd == fd) {
                while (true) {
                    auto state = ::accept_connection(socketfd, event, epollfd);
                    if (!state) {
                        break;
                    }

                    states[state->fd] = state;
                }

                continue;
            }

            bool closed = false;
            if (events[i].events & EPOLLIN) {
                auto state = states.at(fd);
                if (!process_input(*state, handler)) {
                    finalize(fd);
                    closed = true;
                }
            }

            if (events[i].events & EPOLLOUT && !closed) {
                auto state = states.at(fd);
                if (!process_output(*state)) {
                    finalize(fd);
                }
            }
        }
    }

    LOG_INFO("exiting");

    close(socketfd);

    return 0;
}
