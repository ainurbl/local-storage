#include "kv.pb.h"
#include "log.h"
#include "protocol.h"
#include "rpc.h"

#include <array>
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <string>

#include <errno.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <resolv.h>
#include <sys/epoll.h>
#include <sys/socket.h>

static_assert(EAGAIN == EWOULDBLOCK);

using namespace NLogging;
using namespace NProtocol;
using namespace NRpc;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr int max_events = 32;
constexpr int timeout = 1000;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv) {
    /*
     * simplistic arg parsing
     * TODO proper argparse lib
     */

    if (argc < 3) {
        return 1;
    }
    const auto port = atoi(argv[1]);
    const auto max_requests = atoi(argv[2]);
    std::vector<std::string> stages;

    for (int i = 3; i < argc; ++i) {
        stages.push_back(argv[i]);
    }

    if (stages.empty()) {
        stages = {"put", "get"};
    }

    /*
     * socket initialization
     */

    int socketfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (socketfd == -1) {
        return 1;
    }

    int epollfd = epoll_create1(0);
    if (epollfd == -1) {
        LOG_ERROR("epoll_create1 failed");
        return 1;
    }

    struct epoll_event event;
    event.events = EPOLLIN | EPOLLOUT | EPOLLET;
    event.data.fd = socketfd;
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, socketfd, &event) == -1) {
        LOG_ERROR("epoll_ctl failed");
        return 1;
    }

    struct sockaddr_in dest;
    bzero(&dest, sizeof(dest));
    dest.sin_family = AF_INET;
    dest.sin_port = htons(port);

    if (inet_pton(AF_INET, "127.0.0.1", &dest.sin_addr.s_addr) == 0) {
        perror("failed to convert address");
        return errno;
    }

    if (connect(socketfd, (struct sockaddr*)&dest, sizeof(dest)) != 0) {
        if (errno != EINPROGRESS) {
            perror("failed to connect");
            return errno;
        }
    }

    SocketState state;
    state.fd = socketfd;

    uint64_t request_count = 0;
    std::unordered_map <uint64_t, uint64_t> expected_gets;

    if (std::strcmp(argv[2], "get_inserted") == 0) {
        if (argc < 4) {
            LOG_ERROR("failed get_inserted request: format is ./client [PORT] get_inserted [KEY1] [KEY2] ...");
            return 1;
        }
        for (int i = 3; i < argc; ++i) {
            NProto::TGetInsertedRequest get_inserted_request;
            get_inserted_request.set_request_id(request_count++);
            get_inserted_request.set_key(argv[i]);

            std::stringstream message;
            serialize_header(GET_INSERTED_REQUEST, get_inserted_request.ByteSizeLong(), message);
            get_inserted_request.SerializeToOstream(&message);

            state.output_queue.push_back(message.str());
        }

    } else {

        /*
         * generating requests
         */

        auto generate_data = [](int i) {
            return i * 4;
        };

        auto generate_string = [](int i) {
            return std::to_string(i) + std::string(1000, 'z');
        };

        auto stage_put = [&]() {
            for (int i = 0; i < max_requests; ++i) {
                std::stringstream key;
                key << "key" << i;

                NProto::TPutRequest put_request;
                put_request.set_request_id(request_count++);
                put_request.set_key(key.str());
                put_request.set_offset(generate_data(i));

                std::stringstream message;
                serialize_header(PUT_REQUEST, put_request.ByteSizeLong(), message);
                put_request.SerializeToOstream(&message);

                state.output_queue.push_back(message.str());
            }
        };

        auto stage_get = [&]() {
            for (int i = 0; i < max_requests; ++i) {
                std::stringstream key;
                key << "key" << i;

                NProto::TGetRequest get_request;
                get_request.set_request_id(request_count++);
                get_request.set_key(key.str());
                expected_gets[get_request.request_id()] = generate_data(i);

                std::stringstream message;
                serialize_header(GET_REQUEST, get_request.ByteSizeLong(), message);
                get_request.SerializeToOstream(&message);

                state.output_queue.push_back(message.str());
            }
        };

        auto stage_insert = [&]() {
            for (int i = 0; i < max_requests; ++i) {
                std::stringstream key;
                key << "key" << i;

                NProto::TInsertRequest insert_request;
                insert_request.set_request_id(request_count++);
                insert_request.set_key(key.str());
                insert_request.set_val(generate_string(i));

                std::stringstream message;
                serialize_header(INSERT_REQUEST, insert_request.ByteSizeLong(), message);
                insert_request.SerializeToOstream(&message);

                state.output_queue.push_back(message.str());
            }
        };

        std::unordered_map <std::string, std::function<void()>> stage2func = {
                {"put",    stage_put},
                {"get",    stage_get},
                {"insert", stage_insert},
        };

        for (const auto &stage: stages) {
            stage2func.at(stage)();
        }
    }

    /*
     * handler function
     */

    int response_count = 0;

    auto handle_get_inserted = [&] (const std::string& response) {
        NProto::TGetInsertedResponse get_inserted_response;
        if (!get_inserted_response.ParseFromArray(response.data(), response.size())) {
            // TODO proper handling

            abort();
        }

        LOG_DEBUG_S("get_inserted_response: " << get_inserted_response.ShortDebugString());

        std::cout << get_inserted_response.value() << std::endl;

        ++response_count;

        return std::string();
    };

    auto handle_get = [&] (const std::string& response) {
        NProto::TGetResponse get_response;
        if (!get_response.ParseFromArray(response.data(), response.size())) {
            // TODO proper handling

            abort();
        }

        LOG_DEBUG_S("get_response: " << get_response.ShortDebugString());

        auto it = expected_gets.find(get_response.request_id());
        if (it == expected_gets.end()) {
            LOG_ERROR_S("unexpected get request_id "
                << get_response.request_id());
        } else if (it->second != get_response.offset()) {
            LOG_ERROR_S("unexpected data for get request_id "
                << get_response.request_id()
                << ", actual " << get_response.offset()
                << ", expected " << it->second);
        }

        ++response_count;

        return std::string();
    };

    auto handle_put = [&] (const std::string& response) {
        NProto::TPutResponse put_response;
        if (!put_response.ParseFromArray(response.data(), response.size())) {
            // TODO proper handling

            abort();
        }

        LOG_DEBUG_S("put_response: " << put_response.ShortDebugString());

        ++response_count;

        return std::string();
    };

    auto handle_insert = [&] (const std::string& response) {
        NProto::TInsertResponse insert_response;
        if (!insert_response.ParseFromArray(response.data(), response.size())) {
            // TODO proper handling

            abort();
        }

        LOG_DEBUG_S("insert_response: " << insert_response.ShortDebugString());

        ++response_count;

        return std::string();
    };

    Handler handler = [&] (char message_type, const std::string& response) {
        switch (message_type) {
            case PUT_RESPONSE: return handle_put(response);
            case GET_RESPONSE: return handle_get(response);
            case INSERT_RESPONSE: return handle_insert(response);
            case GET_INSERTED_RESPONSE: return handle_get_inserted(response);
        }

        // TODO proper handling

        abort();
        return std::string();
    };

    /*
     * rpc state and event loop
     */

    std::array<struct epoll_event, ::max_events> events;

    int num_ready = epoll_wait(epollfd, events.data(), max_events, timeout);
    for (int i = 0; i < num_ready; i++) {
        if (events[i].events & EPOLLIN) {
            VERIFY(events[i].data.fd == socketfd, "fd mismatch");

            LOG_INFO_S("socket " << socketfd << " connected");
        }
    }

    if (!process_output(state)) {
        LOG_ERROR("failed to send request");
        return 3;
    }

    while (response_count < request_count) {
        num_ready = epoll_wait(epollfd, events.data(), max_events, timeout);
        for (int i = 0; i < num_ready; i++) {
            VERIFY(events[i].data.fd == socketfd, "fd mismatch");

            if (events[i].events & EPOLLIN) {
                if (!process_input(state, handler)) {
                    LOG_ERROR("failed to read response");
                    return 2;
                }
            }

            if ((events[i].events & EPOLLOUT)) {
                if (!process_output(state)) {
                    LOG_ERROR("failed to send request");
                    return 3;
                }
            }
        }
    }

    close(socketfd);

    return 0;
}
