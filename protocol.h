#pragma once

#include "log.h"

#include <cstring>
#include <ostream>
#include <string>

namespace NProtocol {

////////////////////////////////////////////////////////////////////////////////

constexpr char PUT_REQUEST = 1U;
constexpr char PUT_RESPONSE = 2U;
constexpr char GET_REQUEST = 3U;
constexpr char GET_RESPONSE = 4U;
constexpr char INSERT_REQUEST = 5U;
constexpr char INSERT_RESPONSE = 6U;
constexpr char GET_INSERTED_REQUEST = 7U;
constexpr char GET_INSERTED_RESPONSE = 8U;

struct Message
{
    char message_type = 0;
    uint32_t len = 0;
    uint32_t len_bytes = 0;
    std::string buffer;

    size_t to_read() const
    {
        if (!message_type) {
            return 1;
        }

        if (len_bytes < 4) {
            return 4 - len_bytes;
        }

        return len - buffer.size();
    }

    void on_data(char* buf, size_t size)
    {
        if (!message_type) {
            VERIFY(size == 1, "unexpected message_type size");

            message_type = buf[0];
        } else if (len_bytes < 4) {
            VERIFY(size <= 4 - len_bytes, "unexpected len size");

            // XXX not portable
            memcpy(reinterpret_cast<char*>(&len) + len_bytes, buf, size);
            len_bytes += size;

            if (len_bytes == 4) {
                buffer.reserve(len);
            }
        } else {
            VERIFY(size <= len - buffer.size(), "unexpected len size");

            auto old_size = buffer.size();
            buffer.resize(old_size + size);
            memcpy(const_cast<char*>(buffer.data()) + old_size, buf, size);
        }
    }

    bool is_complete() const
    {
        return len_bytes == 4 && to_read() == 0;
    }

    void reset()
    {
        message_type = 0;
        len_bytes = 0;
        len = 0;
        buffer.clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

inline void serialize_header(char message_type, uint32_t len, std::ostream& out)
{
    out.write(&message_type, 1);
    out.write(reinterpret_cast<const char*>(&len), 4);
}

}   // namespace NProtocol
