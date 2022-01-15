#pragma once

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

namespace NLogging {

////////////////////////////////////////////////////////////////////////////////

enum class EVerbosity
{
    MIN = 0,
    ERROR = 1,
    WARN = 2,
    INFO = 3,
    DEBUG = 4,
    MAX,
};

struct LoggingEnv
{
    EVerbosity current_verbosity = EVerbosity::INFO;

    LoggingEnv()
    {
        if (auto value = std::getenv("VERBOSITY")) {
            auto v = atoi(value);
            if (v <= static_cast<int>(EVerbosity::MIN)
                    || v >= static_cast<int>(EVerbosity::MAX)) {
                abort();
            }

            current_verbosity = static_cast<EVerbosity>(v);
        }
    }
};

const LoggingEnv& logging_env();

////////////////////////////////////////////////////////////////////////////////

#define LOG(level, tag, message)                                               \
{                                                                              \
    if (NLogging::level <= NLogging::logging_env().current_verbosity) {        \
        const auto now = std::chrono::system_clock::now();                     \
        const auto tt = std::chrono::system_clock::to_time_t(now);             \
        static const auto format = "%Y-%m-%d %H:%M:%S";                        \
                                                                               \
        std::cerr << "[" << std::put_time(std::localtime(&tt), format)         \
            << " " << tag << "] " << (message) << "\n";                        \
    }                                                                          \
}                                                                              \
// LOG

////////////////////////////////////////////////////////////////////////////////

class LogMessage
{
private:
    std::stringstream Data;

public:
    template <typename T>
    LogMessage& operator<<(const T& t)
    {
        Data << t;
        return *this;
    }

    std::string extract()
    {
        return std::move(Data).str();
    }
};

////////////////////////////////////////////////////////////////////////////////

#define LOG_S(level, tag, message)                                             \
    LOG(level, tag, (NLogging::LogMessage() << message).extract());            \
// LOG_S

#define LOG_DEBUG(message) LOG(EVerbosity::DEBUG, "DEBUG", message);
#define LOG_DEBUG_S(message) LOG_S(EVerbosity::DEBUG, "DEBUG", message);
#define LOG_WARN(message) LOG(EVerbosity::WARN, "WARN", message);
#define LOG_WARN_S(message) LOG_S(EVerbosity::WARN, "WARN", message);
#define LOG_INFO(message) LOG(EVerbosity::INFO, "INFO", message);
#define LOG_INFO_S(message) LOG_S(EVerbosity::INFO, "INFO", message);
#define LOG_ERROR(message) LOG(EVerbosity::ERROR, "ERROR", message);
#define LOG_ERROR_S(message) LOG_S(EVerbosity::ERROR, "ERROR", message);
#define LOG_PERROR(message) LOG_ERROR_S(message << ": " << std::strerror(errno));

#define VERIFY(condition, message)                                             \
    if (!(condition)) {                                                        \
        LOG_ERROR(message);                                                    \
                                                                               \
        abort();                                                               \
    }                                                                          \
// VERIFY

}   // namespace NLogging
