#ifndef CP_PARQUET_READER_H
#define CP_PARQUET_READER_H

#include <optional>
#include <string>
#include <memory>
#include <cstdint>

class ParquetValueReceiver
{
public:
    virtual ~ParquetValueReceiver() noexcept {}
    virtual void receiveBool(bool b) = 0;
    virtual void receiveInt32(std::int32_t i) = 0;
    virtual void receiveInt64(std::int64_t i) = 0;
    virtual void receiveDouble(double d) = 0;
    virtual void receiveString(char const * data, size_t len) = 0;
    virtual void receiveNull() = 0;

protected:
    constexpr ParquetValueReceiver() noexcept {}
};

class ParquetReader
{
    class Impl;
    struct Deleter
    {
        void operator()(Impl*) const noexcept;
    };
    using UniqueImpl = std::unique_ptr<Impl,Deleter>;

    UniqueImpl _impl;
public:
    ParquetReader();

    void setFilepath(std::string filepath) noexcept;
    std::string const& getFilepath() noexcept;
    bool isOpen() const noexcept;
    std::optional<std::string> open();
    std::optional<std::string> close();
    size_t getColumnCount() const noexcept;
    std::string const& getColumnName(size_t idx) const;
    size_t getRowCount() const noexcept;

    void getValue(ParquetValueReceiver& receiver, size_t col, size_t row) const;
};

#endif
