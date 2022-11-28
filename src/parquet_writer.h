#ifndef CP_PARQUET_WRITER_H
#define CP_PARQUET_WRITER_H

#include <memory>
#include <string>
#include <optional>
#include <cstdint>

class ParquetColumnDescriber
{
public:
    virtual ~ParquetColumnDescriber() noexcept {}
    virtual std::string const& name() const = 0;
    virtual int arrowType() const = 0;

    // conditionally applicable methods
    // ================================

    virtual std::int32_t width() const = 0;
    virtual int timeUnit() const = 0;

protected:
    constexpr ParquetColumnDescriber() noexcept {}
};

class ParquetInputBinary
{
public:
    virtual ~ParquetInputBinary() noexcept {}
    virtual std::uint8_t const* data() const = 0;
    virtual size_t size() const = 0;
protected:
    constexpr ParquetInputBinary() noexcept {}
};

class ParquetInputRow
{
public:
    virtual ~ParquetInputRow() noexcept {}
    virtual bool getBool(size_t columnIndex) const = 0;
    virtual std::int8_t getI8(size_t columnIndex) const = 0;
    virtual std::uint8_t getU8(size_t columnIndex) const = 0;
    virtual std::int16_t getI16(size_t columnIndex) const = 0;
    virtual std::uint16_t getU16(size_t columnIndex) const = 0;
    virtual std::int32_t getI32(size_t columnIndex) const = 0;
    virtual std::uint32_t getU32(size_t columnIndex) const = 0;
    virtual std::int64_t getI64(size_t columnIndex) const = 0;
    virtual std::uint64_t getU64(size_t columnIndex) const = 0;
    virtual float getF32(size_t columnIndex) const = 0;
    virtual double getF64(size_t columnIndex) const = 0;
    virtual std::string getString(size_t columnIndex) const = 0;
    virtual std::unique_ptr<ParquetInputBinary> getBinary(size_t columnIndex) const = 0;

protected:
    constexpr ParquetInputRow() noexcept {}
};

class ParquetWriter
{
    class Impl;
    struct Deleter
    {
        void operator()(Impl*) const noexcept;
    };
    using UniqueImpl = std::unique_ptr<Impl,Deleter>;

    UniqueImpl _impl;

public:
    ParquetWriter();

    void setFilepath(std::string filepath) noexcept;
    std::string const& getFilepath() noexcept;

    void addColumn(ParquetColumnDescriber const& describer);

    // Should only be called after all `addColumn()` calls
    // have been made
    void buildSchema();

    std::optional<std::string> open();
    std::optional<std::string> close();

    void appendRow(ParquetInputRow const& row);
    void setRowGroupSize(std::int64_t rowGroupSize);
};

#endif
