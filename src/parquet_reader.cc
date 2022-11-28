#include "parquet_reader.h"

#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

using ArrowSchemaPtr = std::shared_ptr<arrow::Schema>;
using ArrowColumnPtr = std::shared_ptr<arrow::ChunkedArray>;
using ArrowArrayPtr  = std::shared_ptr<arrow::Array>;
using ArrowFieldPtr  = std::shared_ptr<arrow::Field>;

class ParquetReader::Impl
{
    std::string _filepath;
    arrow::MemoryPool* _pool;
    std::shared_ptr<arrow::io::RandomAccessFile> _input;
    std::unique_ptr<parquet::arrow::FileReader> _reader;
    std::vector<ArrowColumnPtr> _columns;
    std::vector<std::vector<ArrowArrayPtr>> _chunksByColumn;
    std::vector<ArrowFieldPtr> _fieldByColumn;
    int64_t _columnCount;
    int64_t _rowCount;
    bool _isOpen;

public:
    Impl()
        : _pool(arrow::default_memory_pool())
        , _columns()
        , _columnCount(0)
        , _rowCount(0)
        , _isOpen(false)
    {}

    void setFilepath(std::string filepath) noexcept {
        _filepath = std::move(filepath);
    }
    std::string const& getFilepath() noexcept {
        return _filepath;
    }
    bool isOpen() const noexcept { return _isOpen; }

    std::optional<std::string> open() {
        _input = arrow::io::MemoryMappedFile::Open(
            _filepath.c_str(), arrow::io::FileMode::READ).ValueOrDie();

        /* Open file */
        auto status = parquet::arrow::OpenFile(_input, _pool, &_reader);
        if (!status.ok()) {
            return std::string("Failed to open file: ") + status.ToString();
        }

        /* Read schema & columns */
        ArrowSchemaPtr schema;
        status = _reader->GetSchema(&schema);
        if (!status.ok()) {
            return std::string("Failed to read schema: " + status.ToString());
        }

        status = _reader->ScanContents({}, 256, &_rowCount);
        if (!status.ok()) {
            return std::string("Failed to read row count: ") + status.ToString();
        }

        _columnCount = schema->num_fields();

        for (auto i = 0; i < _columnCount; i++) {
            ArrowColumnPtr column;
            status = _reader->ReadColumn(i, &column);
            if (!status.ok()) {
                return std::string("Failed to read column: ") + status.ToString();
            }
            _columns.push_back(column);
            _chunksByColumn.push_back(column->chunks());
            _fieldByColumn.push_back(schema->field(i));
        }

        _isOpen = true;
        return std::nullopt;
    }

    std::optional<std::string> close()
    {
        if (auto status = _input->Close(); !status.ok())
        {
            return std::string("Failed to close file: ") + status.ToString();
        }
        _isOpen = false;
        return std::nullopt;
    }

    size_t getColumnCount() const noexcept {
        return static_cast<size_t>(_columnCount);
    }
    std::string const& getColumnName(size_t idx) {
        auto const& field = _fieldByColumn[idx];
        return field->name();
    }

    size_t getRowCount() const noexcept {
        return static_cast<size_t>(_rowCount);
    }

    void getValue(ParquetValueReceiver& receiver, size_t columnIndex, size_t rowIndex) const {
        size_t absoluteIndex = 0;
        auto const chunks = _chunksByColumn[columnIndex];

        for (auto chunk : chunks) {
            auto isInChunk =
                rowIndex >= absoluteIndex
                && rowIndex < (absoluteIndex + chunk->length());

            if (!isInChunk) {
                absoluteIndex += chunk->length();
                continue;
            }

            auto index = rowIndex - absoluteIndex;
            auto array = chunk->data();

            switch (_fieldByColumn[columnIndex]->type()->id()) {
            case arrow::Type::BOOL: {
                auto view = array->GetValues<uint8_t>(1, 0);
                auto viewIndex = index / 8;
                auto offset = index % 8;
                auto bits = view[viewIndex];
                auto value = (bits >> offset) & 1;
                receiver.receiveBool(value ? true : false);
                break;
            }
            case arrow::Type::DATE32: {
                auto view = array->GetValues<int32_t>(1, 0);
                auto value = view[index];
                receiver.receiveInt32(value);
                break;
            }
            case arrow::Type::TIMESTAMP:
            case arrow::Type::INT64: {
                auto view = array->GetValues<int64_t>(1, 0);
                auto value = view[index];
                receiver.receiveInt64(value);
                break;
            }
            case arrow::Type::DOUBLE: {
                auto view = array->GetValues<double>(1, 0);
                auto value = view[index];
                receiver.receiveDouble(value);
                break;
            }
            case arrow::Type::STRING: {
                const int32_t* offsets = array->GetValues<int32_t>(1, 0);
                const char*    view    = array->GetValues<char>(2, 0);

                auto start = offsets[index];
                auto end   = offsets[index + 1];
                auto data  = &view[start];
                auto length = end - start;
                receiver.receiveString(data, length);
                break;
            }
            default: receiver.receiveNull();
            }
            return;
        }
        receiver.receiveNull();
    }
};

ParquetReader::ParquetReader()
    : _impl(new Impl)
{}

void ParquetReader::setFilepath(std::string filepath) noexcept {
    _impl->setFilepath(std::move(filepath));
}
std::string const& ParquetReader::getFilepath() noexcept {
    return _impl->getFilepath();
}
bool ParquetReader::isOpen() const noexcept {
    return _impl->isOpen();
}
std::optional<std::string> ParquetReader::open() {
    return _impl->open();
}
std::optional<std::string> ParquetReader::close() {
    return _impl->close();
}
size_t ParquetReader::getColumnCount() const noexcept {
    return _impl->getColumnCount();
}
std::string const& ParquetReader::getColumnName(size_t columnIndex) const {
    return _impl->getColumnName(columnIndex);
}
size_t ParquetReader::getRowCount() const noexcept {
    return _impl->getRowCount();
}
void ParquetReader::getValue(ParquetValueReceiver& receiver, size_t columnIndex, size_t rowIndex) const {
    return _impl->getValue(receiver, columnIndex, rowIndex);
}

void ParquetReader::Deleter::operator()(ParquetReader::Impl* toDelete) const noexcept
{
    delete toDelete;
}
