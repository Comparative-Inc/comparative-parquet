#include "parquet_writer.h"

#include <arrow/api.h>
#include <arrow/builder.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include <vector>

using ArrowFieldPtr  = std::shared_ptr<arrow::Field>;
using ArrowArrayPtr  = std::shared_ptr<arrow::Array>;
using ArrowSchemaPtr = std::shared_ptr<arrow::Schema>;

static ArrowFieldPtr MakeField(std::string const& name, std::shared_ptr<arrow::DataType> const& type) {
  return std::make_shared<arrow::Field>(name, type);
}

static ArrowFieldPtr MakeField(std::string const& name, arrow::Type::type type, ParquetColumnDescriber const& describer) {
  switch (type) {
  case arrow::Type::type::BOOL:
    return MakeField(name, arrow::boolean());

  case arrow::Type::type::UINT8:
    return MakeField(name, arrow::uint8());

  case arrow::Type::type::INT8:
    return MakeField(name, arrow::int8());

  case arrow::Type::type::UINT16:
    return MakeField(name, arrow::uint16());

  case arrow::Type::type::INT16:
    return MakeField(name, arrow::int16());

  case arrow::Type::type::UINT32:
    return MakeField(name, arrow::uint32());

  case arrow::Type::type::INT32:
    return MakeField(name, arrow::int32());

  case arrow::Type::type::UINT64:
    return MakeField(name, arrow::uint64());

  case arrow::Type::type::INT64:
    return MakeField(name, arrow::int64());

  case arrow::Type::type::FLOAT:
    return MakeField(name, arrow::float32());

  case arrow::Type::type::DOUBLE:
    return MakeField(name, arrow::float64());

  case arrow::Type::type::STRING:
    return MakeField(name, arrow::utf8());

  case arrow::Type::type::BINARY:
    return MakeField(name, arrow::binary());

  case arrow::Type::type::FIXED_SIZE_BINARY: {
    return MakeField(name, arrow::fixed_size_binary(describer.width()));
  }

  case arrow::Type::type::DATE32:
    return MakeField(name, arrow::date32());

  case arrow::Type::type::TIMESTAMP: {
    auto unit = static_cast<arrow::TimeUnit::type>(describer.timeUnit());
    return MakeField(name, arrow::timestamp(unit));
  }

  case arrow::Type::type::TIME32: {
    auto unit = static_cast<arrow::TimeUnit::type>(describer.timeUnit());
    return MakeField(name, arrow::time32(unit));
  }

  case arrow::Type::type::TIME64: {
    auto unit = static_cast<arrow::TimeUnit::type>(describer.timeUnit());
    return MakeField(name, arrow::time64(unit));
  }

  default:
    // Should only happen if JS users use a number instead of the enum
    throw std::runtime_error("Invalid type id");
  }
}

struct Column {
  std::string key;
  arrow::Type::type type;
  std::unique_ptr<arrow::ArrayBuilder> builder;
};

template <typename T>
inline static void AppendScalar(Column& column, const T& value) {
  auto result = arrow::MakeScalar(column.builder->type(), value);
  if (result.ok()) {
    auto const status = column.builder->AppendScalar(*result.ValueOrDie());
    if (!status.ok()) {
        throw std::runtime_error(std::string{"Unable to append scalar: "} + status.ToString());
    }
  } else {
    throw std::runtime_error("Unable to make scalar from input");
  }
}

template <>
void AppendScalar<std::string>(Column& column, const std::string& value) {
    auto status = column.builder->AppendScalar(arrow::StringScalar(value));
    if (!status.ok()) {
        throw std::runtime_error(std::string{"Unable to make append string scalar: "} + status.ToString());
    }
}

template <>
void AppendScalar<std::shared_ptr<arrow::Buffer>>(Column& column, const std::shared_ptr<arrow::Buffer>& value) {
    auto type = column.builder->type();
    arrow::Status status;
    if (type->id() == arrow::Type::type::BINARY) {
        status = column.builder->AppendScalar(arrow::BinaryScalar(value));
    } else { // FIXED_SIZE_BINARY
        if (value->size() != dynamic_cast<arrow::FixedSizeBinaryType&>(*type).byte_width()) {
            throw std::runtime_error("FixedSizeBinary buffer is the wrong size");
        }
        status = column.builder->AppendScalar(arrow::FixedSizeBinaryScalar(value, type));
    }
    if (!status.ok()) {
        throw std::runtime_error(std::string{"Unable to append binary scalar: "} + status.ToString());
    }
}

class ParquetWriter::Impl
{
    std::string _filepath;
    arrow::FieldVector _fields;
    ArrowSchemaPtr _schema;
    std::vector<Column> _columns;
    std::shared_ptr<arrow::io::FileOutputStream> _outfile;
    parquet::WriterProperties::Builder _propBuilder;

public:
    Impl() = default;

    void setFilepath(std::string filepath) noexcept {
        _filepath = std::move(filepath);
    }
    std::string const& getFilepath() noexcept {
        return _filepath;
    }

    void addColumn(ParquetColumnDescriber const& describer) {
        auto const& name = describer.name();
        auto type = static_cast<arrow::Type::type>(describer.arrowType());
        _fields.push_back(MakeField(name, type, describer));

        std::unique_ptr<arrow::ArrayBuilder> builder;
        try {
            PARQUET_THROW_NOT_OK(arrow::MakeBuilder(arrow::default_memory_pool(), _fields.back()->type(), &builder));
        } catch (parquet::ParquetException const& e) {
            throw std::runtime_error(e.what());
        }
        _columns.push_back(Column{std::move(name), type, std::move(builder)});
    }

    void buildSchema() {
        _schema = arrow::schema(_fields);
    }

    std::optional<std::string> open() {
        try {
            PARQUET_ASSIGN_OR_THROW(
                _outfile,
                arrow::io::FileOutputStream::Open(_filepath));
        } catch (parquet::ParquetException const& e) {
            return std::string{e.what()};
        }
        return std::nullopt;
    }
    std::optional<std::string> close() {
        try {
            arrow::ArrayVector arrays;
            for (auto& column : _columns) {
                ArrowArrayPtr out;
                PARQUET_THROW_NOT_OK(column.builder->Finish(&out));
                arrays.push_back(out);
            }
            auto table = arrow::Table::Make(_schema, arrays);
            PARQUET_THROW_NOT_OK(
                parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), _outfile, 3, _propBuilder.build()));

            PARQUET_THROW_NOT_OK(_outfile->Close());
        } catch (parquet::ParquetException const& e) {
            return std::string{e.what()};
        }
        return std::nullopt;
    }

    void appendRow(ParquetInputRow const& inputRow) {
        size_t columnIndex = 0;
        for (auto& column : _columns) {
            appendRowValue(inputRow, column, columnIndex);
            ++columnIndex;
        }
    }

    void setRowGroupSize(std::int64_t rowGroupSize) {
        _propBuilder.max_row_group_length(rowGroupSize);
    }

private:
    void appendRowValue(ParquetInputRow const& inputRow, Column& column, size_t colIdx) {
        switch (column.type) {
        case arrow::Type::type::BOOL:
            AppendScalar(column, inputRow.getBool(colIdx));
            break;
        case arrow::Type::type::UINT8:
            AppendScalar(column, inputRow.getU8(colIdx));
            break;
        case arrow::Type::type::INT8:
            AppendScalar(column, inputRow.getI8(colIdx));
            break;
        case arrow::Type::type::UINT16:
            AppendScalar(column, inputRow.getU16(colIdx));
            break;
        case arrow::Type::type::INT16:
            AppendScalar(column, inputRow.getI16(colIdx));
            break;
        case arrow::Type::type::UINT32:
            AppendScalar(column, inputRow.getU32(colIdx));
            break;
        case arrow::Type::type::INT32:
        case arrow::Type::type::DATE32:
        case arrow::Type::type::TIME32:
            AppendScalar(column, inputRow.getI32(colIdx));
            break;
        case arrow::Type::type::UINT64:
            AppendScalar(column, inputRow.getU64(colIdx));
            break;
        case arrow::Type::type::INT64:
        case arrow::Type::type::TIMESTAMP:
        case arrow::Type::type::TIME64:
            AppendScalar(column, inputRow.getI64(colIdx));
            break;
        case arrow::Type::type::FLOAT:
            AppendScalar(column, inputRow.getF32(colIdx));
            break;
        case arrow::Type::type::DOUBLE:
            AppendScalar(column, inputRow.getF64(colIdx));
            break;
        case arrow::Type::type::STRING:
            AppendScalar(column, inputRow.getString(colIdx));
            break;
        case arrow::Type::type::BINARY:
        case arrow::Type::type::FIXED_SIZE_BINARY: {
            try {
                arrow::BufferBuilder arrowBuf;
                auto const inputBinary = inputRow.getBinary(colIdx);
                PARQUET_THROW_NOT_OK(arrowBuf.Append(inputBinary->data(), inputBinary->size()));
                std::shared_ptr<arrow::Buffer> finalBuf;
                PARQUET_THROW_NOT_OK(arrowBuf.Finish(&finalBuf));
                AppendScalar(column, finalBuf);
            } catch (parquet::ParquetException const& e) {
                throw std::runtime_error(e.what());
            }
            break;
        }

        default:
            // Should only happen if a JS user isn't using the enum
            throw std::runtime_error("Data type not supported");
        }
    }
};

ParquetWriter::ParquetWriter()
    : _impl(new Impl)
{}

void ParquetWriter::setFilepath(std::string filepath) noexcept {
    _impl->setFilepath(std::move(filepath));
}
std::string const& ParquetWriter::getFilepath() noexcept {
    return _impl->getFilepath();
}
void ParquetWriter::addColumn(ParquetColumnDescriber const& describer) {
    _impl->addColumn(describer);
}
void ParquetWriter::buildSchema() {
    _impl->buildSchema();
}
std::optional<std::string> ParquetWriter::open() {
    return _impl->open();
}
std::optional<std::string> ParquetWriter::close() {
    return _impl->close();
}
void ParquetWriter::appendRow(ParquetInputRow const& row) {
    _impl->appendRow(row);
}
void ParquetWriter::setRowGroupSize(std::int64_t rowGroupSize) {
    _impl->setRowGroupSize(rowGroupSize);
}

void ParquetWriter::Deleter::operator()(ParquetWriter::Impl* toDelete) const noexcept {
    delete toDelete;
}
