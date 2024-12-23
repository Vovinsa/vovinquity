#pragma once

#include <string>
#include <utility>
#include <vector>
#include <stdexcept>

namespace storage {
    enum DataType {
        INTEGER, DOUBLE, VARCHAR
    };

    struct Column {
        std::string name;
        DataType type;

        Column(std::string name_, DataType type_) : name(std::move(name_)), type(type_) {}
    };

    class Schema {
    public:
        Schema() = default;

        void InsertColumn(const std::string& name, DataType type) {
            columns_.emplace_back(name, type);
        };
        [[nodiscard]] const std::vector<Column>& GetColumns() const {
            return columns_;
        }

        [[nodiscard]] size_t GetColumnCount() const {
            return columns_.size();
        }

        [[nodiscard]] const Column& GetColumn(size_t index) const {
            return columns_.at(index);
        }

        [[nodiscard]] size_t GetColumnIndex(const std::string& name) const {
            for (size_t i = 0; i < columns_.size(); ++i) {
                if (columns_[i].name == name) {
                    return i;
                }
            }
            throw std::out_of_range("Column name not found: " + name);
        }

    private:
        std::vector<Column> columns_;
    };
}
