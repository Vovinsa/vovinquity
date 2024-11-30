#pragma once

#include <utility>
#include <variant>
#include <vector>
#include <utility>
#include "schema.h"
#include <iostream>

namespace storage {
    using RID = uint64_t;
    using Field = std::variant<int, double, std::string>;

    class Tuple {
    public:
        Tuple(const Schema& schema, std::vector<Field>  fields) : schema_(schema), fields_(std::move(fields)) {
            if (fields_.size() != schema_.GetColumnCount()) {
                throw std::invalid_argument("Number of fields doesn't match schema");
            }
            for (size_t i = 0; i < fields_.size(); ++i) {
                const Column& column = schema_.GetColumn(i);
                const Field& field = fields_[i];

                switch (column.type) {
                    case INTEGER:
                        if (!std::holds_alternative<int>(field)) {
                            throw std::invalid_argument("Field type mismatch: expected INTEGER");
                        }
                        break;
                    case DOUBLE:
                        if (!std::holds_alternative<double>(field)) {
                            throw std::invalid_argument("Field type mismatch: expected DOUBLE");
                        }
                        break;
                    case VARCHAR:
                        if (!std::holds_alternative<std::string>(field)) {
                            throw std::invalid_argument("Field type mismatch: expected VARCHAR");
                        }
                        break;
                    default:
                        throw std::invalid_argument("Unknown column type in schema");
                }
            }
        }

        const Field& GetField(size_t index) {
            if (index >= fields_.size()) {
                throw std::out_of_range("Field index out of range");
            }
            return fields_.at(index);
        }
    public:
        const Schema& schema_;
        std::vector<Field> fields_;
    };
}
