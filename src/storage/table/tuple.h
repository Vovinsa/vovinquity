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
