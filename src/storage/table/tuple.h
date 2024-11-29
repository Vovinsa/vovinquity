#pragma once

#include <variant>
#include <vector>
#include <utility>
#include "schema.h"

namespace storage {
    using RID = uint64_t;
    using Field = std::variant<int, double, std::string>;

    class Tuple {
    public:
        Tuple(const Schema& schema, const std::vector<Field>& fields) : schema_(schema), fields_(fields) {
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
    private:
        const Schema& schema_;
        const std::vector<Field>& fields_;
    };
}
