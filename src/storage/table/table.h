#pragma once

#include "schema.h"
#include "tuple.h"
#include "bplus_index.h"
#include <utility>
#include <vector>
#include <unordered_map>

namespace storage {
    class Table {
    public:
        explicit Table(Schema&  schema) : schema_(schema), next_rid_(0) {};
        ~Table() = default;

        RID InsertTuple(const std::vector<Field>& fields);
        [[nodiscard]] std::shared_ptr<Tuple> GetTuple(RID rid) const;
        bool RemoveTuple(RID rid);
        bool UpdateTuple(RID rid, const std::vector<Field>& fields);
        [[nodiscard]] std::vector<RID> GetAllRID() const;

        template<typename KeyType>
        void CreateIndex(const std::string& name, size_t column_index, int degree);

        template<typename KeyType>
        std::shared_ptr<BPlusIndex<KeyType>> GetIndex(const std::string& name) const;

        [[nodiscard]] size_t GetRowCount() const;
    private:
        const Schema schema_;
        std::vector<std::shared_ptr<Tuple>> tuples_;
        RID next_rid_;
        std::unordered_map<std::string, std::shared_ptr<IndexBase>> indexes_;
    };
}
