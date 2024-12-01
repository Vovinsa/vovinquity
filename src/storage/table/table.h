#pragma once

#include "schema.h"
#include "tuple.h"
#include "bplus_index.h"
#include <utility>
#include <vector>
#include <unordered_map>
#include <fstream>
#include <sstream>
#include <algorithm>

namespace storage {
    using IndexVariant = std::variant<
            std::shared_ptr<BPlusIndex<int>>,
            std::shared_ptr<BPlusIndex<double>>,
            std::shared_ptr<BPlusIndex<std::string>>
    >;

    struct IndexInfo {
        size_t column_index;
        DataType data_type;
        IndexVariant index;
    };

    class Table {
    public:
        explicit Table(const Schema&  schema) : schema_(schema), next_rid_(0) {};
        ~Table() = default;

        virtual RID InsertTuple(const std::vector<Field>& fields);
        [[nodiscard]] std::shared_ptr<Tuple> GetTuple(RID rid) const;
        virtual bool RemoveTuple(RID rid);
        virtual bool UpdateTuple(RID rid, const std::vector<Field>& fields);
        [[nodiscard]] std::vector<RID> GetAllRID() const;

        template<typename KeyType>
        void CreateIndex(const std::string& name, size_t column_index, int degree);

        template<typename KeyType>
        std::shared_ptr<BPlusIndex<KeyType>> GetIndex(const std::string& name) const;

        [[nodiscard]] size_t GetRowCount() const;

        void SaveToFile(const std::string& file_name) const;
        void LoadFromFile(const std::string& file_name);

    protected:
        const Schema schema_;
        std::unordered_map<RID, std::shared_ptr<Tuple>> tuples_;
        RID next_rid_;
        std::unordered_map<std::string, IndexInfo> indexes_;
    };
}
