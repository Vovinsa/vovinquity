#pragma once

#include "tuple.h"
#include "bplus_tree.h"
#include <vector>
#include <utility>
#include <map>

namespace storage {
    class IndexBase {
    public:
        virtual ~IndexBase() = default;
    };

    template<typename KeyType>
    class BPlusIndex : public IndexBase {
    public:
        explicit BPlusIndex(int degree);
        ~BPlusIndex() override = default;

        void Insert(const KeyType& key, RID rid);
        void Remove(const KeyType& key, RID rid);
        [[nodiscard]] std::vector<RID> Search(const KeyType& key) const;
        [[nodiscard]] std::vector<RID> RangeQuery(const KeyType& lower, const KeyType& upper) const;
    private:
        std::unique_ptr<BPlusTree<KeyType>> bplus_tree_;
        std::multimap<KeyType, RID> key_rid_;
    };
}
