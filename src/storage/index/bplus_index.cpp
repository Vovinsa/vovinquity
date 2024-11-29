#include "bplus_index.h"

namespace storage {
    template<typename KeyType>
    BPlusIndex<KeyType>::BPlusIndex(int degree) {
        bplus_tree_ = std::make_unique<BPlusTree<KeyType>>(degree);
    }

    template<typename KeyType>
    void BPlusIndex<KeyType>::Insert(const KeyType &key, RID rid) {
        bplus_tree_->Insert(key);
        key_rid_.emplace(key, rid);
    }

    template<typename KeyType>
    void BPlusIndex<KeyType>::Remove(const KeyType &key, RID rid) {
        auto range = key_rid_.equal_range(key);
        for (auto it = range.first; it != range.second; ++it) {
            if (it->second == rid) {
                key_rid_.erase(it);
                break;
            }
        }
        if (key_rid_.count(key) == 0) bplus_tree_->Remove(key);
    }

    template<typename KeyType>
    std::vector<RID> BPlusIndex<KeyType>::Search(const KeyType &key) const {
        std::vector<RID> rids;
        auto range = key_rid_.equal_range(key);
        for (auto it = range.first; it != range.second; ++it) rids.push_back(it->second);
        return rids;
    }

    template<typename KeyType>
    std::vector<RID> BPlusIndex<KeyType>::RangeQuery(const KeyType &lower, const KeyType &upper) const {
        std::vector<KeyType> keys_in_range = bplus_tree_->RangeQuery(lower, upper);
        std::vector<RID> rids;
        for (const auto& key : keys_in_range) {
            auto range = key_rid_.equal_range(key);
            for (auto it = range.first; it != range.second; ++it) rids.push_back(it->second);
        }
        return rids;
    }

    template class BPlusIndex<int>;
    template class BPlusIndex<std::string>;
    template class BPlusIndex<double>;
}
