#include "table.h"
#include <stdexcept>
#include <iostream>

namespace storage {
    template<typename KeyType>
    void Table::CreateIndex(const std::string &name, size_t column_index, int degree) {
        if (column_index >= schema_.GetColumnCount()) {
            throw std::out_of_range("Columns index out of range");
        }
        if (indexes_.find(name) != indexes_.end()) {
            throw std::invalid_argument("Index with the given name already exists");
        }
        auto index = std::make_shared<BPlusIndex<KeyType>>(degree);
        for (RID rid = 0; rid < tuples_.size(); ++rid) {
            if (tuples_[rid]) {
                const Field& field = tuples_[rid]->GetField(column_index);
                if (std::holds_alternative<KeyType>(field)) {
                    KeyType key = std::get<KeyType>(field);
                    index->Insert(key, rid);
                }
            }
        }
        indexes_[name] = index;
    }

    template<typename KeyType>
    std::shared_ptr<BPlusIndex<KeyType>> Table::GetIndex(const std::string &name) const {
        auto it = indexes_.find(name);
        if (it == indexes_.end()) throw std::invalid_argument("Index not found");
        return std::dynamic_pointer_cast<BPlusIndex<KeyType>>(it->second);
    }

    RID Table::InsertTuple(const std::vector<Field> &fields) {
        RID rid = next_rid_++;
        if (rid >= tuples_.size()) {
            tuples_.resize(rid + 1);
        }
        tuples_[rid] = std::make_shared<Tuple>(schema_, fields);
        for (const auto& [name, index_ptr] : indexes_) {
            size_t column_index = 0;
            for (; column_index < schema_.GetColumnCount(); ++column_index) {
                if (schema_.GetColumn(column_index).name == name) break;
            }
            if (column_index >= schema_.GetColumnCount()) continue;
            const Field& field = tuples_[rid]->GetField(column_index);
            if (auto int_ptr = std::get_if<int>(&field)) {
                auto index = std::dynamic_pointer_cast<BPlusIndex<int>>(index_ptr);
                if (index) {
                    index->Insert(*int_ptr, rid);
                }
            }
            if (auto double_ptr = std::get_if<double>(&field)) {
                auto index = std::dynamic_pointer_cast<BPlusIndex<double>>(index_ptr);
                if (index) index->Insert(*double_ptr, rid);
            }
            if (auto str_ptr = std::get_if<std::string>(&field)) {
                auto index = std::dynamic_pointer_cast<BPlusIndex<std::string>>(index_ptr);
                if (index) index->Insert(*str_ptr, rid);
            }
        }
        return rid;
    }

    std::shared_ptr<Tuple> Table::GetTuple(RID rid) const {
        if (rid >= tuples_.size()) throw std::out_of_range("Invalid RID");
        return tuples_[rid];
    }

    bool Table::RemoveTuple(RID rid) {
        if (rid >= tuples_.size() || !tuples_[rid]) {
            return false;
        }
        for (const auto& [name, index_ptr] : indexes_) {
            size_t column_index = 0;
            for (; column_index < schema_.GetColumnCount(); ++column_index) {
                if (schema_.GetColumn(column_index).name == name) break;
            }
            if (column_index >= schema_.GetColumnCount()) continue;

            const Field& field = tuples_[rid]->GetField(column_index);
            if (auto int_ptr = std::get_if<int>(&field)) {
                auto index = std::dynamic_pointer_cast<BPlusIndex<int>>(index_ptr);
                if (index) index->Remove(*int_ptr, rid);
            }
            if (auto double_ptr = std::get_if<double>(&field)) {
                auto index = std::dynamic_pointer_cast<BPlusIndex<double>>(index_ptr);
                if (index) index->Remove(*double_ptr, rid);
            }
            if (auto str_ptr = std::get_if<std::string>(&field)) {
                auto index = std::dynamic_pointer_cast<BPlusIndex<std::string>>(index_ptr);
                if (index) index->Remove(*str_ptr, rid);
            }
        }

        tuples_[rid].reset();
        return true;
    }

    bool Table::UpdateTuple(RID rid, const std::vector<Field> &fields) {
        if (rid >= tuples_.size() || !tuples_[rid]) return false;
        for (const auto& [name, index_ptr] : indexes_) {
            size_t column_index = 0;
            for (; column_index < schema_.GetColumnCount(); ++column_index) {
                if (schema_.GetColumn(column_index).name == name) {
                    break;
                }
            }
            if (column_index >= schema_.GetColumnCount()) continue;

            const Field& old_field = tuples_[rid]->GetField(column_index);
            const Field& new_field = fields[column_index];

            // int
            if (auto int_old_ptr = std::get_if<int>(&old_field)) {
                auto index = std::dynamic_pointer_cast<BPlusIndex<int>>(index_ptr);
                if (index) index->Remove(*int_old_ptr, rid);
            }
            if (auto int_new_ptr = std::get_if<int>(&new_field)) {
                auto index = std::dynamic_pointer_cast<BPlusIndex<int>>(index_ptr);
                if (index) index->Insert(*int_new_ptr, rid);
            }

            // double
            if (auto double_old_ptr = std::get_if<double>(&old_field)) {
                auto index = std::dynamic_pointer_cast<BPlusIndex<double>>(index_ptr);
                if (index) index->Remove(*double_old_ptr, rid);
            }
            if (auto double_new_ptr = std::get_if<double>(&new_field)) {
                auto index = std::dynamic_pointer_cast<BPlusIndex<double>>(index_ptr);
                if (index) index->Insert(*double_new_ptr, rid);
            }

            // str
            if (auto str_old_ptr = std::get_if<std::string>(&old_field)) {
                auto index = std::dynamic_pointer_cast<BPlusIndex<std::string>>(index_ptr);
                if (index) index->Remove(*str_old_ptr, rid);
            }
            if (auto str_new_ptr = std::get_if<std::string>(&new_field)) {
                auto index = std::dynamic_pointer_cast<BPlusIndex<std::string>>(index_ptr);
                if (index) index->Insert(*str_new_ptr, rid);
            }
        }
        tuples_[rid] = std::make_shared<Tuple>(schema_, fields);
        return true;
    }

    std::vector<RID> Table::GetAllRID() const {
        std::vector<RID> row_ids;
        for (RID row_id = 0; row_id < tuples_.size(); ++row_id) {
            if (tuples_[row_id]) {
                row_ids.push_back(row_id);
            }
        }
        return row_ids;
    }

    size_t Table::GetRowCount() const {
        size_t count = 0;
        for (const auto& tuple : tuples_) {
            ++count;
        }
        return count;
    }

    template void Table::CreateIndex<int>(const std::string &name, size_t column_index, int degree);
    template void Table::CreateIndex<double>(const std::string &name, size_t column_index, int degree);
    template void Table::CreateIndex<std::string>(const std::string &name, size_t column_index, int degree);

    template std::shared_ptr<BPlusIndex<int>> Table::GetIndex<int>(const std::string &name) const;
    template std::shared_ptr<BPlusIndex<double>> Table::GetIndex<double>(const std::string &name) const;
    template std::shared_ptr<BPlusIndex<std::string>> Table::GetIndex<std::string>(const std::string &name) const;
}
