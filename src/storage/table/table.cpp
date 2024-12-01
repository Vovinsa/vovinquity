#include "table.h"
#include "schema.h"
#include <stdexcept>
#include <iostream>

namespace storage {
    template<typename KeyType>
    void Table::CreateIndex(const std::string &name, size_t column_index, int degree) {
        if (indexes_.find(name) != indexes_.end()) throw std::invalid_argument("Index with the given name already exists");
        if (column_index >= schema_.GetColumnCount()) throw std::out_of_range("Column index out of range");

        DataType data_type = schema_.GetColumn(column_index).type;
        if ((data_type == DataType::INTEGER && !std::is_same<KeyType, int>::value) ||
            (data_type == DataType::DOUBLE && !std::is_same<KeyType, double>::value) ||
            (data_type == DataType::VARCHAR && !std::is_same<KeyType, std::string>::value)) {
            throw std::invalid_argument("KeyType does not match column data type");
        }

        auto index = std::make_shared<BPlusIndex<KeyType>>(degree);

        for (const auto& [rid, tuple] : tuples_) {
            const Field& field = tuple->GetField(column_index);
            KeyType key = std::get<KeyType>(field);
            index->Insert(key, rid);
        }
        IndexVariant index_variant{std::in_place_type<std::shared_ptr<BPlusIndex<KeyType>>>, index};
        IndexInfo index_info{column_index, data_type, index_variant};
        indexes_[name] = index_info;
    }

    template<typename KeyType>
    std::shared_ptr<BPlusIndex<KeyType>> Table::GetIndex(const std::string &name) const {
        auto it = indexes_.find(name);
        if (it == indexes_.end()) throw std::invalid_argument("Index not found");

        DataType data_type = it->second.data_type;
        if ((data_type == DataType::INTEGER && !std::is_same<KeyType, int>::value) ||
            (data_type == DataType::DOUBLE && !std::is_same<KeyType, double>::value) ||
            (data_type == DataType::VARCHAR && !std::is_same<KeyType, std::string>::value)) {
            throw std::invalid_argument("KeyType does not match index data type");
        }

        return std::get<std::shared_ptr<BPlusIndex<KeyType>>>(it->second.index);
    }

    RID Table::InsertTuple(const std::vector<Field> &fields) {
        RID rid = next_rid_++;
        tuples_[rid] = std::make_shared<Tuple>(schema_, fields);

        for (const auto& [name, index_info] : indexes_) {
            size_t column_index = index_info.column_index;
            const Field& field = fields[column_index];
            DataType data_type = index_info.data_type;

            if (data_type == DataType::INTEGER) {
                auto index = std::get<std::shared_ptr<BPlusIndex<int>>>(index_info.index);
                int key = std::get<int>(field);
                index->Insert(key, rid);
            } else if (data_type == DataType::DOUBLE) {
                auto index = std::get<std::shared_ptr<BPlusIndex<double>>>(index_info.index);
                double key = std::get<double>(field);
                index->Insert(key, rid);
            } else if (data_type == DataType::VARCHAR) {
                auto index = std::get<std::shared_ptr<BPlusIndex<std::string>>>(index_info.index);
                std::string key = std::get<std::string>(field);
                index->Insert(key, rid);
            }
        }
        return rid;
    }

    std::shared_ptr<Tuple> Table::GetTuple(RID rid) const {
        auto it = tuples_.find(rid);
        if (it == tuples_.end()) throw std::out_of_range("Invalid RID");
        return it->second;
    }

    bool Table::RemoveTuple(RID rid) {
        auto it = tuples_.find(rid);
        if (it == tuples_.end()) return false;

        for (const auto& [name, index_info] : indexes_) {
            size_t column_index = index_info.column_index;
            const Field& field = it->second->GetField(column_index);
            DataType data_type = index_info.data_type;

            if (data_type == DataType::INTEGER) {
                auto index = std::get<std::shared_ptr<BPlusIndex<int>>>(index_info.index);
                int key = std::get<int>(field);
                index->Remove(key, rid);
            } else if (data_type == DataType::DOUBLE) {
                auto index = std::get<std::shared_ptr<BPlusIndex<double>>>(index_info.index);
                double key = std::get<double>(field);
                index->Remove(key, rid);
            } else if (data_type == DataType::VARCHAR) {
                auto index = std::get<std::shared_ptr<BPlusIndex<std::string>>>(index_info.index);
                std::string key = std::get<std::string>(field);
                index->Remove(key, rid);
            }
        }
        tuples_.erase(it);
        return true;
    }

    bool Table::UpdateTuple(RID rid, const std::vector<Field> &fields) {
        auto it = tuples_.find(rid);
        if (it == tuples_.end()) return false;

        for (const auto& [name, index_info] : indexes_) {
            size_t column_index = index_info.column_index;
            const Field& old_field = it->second->GetField(column_index);
            const Field& new_field = fields[column_index];
            DataType data_type = index_info.data_type;

            if (data_type == DataType::INTEGER) {
                auto index = std::get<std::shared_ptr<BPlusIndex<int>>>(index_info.index);
                int old_key = std::get<int>(old_field);
                int new_key = std::get<int>(new_field);
                index->Remove(old_key, rid);
                index->Insert(new_key, rid);
            } else if (data_type == DataType::DOUBLE) {
                auto index = std::get<std::shared_ptr<BPlusIndex<double>>>(index_info.index);
                double old_key = std::get<double>(old_field);
                double new_key = std::get<double>(new_field);
                index->Remove(old_key, rid);
                index->Insert(new_key, rid);
            } else if (data_type == DataType::VARCHAR) {
                auto index = std::get<std::shared_ptr<BPlusIndex<std::string>>>(index_info.index);
                std::string old_key = std::get<std::string>(old_field);
                std::string new_key = std::get<std::string>(new_field);
                index->Remove(old_key, rid);
                index->Insert(new_key, rid);
            }
        }
        it->second = std::make_shared<Tuple>(schema_, fields);
        return true;
    }

    std::vector<RID> Table::GetAllRID() const {
        std::vector<RID> rids;
        for (const auto& [rid, tuple] : tuples_) rids.push_back(rid);
        return rids;
    }

    size_t Table::GetRowCount() const {
        return tuples_.size();
    }

    void Table::SaveToFile(const std::string& file_name) const {
        std::ofstream out(file_name);
        if (!out) throw std::ios_base::failure("Failed to open file for writing");

        out << "SCHEMA\n";
        for (const auto& column : schema_.GetColumns()) out << column.name << "," << static_cast<int>(column.type) << "\n";

        out << "DATA\n";
        for (const auto& [rid, tuple] : tuples_) {
            out << rid << ",";
            for (size_t i = 0; i < schema_.GetColumnCount(); ++i) {
                const Field& field = tuple->GetField(i);
                if (std::holds_alternative<int>(field)) out << std::get<int>(field);
                else if (std::holds_alternative<double>(field)) out << std::get<double>(field);
                else if (std::holds_alternative<std::string>(field)) {
                    std::string value = std::get<std::string>(field);
                    if (value.find('"') != std::string::npos) {
                        size_t pos = 0;
                        while ((pos = value.find('"', pos)) != std::string::npos) {
                            value.insert(pos, 1, '"');
                            pos += 2;
                        }
                    }
                    if (value.find(',') != std::string::npos || value.find('"') != std::string::npos) out << "\"" << value << "\"";
                    else out << value;
                }
                if (i < schema_.GetColumnCount() - 1) out << ",";
            }
            out << "\n";
        }

        out << "INDEXES\n";
        for (const auto& [name, index_info] : indexes_) {
            out << name << ",";
            out << index_info.column_index << ",";
            out << static_cast<int>(index_info.data_type) << "\n";
        }

        out.close();
    }

    void Table::LoadFromFile(const std::string& file_name) {
        std::ifstream in(file_name);
        if (!in) throw std::ios_base::failure("Failed to open file for reading");

        std::string line;
        bool reading_schema = false;
        bool reading_data = false;
        bool reading_indexes = false;

        tuples_.clear();
        indexes_.clear();
        next_rid_ = 0;

        Schema file_schema;

        while (std::getline(in, line)) {
            if (line == "SCHEMA") {
                reading_schema = true;
                reading_data = false;
                reading_indexes = false;
                continue;
            } else if (line == "DATA") {
                reading_schema = false;
                reading_data = true;
                reading_indexes = false;
                continue;
            } else if (line == "INDEXES") {
                reading_schema = false;
                reading_data = false;
                reading_indexes = true;
                continue;
            }

            if (reading_schema) {
                std::istringstream ss(line);
                std::string name;
                std::string type_str;
                if (std::getline(ss, name, ',') && std::getline(ss, type_str)) {
                    DataType type = static_cast<DataType>(std::stoi(type_str));
                    file_schema.InsertColumn(name, type);
                }
            } else if (reading_data) {
                std::istringstream ss(line);
                std::string rid_str;
                if (!std::getline(ss, rid_str, ',')) continue;
                RID rid = std::stoull(rid_str);

                std::vector<Field> fields;
                size_t column_index = 0;
                std::string token;
                while (column_index < file_schema.GetColumnCount()) {
                    if (!std::getline(ss, token, ',')) throw std::runtime_error("Insufficient data fields in row");

                    if (!token.empty() && token.front() == '"') {
                        while (!token.empty() && token.back() != '"') {
                            std::string next_token;
                            if (!std::getline(ss, next_token, ',')) throw std::runtime_error("Unmatched quotes in string field");
                            token += "," + next_token;
                        }
                        token = token.substr(1, token.size() - 2);
                        size_t pos = 0;
                        while ((pos = token.find("\"\"", pos)) != std::string::npos) {
                            token.replace(pos, 2, "\"");
                            ++pos;
                        }
                    }

                    const auto& column = file_schema.GetColumn(column_index);
                    if (column.type == DataType::INTEGER) fields.emplace_back(std::stoi(token));
                    else if (column.type == DataType::DOUBLE) fields.emplace_back(std::stod(token));
                    else if (column.type == DataType::VARCHAR) fields.emplace_back(token);
                    ++column_index;
                }
                if (fields.size() != schema_.GetColumnCount()) throw std::runtime_error("Mismatch in number of fields while loading data");
                if (rid >= next_rid_) next_rid_ = rid + 1;
                tuples_[rid] = std::make_shared<Tuple>(schema_, fields);
            } else if (reading_indexes) {
                std::istringstream ss(line);
                std::string name;
                std::string column_index_str;
                std::string data_type_str;
                if (std::getline(ss, name, ',') && std::getline(ss, column_index_str, ',') && std::getline(ss, data_type_str)) {
                    size_t column_index = std::stoul(column_index_str);
                    DataType data_type = static_cast<DataType>(std::stoi(data_type_str));

                    if (data_type == DataType::INTEGER) CreateIndex<int>(name, column_index, 3);
                    else if (data_type == DataType::DOUBLE) CreateIndex<double>(name, column_index, 3);
                    else if (data_type == DataType::VARCHAR) CreateIndex<std::string>(name, column_index, 3);
                }
            }
        }
        in.close();

        if (file_schema.GetColumnCount() != schema_.GetColumnCount()) throw std::runtime_error("Schema mismatch: different number of columns");
        for (size_t i = 0; i < schema_.GetColumnCount(); ++i) {
            const auto& table_column = schema_.GetColumn(i);
            const auto& file_column = file_schema.GetColumn(i);
            if (table_column.name != file_column.name || table_column.type != file_column.type) {
                throw std::runtime_error("Schema mismatch: column definitions do not match");
            }
        }
    }


    template void Table::CreateIndex<int>(const std::string &name, size_t column_index, int degree);
    template void Table::CreateIndex<double>(const std::string &name, size_t column_index, int degree);
    template void Table::CreateIndex<std::string>(const std::string &name, size_t column_index, int degree);

    template std::shared_ptr<BPlusIndex<int>> Table::GetIndex<int>(const std::string &name) const;
    template std::shared_ptr<BPlusIndex<double>> Table::GetIndex<double>(const std::string &name) const;
    template std::shared_ptr<BPlusIndex<std::string>> Table::GetIndex<std::string>(const std::string &name) const;
}
