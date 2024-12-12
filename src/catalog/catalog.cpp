#include "catalog.h"
#include <stdexcept>

namespace catalog {
    Catalog::Catalog()
            : next_table_id_(0), next_column_id_(0), next_index_id_(0),
              tables_system_table_([]() {
                  storage::Schema schema;
                  schema.InsertColumn("table_id", storage::DataType::INTEGER);
                  schema.InsertColumn("table_name", storage::DataType::VARCHAR);
                  return schema;
              }()),
              columns_system_table_([]() {
                  storage::Schema schema;
                  schema.InsertColumn("column_id", storage::DataType::INTEGER);
                  schema.InsertColumn("table_id", storage::DataType::INTEGER);
                  schema.InsertColumn("column_name", storage::DataType::VARCHAR);
                  schema.InsertColumn("data_type", storage::DataType::INTEGER);
                  return schema;
              }()),
              indexes_system_table_([]() {
                  storage::Schema schema;
                  schema.InsertColumn("index_id", storage::DataType::INTEGER);
                  schema.InsertColumn("index_name", storage::DataType::VARCHAR);
                  schema.InsertColumn("table_id", storage::DataType::INTEGER);
                  return schema;
              }()),
              index_columns_system_table_([]() {
                  storage::Schema schema;
                  schema.InsertColumn("index_id", storage::DataType::INTEGER);
                  schema.InsertColumn("column_id", storage::DataType::INTEGER);
                  schema.InsertColumn("ordinal_position", storage::DataType::INTEGER);
                  return schema;
              }()) {
        LoadSystemTables();
    }

    void Catalog::CreateTable(const std::string& table_name, const storage::Schema& schema) {
        if (HasTable(table_name)) throw std::invalid_argument("Table already exists: " + table_name);

        auto table = std::make_shared<storage::Table>(schema);
        tables_[table_name] = table;

        int table_id = next_table_id_++;
        tables_system_table_.AddRecord({table_id, table_name});

        for (size_t i = 0; i < schema.GetColumnCount(); ++i) {
            const auto& column = schema.GetColumn(i);
            columns_system_table_.AddRecord({next_column_id_++, table_id, column.name, static_cast<int>(column.type)});
        }
    }

    std::shared_ptr<storage::Table> Catalog::GetTable(const std::string& table_name) const {
        auto it = tables_.find(table_name);
        if (it == tables_.end()) throw std::invalid_argument("Table not found: " + table_name);
        return it->second;
    }

    bool Catalog::HasTable(const std::string& table_name) const {
        return tables_.find(table_name) != tables_.end();
    }

    void catalog::Catalog::DropTable(const std::string& table_name) {
        if (!HasTable(table_name)) throw std::runtime_error("Table does not exist: " + table_name);

        auto table_records = tables_system_table_.FindRecords(
                [&](const storage::TableRecord& record) {
                    return record.table_name == table_name;
                }
        );

        if (table_records.empty()) throw std::runtime_error("Table not found in system tables: " + table_name);

        int table_id = table_records[0].table_id;

        indexes_system_table_.RemoveRecords(
                [&](const storage::IndexRecord& record) {
                    return record.table_id == table_id;
                }
        );
        index_columns_system_table_.RemoveRecords(
                [&](const storage::IndexColumnRecord& record) {
                    return record.index_id == table_id;
                }
        );
        tables_system_table_.RemoveRecords(
                [&](const storage::TableRecord& record) {
                    return record.table_id == table_id;
                }
        );
        tables_.erase(table_name);
    }


    template<typename KeyType>
    void Catalog::CreateIndex(const std::string& index_name, const std::string& table_name, size_t column_index, int degree) {
        if (!HasTable(table_name)) throw std::invalid_argument("Table not found: " + table_name);

        auto table = GetTable(table_name);
        table->CreateIndex<KeyType>(index_name, column_index, degree);
        int index_id = next_index_id_++;

        auto table_records = tables_system_table_.FindRecords([&](const storage::TableRecord& record) {
            return record.table_name == table_name;
        });

        if (table_records.empty()) throw std::invalid_argument("Table not found in system table: " + table_name);

        int table_id = table_records.front().table_id;
        indexes_system_table_.AddRecord({index_id, index_name, table_id});
        const auto& column = table->GetSchema().GetColumn(column_index);

        auto column_records = columns_system_table_.FindRecords([&](const storage::ColumnRecord& record) {
            return record.table_id == table_id && record.column_name == column.name;
        });

        if (column_records.empty()) throw std::invalid_argument("Column not found in system table: " + column.name);

        int column_id = column_records.front().column_id;
        index_columns_system_table_.AddRecord({index_id, column_id, 1});
    }

    std::vector<std::pair<storage::IndexRecord, std::vector<std::string>>> Catalog::GetIndexesForTable(const std::string &table_name) const {
        if (!HasTable(table_name)) throw std::runtime_error("Table does not exist: " + table_name);

        auto table_records = tables_system_table_.FindRecords(
                [&](const storage::TableRecord& record) {
                    return record.table_name == table_name;
                });

        if (table_records.empty()) throw std::runtime_error("Table not found in system tables: " + table_name);

        int table_id = table_records.front().table_id;

        auto index_records = indexes_system_table_.FindRecords(
                [&](const storage::IndexRecord& record) {
                    return record.table_id == table_id;
                });
        std::vector<std::pair<storage::IndexRecord, std::vector<std::string>>> index_with_columns;
        for (const auto& index_record : index_records) {
            int index_id = index_record.index_id;
            auto index_columns = index_columns_system_table_.FindRecords(
                    [&](const storage::IndexColumnRecord& record) {
                        return record.index_id == index_id;
                    });
            std::vector<std::string> column_names;
            for (const auto& index_column : index_columns) {
                int column_id = index_column.column_id;
                auto column_records = columns_system_table_.FindRecords(
                        [&](const storage::ColumnRecord& record) {
                            return record.column_id == column_id;
                        });

                if (!column_records.empty()) column_names.push_back(column_records.front().column_name);
            }
            index_with_columns.emplace_back(index_record, column_names);
        }

        return index_with_columns;
    }

    const storage::GenericSystemTable<storage::TableRecord>& Catalog::GetTablesSystemTable() const {
        return tables_system_table_;
    }

    const storage::GenericSystemTable<storage::ColumnRecord>& Catalog::GetColumnsSystemTable() const {
        return columns_system_table_;
    }

    const storage::GenericSystemTable<storage::IndexRecord>& Catalog::GetIndexesSystemTable() const {
        return indexes_system_table_;
    }

    const storage::GenericSystemTable<storage::IndexColumnRecord>& Catalog::GetIndexColumnsSystemTable() const {
        return index_columns_system_table_;
    }


    void Catalog::LoadSystemTables() {
        // todo
    }

    void Catalog::SaveSystemTables() const {
        // todo
    }

    template void Catalog::CreateIndex<int>(const std::string&, const std::string&, size_t, int);
    template void Catalog::CreateIndex<double>(const std::string&, const std::string&, size_t, int);
    template void Catalog::CreateIndex<std::string>(const std::string&, const std::string&, size_t, int);
}
