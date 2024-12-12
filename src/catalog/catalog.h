#pragma once

#include "generic_system_table.h"
#include "schema.h"
#include "table.h"
#include <unordered_map>
#include <string>
#include <memory>

namespace catalog {
    class Catalog {
    public:
        Catalog();
        ~Catalog() = default;

        void CreateTable(const std::string& table_name, const storage::Schema& schema);
        std::shared_ptr<storage::Table> GetTable(const std::string& table_name) const;
        bool HasTable(const std::string& table_name) const;
        void DropTable(const std::string& table_name);

        template<typename KeyType>
        void CreateIndex(const std::string& index_name, const std::string& table_name, size_t column_index, int degree);
        std::vector<std::pair<storage::IndexRecord, std::vector<std::string>>> GetIndexesForTable(const std::string& table_name) const;

        const storage::GenericSystemTable<storage::TableRecord>& GetTablesSystemTable() const;
        const storage::GenericSystemTable<storage::ColumnRecord>& GetColumnsSystemTable() const;
        const storage::GenericSystemTable<storage::IndexRecord>& GetIndexesSystemTable() const;
        const storage::GenericSystemTable<storage::IndexColumnRecord>& GetIndexColumnsSystemTable() const;


        void LoadSystemTables();
        void SaveSystemTables() const;

    private:
        storage::GenericSystemTable<storage::TableRecord> tables_system_table_;
        storage::GenericSystemTable<storage::ColumnRecord> columns_system_table_;
        storage::GenericSystemTable<storage::IndexRecord> indexes_system_table_;
        storage::GenericSystemTable<storage::IndexColumnRecord> index_columns_system_table_;

        std::unordered_map<std::string, std::shared_ptr<storage::Table>> tables_;

        int next_table_id_;
        int next_column_id_;
        int next_index_id_;
    };

}
