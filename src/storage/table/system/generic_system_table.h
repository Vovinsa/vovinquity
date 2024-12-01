#pragma once

#include "schema.h"
#include "tuple.h"
#include "system_table.h"
#include <string>
#include <vector>
#include <functional>

namespace storage {
    struct TableRecord {
        int table_id;
        std::string table_name;
    };

    struct ColumnRecord {
        int column_id;
        int table_id;
        std::string column_name;
        int data_type;
    };

    struct IndexRecord {
        int index_id;
        std::string index_name;
        int table_id;
    };

    struct IndexColumnRecord {
        int index_id;
        int column_id;
        int ordinal_position;
    };

    template<typename RecordType>
    class GenericSystemTable : public SystemTable {
    public:
        explicit GenericSystemTable(const Schema& schema);
        ~GenericSystemTable() = default;

        RID AddRecord(const RecordType& record);
        std::vector<RecordType> GetAllRecords() const;
        std::vector<RecordType> FindRecords(const std::function<bool(const RecordType&)>& predicate) const;

    private:
        std::vector<Field> RecordToFields(const RecordType& record) const;
        RecordType FieldsToRecord(const std::shared_ptr<Tuple>& tuple) const;
    };

    template<typename RecordType>
    GenericSystemTable<RecordType>::GenericSystemTable(const Schema& schema)
            : SystemTable(schema) {}

    template<typename RecordType>
    RID GenericSystemTable<RecordType>::AddRecord(const RecordType& record) {
        std::vector<Field> fields = RecordToFields(record);
        return InternalInsertTuple(fields);
    }

    template<typename RecordType>
    std::vector<RecordType> GenericSystemTable<RecordType>::GetAllRecords() const {
        std::vector<RecordType> records;
        for (const auto& [rid, tuple] : tuples_) {
            records.push_back(FieldsToRecord(tuple));
        }
        return records;
    }

    template<typename RecordType>
    std::vector<RecordType> GenericSystemTable<RecordType>::FindRecords(const std::function<bool(const RecordType&)>& predicate) const {
        std::vector<RecordType> records;
        for (const auto& [rid, tuple] : tuples_) {
            RecordType record = FieldsToRecord(tuple);
            if (predicate(record)) {
                records.push_back(record);
            }
        }
        return records;
    }

    template<>
    inline std::vector<Field> GenericSystemTable<TableRecord>::RecordToFields(const TableRecord& record) const {
        return {record.table_id, record.table_name};
    }

    template<>
    inline TableRecord GenericSystemTable<TableRecord>::FieldsToRecord(const std::shared_ptr<Tuple>& tuple) const {
        TableRecord record;
        record.table_id = std::get<int>(tuple->GetField(0));
        record.table_name = std::get<std::string>(tuple->GetField(1));
        return record;
    }

    template<>
    inline std::vector<Field> GenericSystemTable<ColumnRecord>::RecordToFields(const ColumnRecord& record) const {
        return {record.column_id, record.table_id, record.column_name, record.data_type};
    }

    template<>
    inline ColumnRecord GenericSystemTable<ColumnRecord>::FieldsToRecord(const std::shared_ptr<Tuple>& tuple) const {
        ColumnRecord record;
        record.column_id = std::get<int>(tuple->GetField(0));
        record.table_id = std::get<int>(tuple->GetField(1));
        record.column_name = std::get<std::string>(tuple->GetField(2));
        record.data_type = std::get<int>(tuple->GetField(3));
        return record;
    }

    template<>
    inline std::vector<Field> GenericSystemTable<IndexRecord>::RecordToFields(const IndexRecord& record) const {
        return {record.index_id, record.index_name, record.table_id};
    }

    template<>
    inline IndexRecord GenericSystemTable<IndexRecord>::FieldsToRecord(const std::shared_ptr<Tuple>& tuple) const {
        IndexRecord record;
        record.index_id = std::get<int>(tuple->GetField(0));
        record.index_name = std::get<std::string>(tuple->GetField(1));
        record.table_id = std::get<int>(tuple->GetField(2));
        return record;
    }

    template<>
    inline std::vector<Field> GenericSystemTable<IndexColumnRecord>::RecordToFields(const IndexColumnRecord& record) const {
        return {record.index_id, record.column_id, record.ordinal_position};
    }

    template<>
    inline IndexColumnRecord GenericSystemTable<IndexColumnRecord>::FieldsToRecord(const std::shared_ptr<Tuple>& tuple) const {
        IndexColumnRecord record;
        record.index_id = std::get<int>(tuple->GetField(0));
        record.column_id = std::get<int>(tuple->GetField(1));
        record.ordinal_position = std::get<int>(tuple->GetField(2));
        return record;
    }

}
