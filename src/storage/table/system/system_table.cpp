#include "system_table.h"

namespace storage {

    SystemTable::SystemTable(const Schema& schema) : Table(schema) {}

    RID SystemTable::InsertTuple(const std::vector<Field>& fields) {
        throw std::runtime_error("Cannot insert into system table directly");
    }

    bool SystemTable::RemoveTuple(RID rid) {
        throw std::runtime_error("Cannot delete from system table directly");
    }

    bool SystemTable::UpdateTuple(RID rid, const std::vector<Field> &fields) {
        throw std::runtime_error("Cannot update system table directly");
    }

    RID SystemTable::InternalInsertTuple(const std::vector<Field>& fields) {
        return Table::InsertTuple(fields);
    }

    bool SystemTable::InternalRemoveTuple(RID rid) {
        return Table::RemoveTuple(rid);
    }

    bool SystemTable::InternalUpdateTuple(const RID rid, const std::vector<Field>& fields) {
        return Table::UpdateTuple(rid, fields);
    }
}

