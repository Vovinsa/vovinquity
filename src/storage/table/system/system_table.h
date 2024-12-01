#pragma once

#include "table.h"

namespace storage {
    class SystemTable : public Table {
    public:
        explicit SystemTable(const Schema& schema);
        ~SystemTable() = default;

        RID InsertTuple(const std::vector<Field> &fields) override;
        bool RemoveTuple(RID rid) override;
        bool UpdateTuple(RID rid, const std::vector<Field> &fields) override;

    protected:
        bool InternalRemoveTuple(RID rid);
        bool InternalUpdateTuple(const RID rid, const std::vector<Field>& fields);
        RID InternalInsertTuple(const std::vector<Field>& fields);
    };
}