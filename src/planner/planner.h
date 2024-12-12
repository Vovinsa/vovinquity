#pragma once

#include "catalog.h"
#include "nodes.h"

namespace planner {
    class Planner {
    public:
        explicit Planner(std::shared_ptr<catalog::Catalog> catalog)
                : catalog_(std::move(catalog)) {}
        std::unique_ptr<PlanNode> CreatePlan(std::unique_ptr<PlanNode> logical_plan);
    private:
        std::shared_ptr<catalog::Catalog> catalog_;
        bool HasIndexForColumn(const std::string& table_name, const std::string& column_name, std::string& index_name) const;
    };
}
