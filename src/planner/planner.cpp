#include "planner.h"

namespace planner {
    std::unique_ptr<PlanNode> Planner::CreatePlan(std::unique_ptr<PlanNode> logical_plan) {
        switch (logical_plan->GetType()) {
            case SELECT_STATEMENT: {
                auto select_node = dynamic_cast<SelectNode*>(logical_plan.get());
                if (!catalog_->HasTable(select_node->GetTableName())) {
                    throw std::runtime_error("Table not found: " + select_node->GetTableName());
                }
                return std::make_unique<SelectNode>(select_node->GetColumns(), select_node->GetTableName());
            }
            case INSERT_STATEMENT: {
                auto insert_node = dynamic_cast<InsertNode*>(logical_plan.get());
                if (!catalog_->HasTable(insert_node->GetTableName())) {
                    throw std::runtime_error("Table not found: " + insert_node->GetTableName());
                }
                return std::make_unique<InsertNode>(
                        insert_node->GetTableName(),
                        insert_node->GetColumns(),
                        insert_node->GetValues()
                        );
            }
            case FILTER_STATEMENT: {
                auto filter_node = dynamic_cast<FilterNode*>(logical_plan.get());
                if (!filter_node) {
                    throw std::runtime_error("Invalid FilterNode");
                }
                auto& children = filter_node->GetChildren();
                if (children.empty()) {
                    throw std::runtime_error("FilterNode has no children");
                }
                std::string index_name;
                bool has_index = HasIndexForColumn(filter_node->GetTableName(),
                                                   filter_node->GetColumnName(), index_name);
                auto child_plan = CreatePlan(std::move(children.front()));
                return std::make_unique<FilterNode>(
                        std::move(child_plan),
                        filter_node->GetPredicate(),
                        filter_node->GetColumnName(),
                        has_index ? index_name : "",
                        filter_node->GetTableName()
                        );
            }
            case SORT_STATEMENT: {
                auto sort_node = dynamic_cast<SortNode*>(logical_plan.get());
                if (!sort_node) throw std::runtime_error("Invalid SortNode");

                auto& children = sort_node->GetChildren();
                if (children.empty()) throw std::runtime_error("SortNode has no children");

                auto child_plan = CreatePlan(std::move(children.front()));
                return std::make_unique<SortNode>(
                        std::move(child_plan),
                        sort_node->GetSortColumns()
                );
            }
            case AGGREGATE_STATEMENT: {
                auto aggregate_node = dynamic_cast<AggregateNode*>(logical_plan.get());
                if (!aggregate_node) throw std::runtime_error("Invalid AggregateNode");

                auto& children = aggregate_node->GetChildren();
                if (children.empty()) throw std::runtime_error("AggregateNode has no children");

                auto child_plan = CreatePlan(std::move(children.front()));
                return std::make_unique<AggregateNode>(
                        std::move(child_plan),
                        aggregate_node->GetGroupColumns(),
                        aggregate_node->GetAggregateFunctions(),
                        aggregate_node->GetTableName()
                );
            }
            case CREATE_TABLE_STATEMENT: {
                auto create_table_node = dynamic_cast<CreateTableNode*>(logical_plan.get());
                if (!create_table_node) throw std::runtime_error("Invalid CreateTableNode");
                return std::make_unique<CreateTableNode>(create_table_node->GetTableName(), create_table_node->GetSchema());
            }
        }
    }

    bool Planner::HasIndexForColumn(const std::string& table_name, const std::string& column_name, std::string& index_name) const {
        auto indexes = catalog_->GetIndexesForTable(table_name);
        for (const auto& [index_record, column_names] : indexes) {
            for (const auto& indexed_column : column_names) {
                if (indexed_column == column_name) {
                    index_name = index_record.index_name;
                    return true;
                }
            }
        }
        return false;
    }

    std::vector<std::unique_ptr<planner::PlanNode>> planner::SelectNode::empty_children_;
    std::vector<std::unique_ptr<planner::PlanNode>> planner::InsertNode::empty_children_;
    std::vector<std::unique_ptr<planner::PlanNode>> planner::CreateTableNode::empty_children_;
}
