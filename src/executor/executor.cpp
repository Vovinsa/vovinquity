#include "executor.h"

namespace executor {
    std::unique_ptr<ExecutorNode> Executor::CreateExecutor(planner::PlanNode* plan) {
        switch (plan->GetType()) {
            case planner::SELECT_STATEMENT: {
                auto select_plan = dynamic_cast<planner::SelectNode*>(plan);
                return std::make_unique<SelectExecutor>(select_plan, catalog_);
            }
            case planner::FILTER_STATEMENT: {
                auto filter_plan = dynamic_cast<planner::FilterNode*>(plan);
                auto child_executor = CreateExecutor(filter_plan->GetChildren()[0].get());
                return std::make_unique<FilterExecutor>(filter_plan, std::move(child_executor), catalog_);
            }
            case planner::SORT_STATEMENT: {
                auto sort_plan = dynamic_cast<planner::SortNode*>(plan);
                auto child_executor = CreateExecutor(sort_plan->GetChildren()[0].get());
                return std::make_unique<SortExecutor>(sort_plan, std::move(child_executor));
            }
            case planner::AGGREGATE_STATEMENT: {
                auto agg_plan = dynamic_cast<planner::AggregateNode*>(plan);
                auto child_executor = CreateExecutor(agg_plan->GetChildren()[0].get());
                return std::make_unique<AggregateExecutor>(agg_plan, std::move(child_executor), catalog_);
            }
            case planner::CREATE_TABLE_STATEMENT: {
                auto create_table_plan = dynamic_cast<planner::CreateTableNode*>(plan);
                return std::make_unique<CreateTableExecutor>(create_table_plan, catalog_);
            }
            case planner::INSERT_STATEMENT: {
                auto insert_plan = dynamic_cast<planner::InsertNode*>(plan);
                return std::make_unique<InsertExecutor>(insert_plan, catalog_);
            }
            default:
                throw std::runtime_error("Unsupported plan node type");
        }
    }

}
