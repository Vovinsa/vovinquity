#pragma once

#include <memory>
#include "planner.h"
#include "executor_nodes.h"
#include "catalog.h"

namespace executor {
    class Executor {
    public:
        explicit Executor(std::shared_ptr<catalog::Catalog> catalog)
                : catalog_(std::move(catalog)) {}
        std::unique_ptr<ExecutorNode> CreateExecutor(planner::PlanNode* plan);

    private:
        std::shared_ptr<catalog::Catalog> catalog_;
    };

}
