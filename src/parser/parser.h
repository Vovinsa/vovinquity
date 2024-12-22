#pragma once

#include "planner.h"

namespace parser {
    class Parser {
    public:
        static std::unique_ptr<planner::PlanNode> Parse(const std::string& query);
    };
}
