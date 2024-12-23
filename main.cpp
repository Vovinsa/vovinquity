#include <iostream>
#include <string>
#include <memory>
#include <cstring>
#include <readline/history.h>
#include <readline/readline.h>

#include "catalog.h"
#include "parser.h"
#include "planner.h"
#include "executor.h"
#include "tuple.h"
#include "schema.h"
#include <iomanip>

void PrintTuplesAsTable(const std::vector<storage::Tuple> &tuples) {
    if (tuples.empty()) {
        std::cout << "(no rows)\n";
        return;
    }
    const auto &schema = tuples[0].GetSchema();
    auto columns = schema.GetColumns();
    size_t col_count = columns.size();

    std::vector<size_t> widths(col_count, 0);

    for (size_t c = 0; c < col_count; c++) {
        widths[c] = std::max(widths[c], columns[c].name.size());
    }

    for (auto &t : tuples) {
        for (size_t c = 0; c < col_count; c++) {
            std::string val_str = std::visit([](auto &&val) {
                std::ostringstream oss;
                oss << val;
                return oss.str();
            }, t.GetField(c));

            widths[c] = std::max(widths[c], val_str.size());
        }
    }

    for (size_t c = 0; c < col_count; c++) {
        std::cout << std::setw(static_cast<int>(widths[c])) << columns[c].name;
        if (c + 1 < col_count) std::cout << " | ";
    }
    std::cout << "\n";

    for (size_t c = 0; c < col_count; c++) {
        std::cout << std::string(widths[c], '-');
        if (c + 1 < col_count) std::cout << "-+-";
    }
    std::cout << "\n";

    for (auto &t : tuples) {
        for (size_t c = 0; c < col_count; c++) {
            std::string val_str = std::visit([](auto &&val) {
                std::ostringstream oss;
                oss << val;
                return oss.str();
            }, t.GetField(c));

            std::cout << std::setw(static_cast<int>(widths[c])) << val_str;
            if (c + 1 < col_count) std::cout << " | ";
        }
        std::cout << "\n";
    }
    std::cout << tuples.size() << " row(s).\n";
}


int main() {
    using_history();

    auto catalog = std::make_shared<catalog::Catalog>();

    planner::Planner planner(catalog);

    executor::Executor executor(catalog);

    std::cout << "Welcome to mini DB vovinquity!\n";
    std::cout << "Type EXIT or QUIT to stop.\n\n";

    while (true) {
        char* input = readline("sql> ");
        if (!input) {
            std::cout << "\nEOF received, exiting.\n";
            break;
        }

        if (std::strlen(input) == 0) {
            free(input);
            continue;
        }

        add_history(input);

        std::string query(input);

        free(input);

        {
            std::string upper;
            upper.reserve(query.size());
            for (char c : query) upper.push_back(std::toupper(static_cast<unsigned char>(c)));

            if (upper == "EXIT" || upper == "QUIT") {
                std::cout << "Bye!\n";
                break;
            }
        }

        try {
            auto logical_plan = parser::Parser::Parse(query);

            auto physical_plan = planner.CreatePlan(std::move(logical_plan));

            auto executor_node = executor.CreateExecutor(physical_plan.get());

            auto result = executor_node->Execute();

            if (!result.empty()) PrintTuplesAsTable(result);
            else std::cout << "(no rows)\n";
        }
        catch (const std::exception &ex) {
            std::cerr << "Error: " << ex.what() << "\n";
        }
    }

    return 0;
}
