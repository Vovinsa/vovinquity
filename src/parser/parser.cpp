#include "parser.h"
#include <algorithm>
#include <cctype>
#include <stdexcept>
#include <vector>
#include <iostream>
#include <charconv>

namespace {
    std::string ToUpper(const std::string &s) {
        std::string result;
        result.reserve(s.size());
        for (char c : s) {
            result.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
        }
        return result;
    }

    std::vector<std::string> SplitTokens(const std::string& query) {
        std::vector<std::string> tokens;
        std::string current;
        for (size_t i = 0; i < query.size(); i++) {
            char c = query[i];
            if (std::isspace(static_cast<unsigned char>(c))) {
                if (!current.empty()) {
                    tokens.push_back(current);
                    current.clear();
                }
                continue;
            } else if (c == '(' || c == ')' || c == ',' || c == ';' || c == '=') {
                if (!current.empty()) {
                    tokens.push_back(current);
                    current.clear();
                }
                tokens.push_back(std::string(1, c));
            } else current.push_back(c);
        }
        if (!current.empty()) tokens.push_back(current);

        return tokens;
    }

    void ExpectTokenCaseInsensitive(const std::vector<std::string>& tokens,
                                    size_t &pos,
                                    const std::string& expected_upper) {
        if (pos >= tokens.size()) {
            throw std::runtime_error("Unexpected end of query. Expected token: " + expected_upper);
        }
        std::string current_upper = ToUpper(tokens[pos]);
        if (current_upper != expected_upper) {
            throw std::runtime_error("Expected token: " + expected_upper + ", got: " + tokens[pos]);
        }
        ++pos;
    }

    bool MatchTokenCaseInsensitive(const std::vector<std::string>& tokens,
                                   size_t pos,
                                   const std::string& match_upper) {
        if (pos >= tokens.size()) return false;
        return (ToUpper(tokens[pos]) == match_upper);
    }

    bool TryParseInt(const std::string& token, int& out_value) {
        auto [p, ec] = std::from_chars(token.data(), token.data() + token.size(), out_value);
        if (ec == std::errc() && p == token.data() + token.size()) {
            return true;
        }
        return false;
    }

    bool TryParseDouble(const std::string& token, double& out_value) {
        try {
            size_t idx = 0;
            out_value = std::stod(token, &idx);
            if (idx == token.size()) return true;
            return false;
        } catch(...) {
            return false;
        }
    }

    std::string StripSingleQuotes(const std::string& s) {
        if (s.size() >= 2 && s.front() == '\'' && s.back() == '\'') {
            return s.substr(1, s.size() - 2);
        }
        return s;
    }

    storage::Field ParseLiteral(const std::string& token) {
        if (!token.empty() && token.front() == '\'' && token.back() == '\'') {
            return storage::Field{ StripSingleQuotes(token) };
        }
        {
            int int_val = 0;
            if (TryParseInt(token, int_val)) {
                return storage::Field{int_val};
            }
        }
        {
            double dbl_val = 0.0;
            if (TryParseDouble(token, dbl_val)) {
                return storage::Field{dbl_val};
            }
        }
        return storage::Field{ token };
    }

    storage::DataType ParseDataType(const std::string& type_str) {
        auto up = ToUpper(type_str);
        if (up == "INT" || up == "INTEGER") {
            return storage::DataType::INTEGER;
        } else if (up == "DOUBLE") {
            return storage::DataType::DOUBLE;
        } else if (up == "VARCHAR") {
            return storage::DataType::VARCHAR;
        }
        throw std::runtime_error("Unknown data type: " + type_str);
    }

    std::unique_ptr<planner::PlanNode> ParseSelect(const std::vector<std::string>& tokens, size_t& pos) {
        ExpectTokenCaseInsensitive(tokens, pos, "SELECT");

        std::vector<std::string> columns;
        while (pos < tokens.size()) {
            if (MatchTokenCaseInsensitive(tokens, pos, "FROM")) {
                break;
            }
            if (tokens[pos] == ",") {
                pos++;
                continue;
            }
            if (tokens[pos] == "*") {
                columns.push_back("*");
                pos++;
                break;
            }
            columns.push_back(tokens[pos]);
            pos++;
        }

        if (columns.empty()) {
            throw std::runtime_error("No columns specified after SELECT");
        }

        ExpectTokenCaseInsensitive(tokens, pos, "FROM");
        if (pos >= tokens.size()) {
            throw std::runtime_error("Table name expected after FROM");
        }
        std::string table_name = tokens[pos++];

        auto select_node = std::make_unique<planner::SelectNode>(columns, table_name);
        std::unique_ptr<planner::PlanNode> current_node = std::move(select_node);

        if (pos < tokens.size() && MatchTokenCaseInsensitive(tokens, pos, "WHERE")) {
            ++pos;
            if (pos >= tokens.size()) {
                throw std::runtime_error("Expected column after WHERE");
            }
            std::string where_col = tokens[pos++];

            if (pos >= tokens.size()) {
                throw std::runtime_error("Expected operator after column in WHERE clause");
            }
            std::string op = tokens[pos++];

            static const std::vector<std::string> valid_ops = {"=", "<", ">", "<=", ">="};
            if (std::find(valid_ops.begin(), valid_ops.end(), op) == valid_ops.end()) {
                throw std::runtime_error("Expected comparison operator (=,<,>,<=,>=) but got: " + op);
            }

            if (pos >= tokens.size()) {
                throw std::runtime_error("Expected value after operator in WHERE clause");
            }
            std::string where_value = tokens[pos++];

            std::string predicate = where_col + op + where_value;

            std::string index_name;
            current_node = std::make_unique<planner::FilterNode>(
                    std::move(current_node),
                    predicate,
                    where_col,
                    index_name,
                    table_name
            );
        }

        if (pos < tokens.size() && MatchTokenCaseInsensitive(tokens, pos, "GROUP")) {
            ++pos;
            ExpectTokenCaseInsensitive(tokens, pos, "BY");
            std::vector<std::string> group_cols;
            while (pos < tokens.size()) {
                if (MatchTokenCaseInsensitive(tokens, pos, "ORDER") ||
                    MatchTokenCaseInsensitive(tokens, pos, "WHERE") ||
                    tokens[pos] == ";") {
                    break;
                }
                if (tokens[pos] == ",") {
                    ++pos;
                    continue;
                }
                group_cols.push_back(tokens[pos]);
                ++pos;
            }
            if (group_cols.empty()) {
                throw std::runtime_error("No columns after GROUP BY");
            }
            std::vector<planner::AggInstruction> aggregates;

            current_node = std::make_unique<planner::AggregateNode>(
                    std::move(current_node),
                    group_cols,
                    aggregates,
                    table_name
            );
        }

        if (pos < tokens.size() && MatchTokenCaseInsensitive(tokens, pos, "ORDER")) {
            ++pos;
            ExpectTokenCaseInsensitive(tokens, pos, "BY");
            std::vector<std::string> sort_cols;
            while (pos < tokens.size()) {
                if (tokens[pos] == ";" ||
                    MatchTokenCaseInsensitive(tokens, pos, "WHERE") ||
                    MatchTokenCaseInsensitive(tokens, pos, "GROUP")) {
                    break;
                }
                if (tokens[pos] == ",") {
                    ++pos;
                    continue;
                }
                sort_cols.push_back(tokens[pos]);
                ++pos;
            }
            if (sort_cols.empty()) {
                throw std::runtime_error("No columns after ORDER BY");
            }
            current_node = std::make_unique<planner::SortNode>(
                    std::move(current_node),
                    sort_cols
            );
        }

        return current_node;
    }

    std::unique_ptr<planner::PlanNode> ParseInsert(const std::vector<std::string>& tokens, size_t& pos) {
        ExpectTokenCaseInsensitive(tokens, pos, "INSERT");
        ExpectTokenCaseInsensitive(tokens, pos, "INTO");

        if (pos >= tokens.size()) {
            throw std::runtime_error("Table name expected after INSERT INTO");
        }
        std::string table_name = tokens[pos++];

        if (pos >= tokens.size() || tokens[pos] != "(") {
            throw std::runtime_error("Expected '(' after table name in INSERT");
        }
        ++pos;

        std::vector<std::string> columns;
        while (pos < tokens.size()) {
            if (tokens[pos] == ")") {
                ++pos;
                break;
            }
            if (tokens[pos] == ",") {
                ++pos;
                continue;
            }
            columns.push_back(tokens[pos]);
            ++pos;
        }
        if (columns.empty()) {
            throw std::runtime_error("No columns specified in INSERT");
        }

        ExpectTokenCaseInsensitive(tokens, pos, "VALUES");

        if (pos >= tokens.size() || tokens[pos] != "(") {
            throw std::runtime_error("Expected '(' after VALUES in INSERT");
        }
        ++pos;

        std::vector<storage::Field> values;
        while (pos < tokens.size()) {
            if (tokens[pos] == ")") {
                ++pos;
                break;
            }
            if (tokens[pos] == ",") {
                ++pos;
                continue;
            }
            storage::Field field_val = ParseLiteral(tokens[pos]);
            values.push_back(field_val);
            ++pos;
        }
        if (values.empty()) {
            throw std::runtime_error("No values specified in INSERT");
        }
        if (values.size() != columns.size()) {
            throw std::runtime_error("Columns count differs from values count in INSERT");
        }

        auto insert_node = std::make_unique<planner::InsertNode>(
                table_name,
                columns,
                values
        );
        return insert_node;
    }

    std::unique_ptr<planner::PlanNode> ParseCreateTable(const std::vector<std::string>& tokens, size_t& pos) {
        ExpectTokenCaseInsensitive(tokens, pos, "CREATE");
        ExpectTokenCaseInsensitive(tokens, pos, "TABLE");

        if (pos >= tokens.size()) {
            throw std::runtime_error("Table name expected after CREATE TABLE");
        }
        std::string table_name = tokens[pos++];

        if (pos >= tokens.size() || tokens[pos] != "(") {
            throw std::runtime_error("Expected '(' after table name in CREATE TABLE");
        }
        ++pos;

        storage::Schema schema;
        while (pos < tokens.size()) {
            if (tokens[pos] == ")") {
                ++pos;
                break;
            }
            if (tokens[pos] == ",") {
                ++pos;
                continue;
            }
            std::string colName = tokens[pos++];
            if (pos >= tokens.size()) {
                throw std::runtime_error("Expected column type after column name");
            }
            std::string colTypeStr = tokens[pos++];
            storage::DataType colType = ParseDataType(colTypeStr);
            schema.InsertColumn(colName, colType);
        }
        if (schema.GetColumnCount() == 0) {
            throw std::runtime_error("No columns found in CREATE TABLE definition");
        }

        auto create_node = std::make_unique<planner::CreateTableNode>(table_name, schema);
        return create_node;
    }

}


namespace parser {
    std::unique_ptr<planner::PlanNode> Parser::Parse(const std::string& query) {
        auto tokens = SplitTokens(query);
        if (tokens.empty()) {
            throw std::runtime_error("Empty query");
        }

        size_t pos = 0;
        std::string first_upper = ToUpper(tokens[0]);

        if (first_upper == "SELECT") {
            return ParseSelect(tokens, pos);
        } else if (first_upper == "INSERT") {
            return ParseInsert(tokens, pos);
        } else if (first_upper == "CREATE") {
            return ParseCreateTable(tokens, pos);
        } else {
            throw std::runtime_error("Unsupported query type: " + tokens[0]);
        }
    }
}
