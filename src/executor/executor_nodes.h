#pragma once

#include "planner.h"
#include "tuple.h"
#include "regex"
#include "bplus_index.h"
#include "schema.h"
#include <stdexcept>
#include <limits>
#include <memory>
#include <unordered_map>
#include <algorithm>
#include <iostream>

namespace executor {
    class ExecutorNode {
    public:
        explicit ExecutorNode(planner::PlanNode *plan) : plan_(plan) {};
        virtual ~ExecutorNode() = default;
        virtual std::vector<storage::Tuple> Execute() = 0;

    protected:
        planner::PlanNode *plan_;
    };

    class SelectExecutor : public ExecutorNode {
    public:
        SelectExecutor(planner::SelectNode *plan, std::shared_ptr<catalog::Catalog> catalog)
                : ExecutorNode(plan), catalog_(catalog) {}
        std::vector<storage::Tuple> Execute() override {
            auto select_node = dynamic_cast<planner::SelectNode*>(plan_);
            std::string table_name = select_node->GetTableName();
            if (!catalog_->HasTable(table_name)) throw std::runtime_error("Table not found " + table_name);
            auto table = catalog_->GetTable(table_name);

            auto full_schema = table->GetSchema();

            std::vector<storage::Column> selected_columns;
            selected_columns.reserve(select_node->GetColumns().size());
            std::vector<size_t> column_indexes;
            column_indexes.reserve(select_node->GetColumns().size());

            for (const auto &col_name : select_node->GetColumns()) {
                if (col_name == "*") {
                    for (size_t i = 0; i < full_schema.GetColumnCount(); i++) {
                        selected_columns.push_back(full_schema.GetColumn(i));
                        column_indexes.push_back(i);
                    }
                    break;
                } else {
                    size_t idx = full_schema.GetColumnIndex(col_name);
                    selected_columns.push_back(full_schema.GetColumn(idx));
                    column_indexes.push_back(idx);
                }
            }


            storage::Schema select_schema;
            for (auto& select_column : selected_columns) {
                select_schema.InsertColumn(select_column.name, select_column.type);
            }

            std::vector<storage::Tuple> result;
            for (const auto &rid: table->GetAllRID()) {
                auto tuple = table->GetTuple(rid);
                if (!tuple) continue;
                std::vector<storage::Field> selected_fields;
                selected_fields.reserve(column_indexes.size());

                for (auto idx : column_indexes)
                    selected_fields.push_back(tuple->GetField(idx));

                result.emplace_back(select_schema, std::move(selected_fields));
            }
            return result;
        }

    private:
        std::shared_ptr<catalog::Catalog> catalog_;
    };

    class FilterExecutor : public ExecutorNode {
    public:
        FilterExecutor(planner::FilterNode *plan, std::unique_ptr<ExecutorNode> child_executor,
                       std::shared_ptr<catalog::Catalog> catalog)
                : ExecutorNode(plan), child_executor_(std::move(child_executor)), catalog_(std::move(catalog)) {};

        std::vector<storage::Tuple> Execute() override {
            auto filter_node = dynamic_cast<planner::FilterNode*>(plan_);
            auto input = child_executor_->Execute();
            auto [op, value] = ParsePredicate(filter_node->GetPredicate());
            auto table = catalog_->GetTable(filter_node->GetTableName());
            std::vector<storage::Tuple> result;
            if (!filter_node->GetIndexName().empty()) {
                std::vector<storage::RID> rids;
                switch (value.index()) {
                    case 0: {
                        auto index = table->GetIndex<int>(filter_node->GetIndexName());
                        rids = PerformSearch(op, std::get<int>(value), index);
                        break;
                    }
                    case 1: {
                        auto index = table->GetIndex<double>(filter_node->GetIndexName());
                        rids = PerformSearch(op, std::get<double>(value), index);
                        break;
                    }
                    case 2: {
                        auto index = table->GetIndex<std::string>(filter_node->GetIndexName());
                        rids = PerformSearch(op, std::get<std::string>(value), index);
                        break;
                    }
                    default:
                        throw std::runtime_error("Unsupported index type");
                }
                for (auto rid: rids) {
                    auto tuple = table->GetTuple(rid);
                    result.push_back(*tuple);
                }
            } else {
                for (const auto &tuple: input) {
                    auto a = filter_node->GetColumnName();
                    auto index = tuple.GetFieldIndex(filter_node->GetColumnName());
                    auto field = tuple.GetField(index);
                    if (EvaluatePredicate(field, op, value)) result.push_back(tuple);
                }
            }
            return result;
        }

    private:
        std::unique_ptr<ExecutorNode> child_executor_;
        std::shared_ptr<catalog::Catalog> catalog_;

        std::pair<std::string, storage::Field> ParsePredicate(const std::string &predicate) {
            std::regex pattern(R"(^(\S+)\s*(<=|>=|<|>|=)\s*(\S+)$)");
            std::smatch matches;
            if (std::regex_match(predicate, matches, pattern)) {
                std::string col = matches[1].str();
                std::string op = matches[2].str();
                std::string value_str = matches[3].str();

                if (std::regex_match(value_str, std::regex(R"(\d+\.\d+)"))) {
                    return {op, std::stod(value_str)};
                } else if (std::regex_match(value_str, std::regex(R"(\d+)"))) {
                    return {op, std::stoi(value_str)};
                } else {
                    return {op, value_str};
                }
            }
            throw std::invalid_argument("Invalid predicate format: " + predicate);
        }

        template<typename KeyType>
        std::vector<storage::RID> PerformSearch(const std::string &op, KeyType value,
                                                std::shared_ptr<storage::BPlusIndex<KeyType>> index) {
            if (op == ">") {
                return index->RangeQuery(value, std::numeric_limits<KeyType>::max());
            } else if (op == "<") {
                return index->RangeQuery(std::numeric_limits<KeyType>::min(), value);
            } else if (op == "=") {
                return index->Search(value);
            }
            throw std::invalid_argument("Unsupported operator for range query: " + op);
        }

        bool EvaluatePredicate(const storage::Field &field, const std::string &op, const storage::Field &value) {
            return std::visit([&](auto &&field_value) -> bool {
                using FieldType = std::decay_t<decltype(field_value)>;
                if constexpr (std::is_same_v<FieldType, int> || std::is_same_v<FieldType, double>) {
                    if (auto val_ptr = std::get_if<FieldType>(&value)) {
                        if (op == ">") return field_value > *val_ptr;
                        if (op == "<") return field_value < *val_ptr;
                        if (op == "=") return field_value == *val_ptr;
                    }
                } else if constexpr (std::is_same_v<FieldType, std::string>) {
                    if (auto val_ptr = std::get_if<std::string>(&value)) {
                        if (op == "=") return field_value == *val_ptr;
                    }
                }
                return false;
            }, field);
        }
    };

    class SortExecutor : public ExecutorNode {
    public:
        SortExecutor(planner::SortNode* plan, std::unique_ptr<ExecutorNode> child_executor)
                : ExecutorNode(plan), child_executor_(std::move(child_executor)) {}
        std::vector<storage::Tuple> Execute() override {
            auto sort_node = dynamic_cast<planner::SortNode*>(plan_);
            auto input = child_executor_->Execute();

            std::vector<storage::Tuple*> pointers;
            for (auto &tuple: input) pointers.push_back(&tuple);

            std::sort(pointers.begin(), pointers.end(), [&sort_node](const storage::Tuple *a, const storage::Tuple *b) {
                for (const auto &column: sort_node->GetSortColumns()) {
                    auto index_a = a->GetFieldIndex(column);
                    auto index_b = b->GetFieldIndex(column);

                    const auto &field_a = a->GetField(index_a);
                    const auto &field_b = b->GetField(index_b);

                    if (field_a < field_b) return true;
                    if (field_a > field_b) return false;
                }
                return false;
            });

            std::vector<storage::Tuple> sorted_tuples;
            sorted_tuples.reserve(input.size());
            for (auto *ptr: pointers)
                sorted_tuples.push_back(*ptr);

            return sorted_tuples;
        }
    private:
        std::unique_ptr<ExecutorNode> child_executor_;
    };

    class AggregateExecutor : public ExecutorNode {
    public:
        AggregateExecutor(planner::AggregateNode* plan,
                          std::unique_ptr<ExecutorNode> child_executor,
                          std::shared_ptr<catalog::Catalog> catalog)
                : ExecutorNode(plan),
                  child_executor_(std::move(child_executor)),
                  catalog_(std::move(catalog)) {}

        std::vector<storage::Tuple> Execute() override {
            auto agg_node = dynamic_cast<planner::AggregateNode*>(plan_);
            if (!agg_node) throw std::runtime_error("Invalid plan node cast for AggregateExecutor");

            auto table_name = agg_node->GetTableName();
            if (!catalog_->HasTable(table_name)) {
                throw std::runtime_error("Table not found: " + table_name);
            }

            auto input = child_executor_->Execute();
            if (input.empty() && agg_node->GetGroupColumns().empty() && !agg_node->GetAggregates().empty()) {
                return BuildEmptyAggregateResult(*agg_node);
            } else if (input.empty()) {
                return {};
            }

            const auto &schema = input[0].GetSchema();

            const auto &group_by_cols = agg_node->GetGroupColumns();
            const auto &agg_instructions = agg_node->GetAggregates();

            std::vector<size_t> group_by_indexes;
            group_by_indexes.reserve(group_by_cols.size());
            for (auto &col_name : group_by_cols) {
                group_by_indexes.push_back(schema.GetColumnIndex(col_name));
            }

            std::vector<std::pair<size_t, planner::AggInstruction>> agg_cols;
            agg_cols.reserve(agg_instructions.size());
            for (auto &agg : agg_instructions) {
                size_t idx = schema.GetColumnIndex(agg.column_name);
                agg_cols.emplace_back(idx, agg);
            }

            std::unordered_map<std::vector<storage::Field>, std::vector<storage::Tuple>, VectorFieldHash, VectorFieldEqual> groups;
            for (auto &t: input) {
                std::vector<storage::Field> key;
                key.reserve(group_by_indexes.size());
                for (auto idx : group_by_indexes) key.push_back(t.GetField(idx));

                groups[key].push_back(t);
            }

            storage::Schema output_schema = BuildOutputSchema(schema, group_by_cols, agg_cols);

            std::vector<storage::Tuple> result;
            result.reserve(groups.size());

            for (auto &kv : groups) {
                const auto &key = kv.first;
                const auto &tuples = kv.second;

                std::vector<storage::Field> agg_values = ComputeAggregates(tuples, agg_cols);

                std::vector<storage::Field> out_fields;
                out_fields.reserve(key.size() + agg_values.size());
                out_fields.insert(out_fields.end(), key.begin(), key.end());
                out_fields.insert(out_fields.end(), agg_values.begin(), agg_values.end());

                result.emplace_back(output_schema, std::move(out_fields));
            }

            return result;
        }

    private:
        std::unique_ptr<ExecutorNode> child_executor_;
        std::shared_ptr<catalog::Catalog> catalog_;

        struct FieldHash {
            std::size_t operator()(const storage::Field &f) const noexcept {
                return std::visit([](auto &&arg) {
                    using T = std::decay_t<decltype(arg)>;
                    if constexpr (std::is_same_v<T,int>) return std::hash<int>()(arg);
                    else if constexpr (std::is_same_v<T,double>) return std::hash<double>()(arg);
                    else return std::hash<std::string>()(arg);
                }, f);
            }
        };

        struct FieldEqual {
            bool operator()(const storage::Field &a, const storage::Field &b) const noexcept {
                return a == b;
            }
        };

        struct VectorFieldHash {
            std::size_t operator()(const std::vector<storage::Field> &fields) const noexcept {
                std::size_t seed = 0;
                for (auto &f : fields) {
                    seed ^= FieldHash()(f) + 0x9e3779b97f4a7c16ULL + (seed<<6) + (seed>>2);
                }
                return seed;
            }
        };

        struct VectorFieldEqual {
            bool operator()(const std::vector<storage::Field> &a, const std::vector<storage::Field> &b) const noexcept {
                if (a.size() != b.size()) return false;
                for (size_t i = 0; i < a.size(); i++) {
                    if (!FieldEqual()(a[i], b[i])) return false;
                }
                return true;
            }
        };

        storage::Schema BuildOutputSchema(const storage::Schema &input_schema,
                                          const std::vector<std::string> &group_cols,
                                          const std::vector<std::pair<size_t, planner::AggInstruction>> &agg_cols) {
            storage::Schema output_schema;
            for (auto &g_col : group_cols) {
                size_t idx = input_schema.GetColumnIndex(g_col);
                const auto &col_def = input_schema.GetColumn(idx);
                output_schema.InsertColumn(col_def.name, col_def.type);
            }
            for (auto &p : agg_cols) {
                auto &agg = p.second;
                const auto &col_def = input_schema.GetColumn(p.first);
                storage::DataType out_type;
                switch (agg.type) {
                    case planner::AggType::SUM:
                    case planner::AggType::AVG:
                        out_type = storage::DataType::DOUBLE;
                        break;
                    case planner::AggType::COUNT:
                        out_type = storage::DataType::INTEGER;
                        break;
                }

                std::string agg_col_name;
                switch (agg.type) {
                    case planner::AggType::SUM:   agg_col_name = "SUM(" + agg.column_name + ")";   break;
                    case planner::AggType::COUNT: agg_col_name = "COUNT(" + agg.column_name + ")"; break;
                    case planner::AggType::AVG:   agg_col_name = "AVG(" + agg.column_name + ")";   break;
                }
                output_schema.InsertColumn(agg_col_name, out_type);
            }
            return output_schema;
        }

        std::vector<storage::Field> ComputeAggregates(const std::vector<storage::Tuple> &tuples,
                                                      const std::vector<std::pair<size_t, planner::AggInstruction>> &agg_cols) {
            std::vector<storage::Field> agg_values;
            agg_values.reserve(agg_cols.size());

            for (auto &inst : agg_cols) {
                const auto &agg = inst.second;
                std::vector<storage::Field> column_values;
                column_values.reserve(tuples.size());
                for (auto &t : tuples) column_values.push_back(t.GetField(inst.first));

                storage::Field agg_result;
                switch (agg.type) {
                    case planner::AggType::COUNT: {
                        int count_val = static_cast<int>(tuples.size());
                        agg_result = count_val;
                        break;
                    }
                    case planner::AggType::SUM: {
                        double sum_val = 0.0;
                        for (auto &f : column_values) {
                            if (std::holds_alternative<int>(f)) sum_val += std::get<int>(f);
                            else if (std::holds_alternative<double>(f)) sum_val += std::get<double>(f);
                        }
                        agg_result = sum_val;
                        break;
                    }
                    case planner::AggType::AVG: {
                        double sum_val = 0.0;
                        for (auto &f : column_values) {
                            if (std::holds_alternative<int>(f)) sum_val += std::get<int>(f);
                            else if (std::holds_alternative<double>(f)) sum_val += std::get<double>(f);
                        }
                        double avg_val = tuples.empty() ? 0.0 : sum_val / tuples.size();
                        agg_result = avg_val;
                        break;
                    }
                }
                agg_values.push_back(agg_result);
            }
            return agg_values;
        }

        std::vector<storage::Tuple> BuildEmptyAggregateResult(const planner::AggregateNode &agg_node) {
            std::vector<std::pair<size_t, planner::AggInstruction>> dummy_cols;
            storage::Schema dummy_input_schema;
            storage::Schema output_schema;
            for (auto &agg : agg_node.GetAggregates()) {
                storage::DataType out_type;
                switch (agg.type) {
                    case planner::AggType::COUNT:
                        out_type = storage::DataType::INTEGER;
                        break;
                    case planner::AggType::SUM:
                    case planner::AggType::AVG:
                        out_type = storage::DataType::DOUBLE;
                        break;
                }
                std::string agg_col_name;
                switch (agg.type) {
                    case planner::AggType::SUM:   agg_col_name = "SUM(" + agg.column_name + ")";   break;
                    case planner::AggType::COUNT: agg_col_name = "COUNT(" + agg.column_name + ")"; break;
                    case planner::AggType::AVG:   agg_col_name = "AVG(" + agg.column_name + ")";   break;
                }
                output_schema.InsertColumn(agg_col_name, out_type);
            }

            std::vector<storage::Field> agg_values;
            agg_values.reserve(agg_node.GetAggregates().size());
            for (auto &agg : agg_node.GetAggregates()) {
                switch (agg.type) {
                    case planner::AggType::COUNT:
                        agg_values.push_back(0);
                        break;
                    case planner::AggType::SUM:
                    case planner::AggType::AVG:
                        agg_values.push_back(0.0);
                        break;
                }
            }

            std::vector<storage::Tuple> result;
            result.emplace_back(output_schema, std::move(agg_values));
            return result;
        }
    };


    class CreateTableExecutor : public ExecutorNode {
    public:
        CreateTableExecutor(planner::CreateTableNode* plan, std::shared_ptr<catalog::Catalog> catalog)
                : ExecutorNode(plan), catalog_(std::move(catalog)) {};
        std::vector<storage::Tuple> Execute() override {
            auto create_table_node = dynamic_cast<planner::CreateTableNode*>(plan_);
            catalog_->CreateTable(create_table_node->GetTableName(), create_table_node->GetSchema());
            return {};
        }
    private:
        std::shared_ptr<catalog::Catalog> catalog_;
    };

    class InsertExecutor : public ExecutorNode {
    public:
        InsertExecutor(planner::InsertNode* plan, std::shared_ptr<catalog::Catalog> catalog)
                : ExecutorNode(plan), catalog_(std::move(catalog)) {}

        std::vector<storage::Tuple> Execute() override {
            auto insert_node = dynamic_cast<planner::InsertNode*>(plan_);
            if (!insert_node) {
                throw std::runtime_error("Invalid plan node cast for InsertExecutor");
            }

            auto table_name = insert_node->GetTableName();
            if (!catalog_->HasTable(table_name)) {
                throw std::runtime_error("Table not found: " + table_name);
            }

            auto table = catalog_->GetTable(table_name);
            const auto& schema = table->GetSchema();

            auto cols = insert_node->GetColumns();
            auto vals = insert_node->GetValues();

            if (cols.size() != vals.size()) {
                throw std::runtime_error("Mismatch between columns and values size in InsertExecutor");
            }

            std::vector<storage::Field> fields(schema.GetColumnCount());
            for (size_t i = 0; i < cols.size(); ++i) {
                size_t idx = schema.GetColumnIndex(cols[i]);
                fields[idx] = vals[i];
            }
            table->InsertTuple(fields);
            return {};
        }

    private:
        std::shared_ptr<catalog::Catalog> catalog_;
    };
}
