#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <stdexcept>

namespace planner {
    enum PlanNodeType {
        SELECT_STATEMENT,
        INSERT_STATEMENT,
        FILTER_STATEMENT,
        SORT_STATEMENT,
        AGGREGATE_STATEMENT,
        CREATE_TABLE_STATEMENT
    };

    class PlanNode {
    public:
        virtual ~PlanNode() = default;
        virtual PlanNodeType GetType() const = 0;
        virtual std::vector<std::unique_ptr<PlanNode>>& GetChildren() = 0;
    };

    class SelectNode : public PlanNode {
    public:
        explicit SelectNode(std::vector<std::string> columns, std::string table_name)
                : columns_(std::move(columns)), table_name_(std::move(table_name)) {}

        PlanNodeType GetType() const override { return SELECT_STATEMENT; }
        std::vector<std::unique_ptr<PlanNode>>& GetChildren() override { return empty_children_; }
        const std::vector<std::string>& GetColumns() const { return columns_; }
        const std::string& GetTableName() const { return table_name_; }
    private:
        std::vector<std::string> columns_;
        std::string table_name_;
        static std::vector<std::unique_ptr<PlanNode>> empty_children_;
    };

    class InsertNode : public PlanNode {
    public:
        InsertNode(std::string table_name, std::vector<std::string> columns, std::vector<storage::Field> values)
                : table_name_(std::move(table_name)), columns_(std::move(columns)), values_(std::move(values)) {}
        PlanNodeType GetType() const override { return INSERT_STATEMENT; }
        std::vector<std::unique_ptr<PlanNode>>& GetChildren() override { return empty_children_; }
        const std::string& GetTableName() const { return table_name_; }
        const std::vector<std::string>& GetColumns() const { return columns_; }
        const std::vector<storage::Field>& GetValues() const { return values_; }
    private:
        std::string table_name_;
        std::vector<std::string> columns_;
        std::vector<storage::Field> values_;
        static std::vector<std::unique_ptr<PlanNode>> empty_children_;
    };

    class FilterNode : public PlanNode {
    public:
        FilterNode(std::unique_ptr<PlanNode> child, std::string predicate, std::string column_name, std::string index_name,
                   std::string table_name)
                : predicate_(std::move(predicate)), column_name_(std::move(column_name)), index_name_(std::move(index_name)),
                table_name_(std::move(table_name)) {
            children_.push_back(std::move(child));
        }
        PlanNodeType GetType() const override { return FILTER_STATEMENT; }
        std::vector<std::unique_ptr<PlanNode>>& GetChildren() override { return children_; }
        const std::string& GetPredicate() const { return predicate_; }
        const std::string& GetColumnName() const { return column_name_; }
        const std::string& GetTableName() const { return table_name_; }
        const std::string& GetIndexName() const { return index_name_; }
    private:
        std::string table_name_;
        std::string predicate_;
        std::string column_name_;
        std::string index_name_;
        std::vector<std::unique_ptr<PlanNode>> children_;
    };

    class SortNode : public PlanNode {
    public:
        SortNode(std::unique_ptr<PlanNode> child, std::vector<std::string> sort_columns)
                : sort_columns_(std::move(sort_columns)) {
            children_.push_back(std::move(child));
        }
        PlanNodeType GetType() const override { return SORT_STATEMENT; }
        std::vector<std::unique_ptr<PlanNode>>& GetChildren() override { return children_; }
        const std::vector<std::string>& GetSortColumns() const { return sort_columns_; }
    private:
        std::string table_name_;
        std::vector<std::string> sort_columns_;
        std::vector<std::unique_ptr<PlanNode>> children_;
    };

    enum class AggType {
        SUM,
        COUNT,
        AVG
    };

    struct AggInstruction {
        AggType type;
        std::string column_name;
    };

    class AggregateNode : public PlanNode {
    public:
        AggregateNode(std::unique_ptr<PlanNode> child,
                      std::vector<std::string> group_columns,
                      std::vector<AggInstruction> aggregates,
                      std::string table_name)
                : group_columns_(std::move(group_columns)),
                  aggregates_(std::move(aggregates)),
                  table_name_(std::move(table_name)) {
            children_.push_back(std::move(child));
        }
        PlanNodeType GetType() const override { return AGGREGATE_STATEMENT; }
        std::vector<std::unique_ptr<PlanNode>>& GetChildren() override { return children_; }
        const std::vector<std::string>& GetGroupColumns() const { return group_columns_; }
        const std::vector<AggInstruction>& GetAggregates() const { return aggregates_; }
        const std::string& GetTableName() const { return table_name_; }
    private:
        std::string table_name_;
        std::vector<std::string> group_columns_;
        std::vector<AggInstruction> aggregates_;
        std::vector<std::unique_ptr<PlanNode>> children_;
    };

    class CreateTableNode : public PlanNode {
    public:
        CreateTableNode(std::string table_name, storage::Schema schema)
                : table_name_(std::move(table_name)), schema_(std::move(schema)) {}
        PlanNodeType GetType() const override { return CREATE_TABLE_STATEMENT; }
        std::vector<std::unique_ptr<PlanNode>>& GetChildren() override { return empty_children_; }
        const std::string& GetTableName() const { return table_name_; }
        const storage::Schema& GetSchema() const { return schema_; }
    private:
        std::string table_name_;
        storage::Schema schema_;
        static std::vector<std::unique_ptr<PlanNode>> empty_children_;
    };
}
