#pragma once

#include <vector>
#include <memory>

namespace storage {
    template<typename T>
    class BPlusTree {
    public:
        struct Node {
            bool isLeaf;
            std::vector<T> keys;
            std::vector<std::unique_ptr<Node>> children;
            Node* next;

            explicit Node(bool leaf = false) : isLeaf(leaf), next(nullptr) {}
        };

        explicit BPlusTree(int degree);
        ~BPlusTree() = default;

        void Insert(const T& key);
        void Remove(const T& key);
        bool Search(const T& key) const;
        std::vector<T> RangeQuery(const T& lower, const T& upper) const;
    private:
        std::unique_ptr<Node> root;
        int t;
        void SplitChild(Node* parent, int index, std::unique_ptr<Node>& child);
        void InsertNonFull(Node* node, const T& key);
        bool Search(const Node* node, const T& key) const;
        void Remove(Node* node, const T& key);
        void BorrowFromPrev(Node* node, int index);
        void BorrowFromNext(Node* node, int index);
        void Merge(Node* node, int index);
        int FindKey(Node* node, const T& key) const;
        void RemoveFromNonLeaf(Node* node, int index);
        T GetPred(Node* node, int index) const;
        T GetSucc(Node* node, int index) const;
        void Fill(Node* node, int index);
        void RangeQuery(const Node* node, T lower, T upper, std::vector<T>& result) const;
    };
}
