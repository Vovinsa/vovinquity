#include "bplus_tree.h"
#include "tuple.h"

namespace storage {
    template<typename T>
    BPlusTree<T>::BPlusTree(int degree) : root(nullptr), t(degree) {}

    template<typename T>
    void BPlusTree<T>::Insert(const T &key) {
        if (!root) {
            root = std::make_unique<Node>(true);
            root->keys.push_back(key);
            return;
        }
        if (root->keys.size() == 2 * t - 1) {
            auto newRoot = std::make_unique<Node>();
            newRoot->children.emplace_back(std::move(root));
            SplitChild(newRoot.get(), 0, newRoot->children[0]);
            root = std::move(newRoot);
        }
        InsertNonFull(root.get(), key);
    }

    template<typename T>
    bool BPlusTree<T>::Search(const T &key) const {
        return Search(root.get(), key);
    }

    template<typename T>
    bool BPlusTree<T>::Search(const BPlusTree<T>::Node *node, const T &key) const {
        if (!node) {
            return false;
        }
        int i = 0;
        while (i < node->keys.size() && key > node->keys[i]) {
            ++i;
        }
        if (i < node->keys.size() and node->keys[i] == key) {
            return true;
        }
        if (node->isLeaf) {
            return false;
        }
        return Search(node->children[i].get(), key);
    }

    template<typename T>
    void BPlusTree<T>::SplitChild(Node *parent, int index, std::unique_ptr<Node> &child) {
        auto newChild = std::make_unique<Node>(child->isLeaf);
        newChild->keys.assign(child->keys.begin() + t, child->keys.end());
        child->keys.resize(t - 1);

        if (!child->isLeaf) {
            newChild->children.assign(
                    std::make_move_iterator(child->children.begin() + t),
                    std::make_move_iterator(child->children.end())
            );
            child->children.resize(t);
        } else {
            newChild->next = child->next;
            child->next = newChild.get();
        }

        parent->keys.insert(parent->keys.begin() + index, child->keys[t - 1]);
        parent->children.emplace(parent->children.begin() + index + 1, std::move(newChild));
    }

    template<typename T>
    void BPlusTree<T>::InsertNonFull(Node *node, const T &key) {
        int i = node->keys.size() - 1;

        if (node->isLeaf) {
            node->keys.emplace_back();
            while (i >= 0 && key < node->keys[i]) {
                node->keys[i + 1] = node->keys[i];
                --i;
            }
            node->keys[i + 1] = key;
        } else {
            while (i >= 0 && key < node->keys[i]) {
                --i;
            }
            ++i;
            if (node->children[i]->keys.size() == 2 * t - 1) {
                SplitChild(node, i, node->children[i]);
                if (key > node->keys[i]) {
                    ++i;
                }
            }
            InsertNonFull(node->children[i].get(), key);
        }
    }

    template<typename T>
    void BPlusTree<T>::Remove(const T &key) {
        if (!root) {
            return;
        }
        Remove(root.get(), key);
        if (root->keys.empty()) {
            if (!root->isLeaf) {
                root = std::move(root->children[0]);
            } else {
                root = nullptr;
            }
        }
    }

    template<typename T>
    void BPlusTree<T>::Remove(Node *node, const T &key) {
        int index = FindKey(node, key);
        if (index < node->keys.size() && node->keys[index] == key) {
            if (node->isLeaf) {
                node->keys.erase(node->keys.begin() + index);
            } else {
                RemoveFromNonLeaf(node, index);
            }
        } else {
            if (node->isLeaf) {
                return;
            }
            bool flag = (index == node->keys.size());
            if (node->children[index]->keys.size() < t) {
                Fill(node, index);
            }
            if (flag && index > node->keys.size()) {
                Remove(node->children[index - 1].get(), key);
            } else {
                Remove(node->children[index].get(), key);
            }
        }
    }

    template<typename T>
    int BPlusTree<T>::FindKey(Node *node, const T &key) const {
        int index = 0;
        while (index < node->keys.size() && node->keys[index] < key) {
            ++index;
        }
        return index;
    }

    template<typename T>
    void BPlusTree<T>::RemoveFromNonLeaf(Node *node, int index) {
        T key = node->keys[index];
        if (node->children[index]->keys.size() >= t) {
            T pred = GetPred(node, index);
            node->keys[index] = pred;
            Remove(node->children[index].get(), pred);
        } else if (node->children[index + 1]->keys.size() >= t) {
            T succ = GetSucc(node, index);
            node->keys[index] = succ;
            Remove(node->children[index + 1].get(), succ);
        } else {
            Merge(node, index);
            Remove(node->children[index].get(), key);
        }
    }

    template<typename T>
    T BPlusTree<T>::GetPred(Node *node, int index) const {
        Node *curr = node->children[index].get();
        while (!curr->isLeaf) {
            curr = curr->children[curr->children.size() - 1].get();
        }
        return curr->keys[curr->keys.size() - 1];
    }

    template<typename T>
    T BPlusTree<T>::GetSucc(Node *node, int index) const {
        Node *cur = node->children[index + 1].get();
        while (!cur->isLeaf) {
            cur = cur->children[0].get();
        }
        return cur->keys[0];

    }

    template<typename T>
    void BPlusTree<T>::Fill(Node *node, int index) {
        if (index != 0 && node->children[index - 1]->keys.size() >= t) {
            BorrowFromPrev(node, index);
        } else if (index != node->keys.size() && node->children[index + 1]->keys.size() >= t) {
            BorrowFromNext(node, index);
        } else {
            if (index != node->keys.size()) {
                Merge(node, index);
            } else {
                Merge(node, index - 1);
            }
        }
    }

    template<typename T>
    void BPlusTree<T>::BorrowFromPrev(Node *node, int index) {
        Node *child = node->children[index].get();
        Node *sibling = node->children[index - 1].get();

        child->keys.insert(child->keys.begin(), node->keys[index - 1]);

        if (!child->isLeaf) {
            child->children.insert(child->children.begin(), std::move(sibling->children.back()));
            sibling->children.pop_back();
        }

        node->keys[index - 1] = sibling->keys.back();
        sibling->keys.pop_back();
    }

    template<typename T>
    void BPlusTree<T>::BorrowFromNext(Node *node, int index) {
        Node *child = node->children[index].get();
        Node *sibling = node->children[index + 1].get();

        child->keys.push_back(node->keys[index]);

        if (!child->isLeaf) {
            child->children.push_back(std::move(sibling->children[0]));
            sibling->children.erase(sibling->children.begin());
        }

        node->keys[index] = sibling->keys[0];
        sibling->keys.erase(sibling->keys.begin());
    }

    template<typename T>
    void BPlusTree<T>::Merge(Node *node, int index) {
        Node *child = node->children[index].get();
        Node *sibling = node->children[index + 1].get();

        child->keys.push_back(node->keys[index]);

        child->keys.insert(child->keys.end(),
                           sibling->keys.begin(),
                           sibling->keys.end());

        if (!child->isLeaf) {
            child->children.insert(child->children.end(),
                                   std::make_move_iterator(sibling->children.begin()),
                                   std::make_move_iterator(sibling->children.end())
            );
        } else {
            child->next = sibling->next;
        }

        node->keys.erase(node->keys.begin() + index);
        node->children.erase(node->children.begin() + index + 1);
    }

    template<typename T>
    std::vector<T> BPlusTree<T>::RangeQuery(const T &lower, const T &upper) const {
        std::vector<T> result;
        RangeQuery(root.get(), lower, upper, result);
        return result;
    }

    template<typename T>
    void BPlusTree<T>::RangeQuery(const Node *node, T lower, T upper, std::vector<T> &result) const {
        if (!node) {
            return;
        }

        int i = 0;
        while (i < node->keys.size() && node->keys[i] < lower) {
            ++i;
        }

        for (; i < node->keys.size() && node->keys[i] <= upper; ++i) {
            if (node->isLeaf) {
                result.push_back(node->keys[i]);
            } else {
                RangeQuery(node->children[i].get(), lower, upper, result);
                result.push_back(node->keys[i]);
            }
        }

        if (!node->isLeaf) {
            RangeQuery(node->children[i].get(), lower, upper, result);
        }
    }

    template class BPlusTree<int>;
    template class BPlusTree<double>;
    template class BPlusTree<std::string>;
}
