"""
To run it as a script:
    python apriori.py -f <path_file> -d <delimiter> -s min_support -c min_confidence
    python apriori.py -f restructured.csv -d "," -s 0.1 -c 0

For further details: python apriori.py -h
"""


from itertools import combinations
import argparse


# DATA STRUCTURES

# Define class for a transaction set

class Transactions:

    # Initialization

    def __init__(self, data_iter):
        """
        Argument: data_iter -- iterable object
        ( e.g. [["diapers", "beer"], ["beer", "diapers", "chips", "eggs"]] )

        To define an instance: transactions = Transactions(data_iter)
        """
        # private variables
        self.__singletons = []      # list of items
        self.__TIDs_by_item = {}    # dictionary item - {transaction_ids}
        self.__transaction_id = 0   # number of transactions
        self.map_items(data_iter)

    def map_items(self, data_iter):
        """
        For each transaction map items into a dictionary. Each item(key) is
        associated with the set of transaction IDs containing that item

        It also populates the list of single items and gets the number of transactions
        """
        for transaction in data_iter:
            for item in transaction:
                if not item in self.__TIDs_by_item:
                    self.__singletons.append([item])
                    self.__TIDs_by_item[item] = set()
                self.__TIDs_by_item[item].add(self.__transaction_id)
            self.__transaction_id += 1

    # methods associated with the class

    def get_singletons(self):
        """ Returns the sorted list of items """
        return sorted(self.__singletons)

    def get_count(self, itemset):
        """
        Calculate the number of occurrences of an itemset
        Argument: itemset -- iterable object ( e.g. ["diapers", "beer"] )
        """

        if not itemset:  # the empty set is contained in all transactions
            return 1.0
        if self.__transaction_id == 0:  # if there are no transactions, then
            return 0.0                  # each itemset has zero support

        inter_TIDs = set()  # intersection of transaction IDs
        for item in itemset:
            # get the set of transaction IDs that contain the item
            TIDs = self.__TIDs_by_item.get(item)

            if not TIDs:    # if an item is not contained in any transaction,
                return 0.0  # then the itemset has zero support

            if not inter_TIDs:
                inter_TIDs = TIDs
            else:
                inter_TIDs = inter_TIDs.intersection(TIDs)

        return float(len(inter_TIDs))

    def num_transactions(self):
        """ Returns the number of transactions """
        return self.__transaction_id


# AUXILIARY FUNCTIONS

def apriori_gen(prev_freq_itemsets, length):
    """
    Returns the list of candidates generated according to the logic of Apriori algorithm

    Arguments:
        prev_freq_itemsets -- list of frequent itemsets found in the last step
        length -- length (number of items) of candidates to be generated
    """
    tmp_candidates = self_join(prev_freq_itemsets, length)
    return prune(tmp_candidates, prev_freq_itemsets, length)

def self_join(prev_freq_itemsets, length):
    """
    Returns a temporary list of candidates, obtained through the union of
    the frequent itemsets found in the last step

    Because of the way the algorithm is structured, it is critical that the
    itemset list is sorted and that the items are sorted within each itemset
    """
    tmp_candidates = []
    for i in range(len(prev_freq_itemsets)):
        for j in range(i+1, len(prev_freq_itemsets)):
            # compare the first (length-2) items
            if sorted(prev_freq_itemsets[i])[:length-2] == sorted(prev_freq_itemsets[j])[:length-2]:
                tmp_candidates.append(sorted(prev_freq_itemsets[i].union(prev_freq_itemsets[j])))
    return tmp_candidates

def prune(tmp_candidates, prev_freq_itemsets, length):
    """
    Filter candidates
    A candidate is such only if each of its subsets of cardinality (length-1)
    belongs to the frequent itemsets found in the last step
    """
    next_candidates = [
        itemset for itemset in tmp_candidates
        if all(set(x) in prev_freq_itemsets for x in combinations(itemset, length-1))
    ]
    return next_candidates

# MAIN FUNCTIONS

def apriori_find_freq_sets(transactions, min_support):
    """
    Applies the Improved Apriori algorithm to find frequent itemsets and
    yields a generator

    Arguments:
        transactions -- instance of type Transactions
        min_support -- minimum support (> 0)
    """
    if min_support <= 0:
        raise ValueError('minimum support must be strictly > 0')

    candidates = transactions.get_singletons()
    k = 1
    while candidates:
        k_freq_itemsets = []
        for itemset in candidates:
            count = transactions.get_count(itemset)
            if count >= min_support * transactions.num_transactions():
                k_freq_itemsets.append(set(itemset))
                yield itemset, count  # tuple (frequent itemset, count)
        candidates = apriori_gen(k_freq_itemsets, k+1)
        k += 1

def gen_rules(transactions, freq_itemset, min_confidence):
    """
    Generates association rules from a frequent itemset and yields a generator

    Arguments:
        transactions -- instance of type Transactions
        freq_itemset -- tuple of the form (frequent itemset, count)
        min_confidence -- minimum confidence

    The implementation is along the lines of the function apriori_find_freq_sets
    """

    # start by generating all rules that have a single item as consequent
    candidates = [[x] for x in freq_itemset[0]]
    m = 1
    while candidates:
        m_consequents = []
        for itemset in candidates:
            # find by difference the antecedent set of the rule
            base = sorted(set(freq_itemset[0]).difference(set(itemset)))
            confidence = freq_itemset[1] / transactions.get_count(base)
            lift = confidence * transactions.num_transactions() / transactions.get_count(itemset)
            if confidence >= min_confidence:
                m_consequents.append(set(itemset))
                if base:    # rules whose antecedent is the empty set are discarded
                    yield base, itemset, confidence, lift
                    # tuple (antecedent, consequent, confidence, lift)
        candidates = apriori_gen(m_consequents, m+1)
        m += 1


def apriori(data_iter, min_support, min_confidence):
    """
    Finds frequent itemsets and association rules,
    yields a generator

    Arguments:
        data_iter -- iterable object ( e.g. [[4, 2], [2, 4, 3, 5]] )
        min_support -- minimum support (> 0)
        min_confidence -- minimum confidence
    """

    transactions = Transactions(data_iter)  # define an instance of type Transactions
    freq_itemsets = apriori_find_freq_sets(transactions, min_support)
    for itemset in freq_itemsets:
        yield (itemset[0], itemset[1] / transactions.num_transactions()), list(gen_rules(transactions, itemset, min_confidence))


# LOADING DATA

def load_data(input_file, delimiter):
    """
    Load data from input_file and yields a generator
    """
    with open(input_file) as file_iter:
        for line in file_iter:
            try:    # if items are represented by integers
                transaction = [int(x) for x in line.strip().split(delimiter)]
            except ValueError:  # if items are represented by strings
                transaction = [x for x in line.strip().split(delimiter)]
            yield transaction


def main():

    parser = argparse.ArgumentParser(
        description = "finds frequent itemsets and association rules by applying Improved Apriori algorithm to a transaction set provided")
    parser.add_argument(
        '-f', '--file', help = 'path of the file containing the transactions',
        required = True)
    parser.add_argument(
        '-d', '--delimiter',
        help = 'delimiter used in the file (default: space | for comma: ",")',
        default = " ")
    parser.add_argument(
        '-s', '--min-support',
        help = 'minimum support (must be > 0, default: 0.1)',
        type = float, default = 0.1)
    parser.add_argument(
        '-c', '--min-confidence',
        help = 'minimum confidence (default: 0.0)',
        type = float, default = 0.0)
    args = parser.parse_args()

    data_gen = load_data(args.file, args.delimiter)
    results = list(apriori(data_gen, args.min_support, args.min_confidence))
    for x in results:
        print("itemset: {} support: {:.3f}".format(*x[0]))
        for y in x[1]:
            print("rule: {} => {}".format(y[0],y[1]))
            print("confidence: {:.3f}".format(y[2]))
            print("lift: {:.3f}".format(y[3]))

if __name__ == '__main__':
    main()
