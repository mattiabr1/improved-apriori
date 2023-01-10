# Per eseguirlo come script:
#   python apriori.py -f <path_file> -d <delimiter> -s min_support -c min_confidence
#   python apriori.py -f restructured.csv -d "," -s 0.1 -c 0

# Per ulteriori dettagli: python apriori.py -h


from itertools import combinations
import argparse


# STRUTTURE DATI

# Definisco una classe per un insieme di transazioni

class Transactions:

    # Inizializzazione

    def __init__(self, data_iter):
        """
        Richiede come argomento: data_iter - un oggetto di tipo iterable
        ( ad es. [["diapers", "beer"], ["beer", "diapers", "chips", "eggs"]] )

        Per definire un'istanza: transactions = Transactions(data_iter)
        """
        # variabili private
        self.__singletons = []      # lista degli item
        self.__TIDs_by_item = {}    # dizionario item - {id_transazioni}
        self.__transaction_id = 0   # numero di transazioni
        self.map_items(data_iter)

    def map_items(self, data_iter):
        """
        Per ogni transazione, mappa gli item in un dizionario. Questo, per ogni
        item(chiave) ha come valore l'insieme degli id delle transazioni
        che contengono quell'item

        Inoltre popola la lista dei singoli item e ottiene il numero delle transazioni
        """
        for transaction in data_iter:
            for item in transaction:
                if not item in self.__TIDs_by_item:
                    self.__singletons.append([item])
                    self.__TIDs_by_item[item] = set()
                self.__TIDs_by_item[item].add(self.__transaction_id)
            self.__transaction_id += 1

    # metodi associati alla classe

    def get_singletons(self):
        """ Ritorna la lista ordinata degli item """
        return sorted(self.__singletons)

    def get_count(self, itemset):
        """
        Calcola il numero di occorrenze di un itemset
        Richiede come argomento: itemset -- oggetto di tipo iterable
        ( ad es. ["diapers", "beer"] )
        """

        if not itemset:  # l'insieme vuoto è contenuto in tutte le transazioni
            return 1.0
        if self.__transaction_id == 0:  # se non vi sono transazioni, allora
            return 0.0                  # ogni itemset ha supporto nullo

        inter_TIDs = set()  # intersezione degli id delle transazioni
        for item in itemset:
            # ottiene l'insieme degli id delle transazioni che contengono l'item
            TIDs = self.__TIDs_by_item.get(item)

            if not TIDs:    # se un item non è contenuto in nessuna transazione,
                return 0.0  # allora l'itemset ha supporto nullo

            if not inter_TIDs:
                inter_TIDs = TIDs
            else:
                inter_TIDs = inter_TIDs.intersection(TIDs)

        return float(len(inter_TIDs))

    def num_transactions(self):
        return self.__transaction_id


# FUNZIONI AUSILIARIE

def apriori_gen(prev_freq_itemsets, length):
    """
    Ritorna la lista dei candidati generati secondo la logica dell'algoritmo A-Priori

    Argomenti:
        prev_freq_itemsets -- lista di set frequenti trovati all'ultimo passo
        length -- lunghezza (numero di item) dei candidati da generare
    """
    tmp_candidates = self_join(prev_freq_itemsets, length)
    return prune(tmp_candidates, prev_freq_itemsets, length)

def self_join(prev_freq_itemsets, length):
    """
    Ritorna una lista provvisoria dei candidati, ottenuta attraverso l'unione
    degli insiemi frequenti trovati all'ultimo passo

    Per come è strutturato l'algoritmo, è fondamentale che la lista di itemset
    sia ordinata e che gli item siano ordinati all'interno di ogni itemset
    """
    tmp_candidates = []
    for i in range(len(prev_freq_itemsets)):
        for j in range(i+1, len(prev_freq_itemsets)):
            # confronta i primi (length-2) item
            if sorted(prev_freq_itemsets[i])[:length-2] == sorted(prev_freq_itemsets[j])[:length-2]:
                tmp_candidates.append(sorted(prev_freq_itemsets[i].union(prev_freq_itemsets[j])))
    return tmp_candidates

def prune(tmp_candidates, prev_freq_itemsets, length):
    """
    Filtra i candidati
    Un candidato è tale solo se ogni suo sottoinsieme di cardinalità (length-1)
    appartiene agli insiemi frequenti trovati all'ultimo passo
    """
    next_candidates = [
    itemset for itemset in tmp_candidates
    if all(set(x) in prev_freq_itemsets for x in combinations(itemset, length-1))
    ]
    return next_candidates

# FUNZIONI PRINCIPALI

def apriori_find_freq_sets(transactions, min_support):
    """
    Applica l'algoritmo A-Priori Improved per trovare gli insiemi frequenti
    e ritorna un generatore

    Argomenti:
        transactions -- istanza di tipo Transactions
        min_support -- supporto minimo (> 0)
    """
    if min_support <= 0:
        raise ValueError('il supporto minimo deve essere strettamente > 0')

    candidates = transactions.get_singletons()
    k = 1
    while candidates:
        k_freq_itemsets = []
        for itemset in candidates:
            count = transactions.get_count(itemset)
            if count >= min_support * transactions.num_transactions():
                k_freq_itemsets.append(set(itemset))
                yield itemset, count  # tupla (insieme frequente, conteggio)
        candidates = apriori_gen(k_freq_itemsets, k+1)
        k += 1

def gen_rules(transactions, freq_itemset, min_confidence):
    """
    Genera le regole di associazione a partire da un insieme frequente
    e ritorna un generatore

    Argomenti:
        transactions -- istanza di tipo Transactions
        freq_itemset -- tupla della forma (insieme frequente, conteggio)
        min_confidence -- confidenza minima

    L'implementazione è sulla falsa riga della funzione apriori_find_freq_sets
    """

    # parto generando tutte le regole che hanno come conseguente un singolo item
    candidates = [[x] for x in freq_itemset[0]]
    m = 1
    while candidates:
        m_consequents = []
        for itemset in candidates:
            # trovo per differenza l'insieme antecedente della regola
            base = sorted(set(freq_itemset[0]).difference(set(itemset)))
            confidence = freq_itemset[1] / transactions.get_count(base)
            lift = confidence * transactions.num_transactions() / transactions.get_count(itemset)
            if confidence >= min_confidence:
                m_consequents.append(set(itemset))
                if base:    # vengono scartate le regole il cui antecedente è l'insieme vuoto
                    # tupla (antecedente, conseguente, confidenza, lift)
                    yield base, itemset, confidence, lift
        candidates = apriori_gen(m_consequents, m+1)
        m += 1


def apriori(data_iter, min_support, min_confidence):
    """
    Trova gli insiemi frequenti e le regole di associazione,
    ritorna un generatore

    Argomenti:
        data_iter -- un oggetto di tipo iterable ( ad es. [[4, 2], [2, 4, 3, 5]] )
        min_support -- supporto minimo (> 0)
        min_confidence -- confidenza minima
    """

    transactions = Transactions(data_iter)  # definisce un'istanza di tipo Transactions
    freq_itemsets = apriori_find_freq_sets(transactions, min_support)
    for itemset in freq_itemsets:
        yield (itemset[0], itemset[1] / transactions.num_transactions()), list(gen_rules(transactions, itemset, min_confidence))


# CARICAMENTO DATI

def load_data(input_file, delimiter):
    """
    Carica i dati da input_file e ritorna un generatore
    """
    with open(input_file) as file_iter:
        for line in file_iter:
            try:    # se gli item sono codificati tramite interi
                transaction = [int(x) for x in line.strip().split(delimiter)]
            except ValueError:  # se gli item sono codificati tramite stringhe
                transaction = [x for x in line.strip().split(delimiter)]
            yield transaction


def main():

    parser = argparse.ArgumentParser(
    description = "trova gli insiemi frequenti e le regole di associazione, applicando l'algoritmo A-Priori Improved a un dato insieme di transazioni")
    parser.add_argument(
    '-f', '--file', help = 'percorso al file contenente le transazioni',
    required = True)
    parser.add_argument(
    '-d', '--delimiter',
    help = 'separatore utilizzato nel file (default: spazio | per la virgola: ",")',
    default = " ")
    parser.add_argument(
    '-s', '--min-support',
    help = 'supporto minimo (deve essere > 0, default: 0.1)',
    type = float, default = 0.1)
    parser.add_argument(
    '-c', '--min-confidence',
    help = 'confidenza minima (default: 0.0)',
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
