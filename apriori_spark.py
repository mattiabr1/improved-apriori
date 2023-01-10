# Per eseguirlo come script:
#   python apriori_spark.py -f <path_file> -d <delimiter> -s min_support
#   python apriori_spark.py -f restructured.csv -d "," -s 0.1

# Per ulteriori dettagli: python apriori_spark.py -h


from pyspark import SparkContext, SparkConf
import apriori

import argparse


def try_convert(line, delimiter):
    """
    Splitta la riga del file assegnata in una lista di item
    """
    try:    # se gli item sono codificati tramite interi
        transaction = [int(x) for x in line.strip().split(delimiter)]
    except ValueError:  # se gli item sono codificati tramite stringhe
        transaction = [x for x in line.strip().split(delimiter)]
    return transaction

def spark_find_freq_sets(sc, input_file, delimiter, min_support):
    """
    Trova gli insiemi frequenti utilizzando l'algoritmo A-Priori Spark

    Argomenti:
        sc -- Spark context
        input_file -- percorso al file contenente le transazioni
        delimiter -- separatore utilizzato nel file
        min_support -- supporto minimo (> 0)
    """

    # legge il file e lo trasforma in un Resilient Distributed Dataset (RDD)
    data = sc.textFile(input_file)  # il numero di partizioni viene scelto in automatico
    num_baskets = data.count()  # numero di transazioni

    # trasforma ogni riga(transazione) del RDD in una lista di item, mantendendo
    # il partizionamento originale; viene restituito un nuovo RDD
    baskets = data.map(lambda line: try_convert(line, delimiter))

    # ciascun worker trova gli itemset frequenti nella partizione assegnata
    local_freq_sets = baskets.mapPartitions(lambda partition: [
    (frozenset(x[0]), x[1]) for x in apriori.apriori_find_freq_sets(
    apriori.Transactions(partition), min_support)
    ])

    # si filtrano gli itemset in base al loro conteggio globale
    final_freq_sets = local_freq_sets.reduceByKey(
    lambda k1,k2: k1+k2).filter(lambda x: x[1] >= min_support * num_baskets)

    # viene restituito un generatore
    yield final_freq_sets.collect()


def main():

    parser = argparse.ArgumentParser(
    description = "trova gli insiemi frequenti, applicando l'algoritmo A-Priori Spark a un insieme di transazioni dato")
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
    args = parser.parse_args()


    APP_NAME = "apriori_spark"
    conf = SparkConf().setAppName(APP_NAME)

    conf = conf.setMaster("local[*]")
    sc  = SparkContext(conf = conf)


    results = list(spark_find_freq_sets(args.file, args.delimiter, args.min_support, sc))
    for x in results:
        for y in x:
            print("itemset: {} count: {}".format(list(y[0]), y[1]))


if __name__ == '__main__':
    main()
