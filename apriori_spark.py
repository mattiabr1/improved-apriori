"""
To run it as a script:
    python apriori_spark.py -f <path_file> -d <delimiter> -s min_support
    python apriori_spark.py -f restructured.csv -d "," -s 0.1

For further details: python apriori_spark.py -h
"""


from pyspark import SparkContext, SparkConf
import apriori

import argparse


def try_convert(line, delimiter):
    """
    Splits the assigned file row into a list of items
    """
    try:    # if items are represented by integers
        transaction = [int(x) for x in line.strip().split(delimiter)]
    except ValueError:  # # if items are represented by strings
        transaction = [x for x in line.strip().split(delimiter)]
    return transaction

def spark_find_freq_sets(sc, input_file, delimiter, min_support):
    """
    Finds frequent itemsets by using Apriori Spark algorithm

    Arguments:
        sc -- Spark context
        input_file -- path of the file containing the transactions
        delimiter -- delimiter used in the file
        min_support -- minimum support (> 0)
    """

    # read the file and transform it into a Resilient Distributed Dataset (RDD)
    data = sc.textFile(input_file)  # the number of partitions is chosen automatically
    num_baskets = data.count()  # number of transactions

    # transform each row(transaction) of the RDD into a list of items, maintaining
    # the original partitioning: a new RDD is returned
    baskets = data.map(lambda line: try_convert(line, delimiter))

    # each worker finds frequent itemsets in the assigned partition
    local_freq_sets = baskets.mapPartitions(lambda partition: [
        (frozenset(x[0]), x[1]) for x in apriori.apriori_find_freq_sets(
        apriori.Transactions(partition), min_support)
    ])

    # filter itemsets by their global counts
    final_freq_sets = local_freq_sets.reduceByKey(
        lambda k1,k2: k1+k2).filter(lambda x: x[1] >= min_support * num_baskets)

    # yield a generator
    yield final_freq_sets.collect()


def main():

    parser = argparse.ArgumentParser(
        description = "finds frequent itemsets by applying Apriori Spark algorithm to a transaction set provided")
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
