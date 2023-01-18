"""
Save to a text file the frequent itemsets and association rules uncovered
by applying Apriori to Instacart data
Item IDs are converted to the actual product names
"""

import csv
import apriori


input_file = 'restructured.csv' # path of the file containing the transactions
delimiter = ","   # delimiter used in the file containing the transactions
output_file = 'instacart.txt'   # path of the file where to save the results

file_name_items = 'products.csv' # path of the file containing the product names
                                 # that correspond to the item IDs
PRODUCT = 'product_name'


min_support = 0.01
min_confidence = 0.15


def find_name_items(file, column, id_item):
    """
    Returns the product name that corresponds to a given id_item

    Requires as input the file and the column in which the product names are located
    """
    with open(file, 'r') as fn:
        reader = csv.DictReader(fn)
        return [row[column] for idx, row in enumerate(reader,1) if idx in map(int, id_item)]


with open(output_file, 'w') as fo:

    data_gen = apriori.load_data(input_file, delimiter)
    results = list(apriori.apriori(data_gen, min_support, min_confidence))

    for x in results:

        fo.write("{} support: {:.3f}\n".format(find_name_items(file_name_items, PRODUCT, x[0][0]), x[0][1]))
        print("{} support: {:.3f}\n".format(find_name_items(file_name_items, PRODUCT, x[0][0]), x[0][1]))

        for y in x[1]:

            fo.write("rule: {} => {}\n".format(find_name_items(file_name_items, PRODUCT, y[0]),
            find_name_items(file_name_items, PRODUCT, y[1])))
            print("rule: {} => {}".format(find_name_items(file_name_items, PRODUCT, y[0]),
            find_name_items(file_name_items, PRODUCT, y[1])))

            fo.write("confidence: {:.3f}\n".format(y[2]))
            print("confidence: {:.3f}".format(y[2]))

            fo.write("lift: {:.3f}\n".format(y[3]))
            print("lift: {:.3f}".format(y[3]))
