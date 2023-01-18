"""
The data contained in 'order_products__prior.csv' are reshaped
from long to wide format and saved in the new file 'restructured.csv':
only the variable 'product_id' is considered for the aims of the research

Afterwards, the Apriori algorithm will be applied to the restructured dataframe
"""

import csv

FI = 'order_products__prior.csv'  # path of the original file
FO = 'restructured.csv'     # path where to save the new file
ORDER = 'order_id'
PRODUCT = 'product_id'


with open(FI, 'r') as filein, open(FO, 'w') as fileout:
    """
    Create a dictionary where each order(key) is associated with the list of
    product IDs that belong to that order
    """
    products_by_order = {}
    for row in csv.DictReader(filein, skipinitialspace=True):
        if not row[ORDER] in products_by_order:
            products_by_order[row[ORDER]] = []
        products_by_order[row[ORDER]].append(row[PRODUCT])
        print(row[ORDER])
    # write transactions line by line to the output csv file
    for key in products_by_order.keys():
        writer = csv.writer(fileout)
        writer.writerow(products_by_order[key])
        print(key)
