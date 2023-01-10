"""
I dati contenuti in 'order_products__prior.csv' vengono rimodellati
dal formato lungo al formato largo e salvati nel nuovo file 'restructured.csv',
per gli obiettivi della ricerca si considera solo la variabile 'product_id'.
In seguito, l'algoritmo A-Priori verrà applicato al dataframe ristrutturato.
"""

import csv

FI = 'order_products__prior.csv'  # percorso al file originale
FO = 'restructured.csv'     # percorso dove salvare il nuovo file
ORDER = 'order_id'
PRODUCT = 'product_id'


with open(FI, 'r') as filein, open(FO, 'w') as fileout:
    """
    Creo un dizionario che per ogni ordine(chiave) ha come valore
    la lista di tutti gli id dei prodotti che appartengono a quell'ordine
    """
    products_by_order = {}
    for row in csv.DictReader(filein, skipinitialspace=True):
        if not row[ORDER] in products_by_order:
            products_by_order[row[ORDER]] = []
        products_by_order[row[ORDER]].append(row[PRODUCT])
        print(row[ORDER])
    # scrivo riga per riga le transazioni nel file csv di output
    for key in products_by_order.keys():
        writer = csv.writer(fileout)
        writer.writerow(products_by_order[key])
        print(key)
