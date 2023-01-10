# Salva in un file gli insiemi frequenti e le regole associative ottenuti
# attraverso l'applicazione dell'algoritmo A-Priori Improved al dataset instacart
# Gli id degli item vengono convertiti negli effettivi nomi dei prodotti


import csv
import apriori


input_file = 'restructured.csv' # percorso al file contenente le transazioni
delimiter = ","   # separatore utilizzato nel file contenente le transazioni
output_file = 'instacart.txt'   # percorso al file dove salvare i risultati

file_name_items = 'products.csv' # percorso al file contenente i nomi dei
                                # prodotti a cui corrispondono gli id degli item
PRODUCT = 'product_name'


min_support = 0.01
min_confidence = 0.15


def find_name_items(file, column, id_item):
    """
    Ritorna il nome del prodotto che corrisponde ad un determinato id_item

    Richiede in ingresso il file e la colonna in cui si trovano i nomi dei prodotti
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
