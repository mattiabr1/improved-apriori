# Improved Apriori
Implementation of Apriori algorithm and extension with PySpark.

## Description
Apriori [(Agrawal & Srikant, 1994)](https://www.vldb.org/conf/1994/P487.PDF) is the most popular algorithm for frequent itemset mining and association rule learning in relational databases containing transactions. It allows the discovery of hidden patterns in the data that can e.g. be exploited in order to define business strategies. The classical example is the analysis of supermarket baskets, where one can uncover interesting associations between items such as `{diapers} => {beer}`. See "*Mining of Massive Datasets*" [(Chapter 6)](http://infolab.stanford.edu/~ullman/mmds/ch6.pdf) by Leskovec et al. (2020) for further details.

From the implementation point of view, an improved version of the original Apriori algorithm is used, as described by the paper:

> Al-Maolegi, M., & Arkok, B. (2014). An Improved Apriori Algorithm For Association Rules.
*International Journal on Natural Language Computing, 3*(1) https://arxiv.org/pdf/1403.3948.pdf

## CLI Usage
Prepare the input dataset to be in the "market-basket" format, where each row is a transaction containing the associated items.

Run the program with CSV dataset and default values for minimum support (s=0.1) and minimum confidence (c=0)
```
python apriori.py -f filename.csv
```

Run the program specifying *min_support* and *min_confidence*
```
python apriori.py -f restructured.csv -d "," -s 0.01 -c 0.15
```

The same applies to the Apriori Spark implementation (that does not generate the association rules)

```
python apriori_spark.py -f restructured.csv -d "," -s 0.1
```

To install the Spark APIs for Python, I relied on the instructions in the following [link](http://www.dei.unipd.it/~capri/BDC/PythonInstructions.html). 
The latest version of Spark is not yet compatible with Python 3.8.

## Dataset
The methods can be applied to the [Instacart](https://www.instacart.com) data available at the link https://www.kaggle.com/competitions/instacart-market-basket-analysis/data. Of particular interest are the files `order_products__prior.csv` and `products.csv`: the former contains customer orders, the latter the names of the products to which the item IDs correspond.

The `data_preprocessing.py` script contains the procedure to reshape the dataset `order_products__prior.csv` in the "market-basket" format and save it to a new file `restructured.csv`.

Finally, the `instacart.py` script saves to a text file the frequent itemsets and the association rules uncovered by applying Apriori to Instacart data: item IDs are converted to the actual product names.
