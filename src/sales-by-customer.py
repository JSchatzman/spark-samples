"""Spark implementation of sales by customer."""

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SalesByCustomer")
sc = SparkContext(conf=conf)


def parseline(line):
    """Parse each line of the csv file."""
    fields = line.split(',')
    custid = int(fields[0])
    order_amount = float(fields[2])
    return (custid, order_amount)

lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
parsedLines = lines.map(parseline)
customersales = parsedLines.map(lambda x: (x[0], x[1]))
totalsales = customersales.reduceByKey(lambda total, new: total + new)
totalsalesSorted = totalsales.map(lambda (x, y): (y, x)).sortByKey()
results = totalsalesSorted.collect()

for result in results:
    print(str(result[1]) + '---' + "{:.2f}".format(result[0]))
