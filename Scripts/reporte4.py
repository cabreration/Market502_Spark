from pyspark import SparkContext
from pyspark.sql import SparkSession
import plotly.graph_objs as go

sc = SparkContext("local", "reporte 4")
spark = SparkSession.builder \
                    .master('local[*]') \
                    .appName('reporte 4') \
                    .getOrCreate()

sdfData = spark.read.csv("C:\Users\jacab\Documents\Seminario 2\Lab\Practica1\sales.csv", header=True, sep=",")
raw_sales = sdfData.rdd.map(tuple)

filtered = raw_sales.map(lambda val: (val[0], float(val[11]))).reduceByKey(lambda x, y: x + y)

labels = []
quantities = []

for element in filtered.collect():
    labels.append(element[0])
    quantities.append(element[1])

trace = go.Pie(labels=labels, values=quantities)
data = [trace]

fig = go.Figure(data=data)

fig.show()
