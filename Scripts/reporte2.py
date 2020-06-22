from pyspark import SparkContext
from pyspark.sql import SparkSession
import plotly.graph_objs as go

sc = SparkContext("local", "reporte 2")
spark = SparkSession.builder \
                    .master('local[*]') \
                    .appName('reporte 2') \
                    .getOrCreate()

sdfData = spark.read.csv("C:\Users\jacab\Documents\Seminario 2\Lab\Practica1\\video_games_sales.csv", header=True, sep=",")
raw_vgsales = sdfData.rdd.map(tuple)

filtered = raw_vgsales.map(lambda v: (v[4], v[5])).filter(lambda e: e[1] == 'Nintendo')

result = filtered.countByKey()

labels = []
quantities = []

for key in result:
    labels.append(key)
    quantities.append(result[key])

trace = go.Pie(labels=labels, values=quantities)
data = [trace]

fig = go.Figure(data=data)

fig.show()


