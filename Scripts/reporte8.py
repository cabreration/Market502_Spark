from pyspark import SparkContext
from pyspark.sql import SparkSession
import plotly.graph_objs as go

sc = SparkContext("local", "reporte 8")
spark = SparkSession.builder \
                    .master('local[*]') \
                    .appName('reporte 8') \
                    .getOrCreate()

sdfData = spark.read.csv("C:\Users\jacab\Documents\Seminario 2\Lab\Practica1\police_killings.csv", header=True, sep=",")
raw_killings = sdfData.rdd.map(tuple)
raw_killings = raw_killings.filter(lambda x: x[0] is not None and x[5] is not None)

filtered = raw_killings.map(lambda v: ("Year: " + str(v[5].split('/')[2]), 1))\
    .reduceByKey(lambda x, y: x + y).sortBy(lambda k: k[1], False).take(5)
print(filtered)

x_axis = []
y_axis = []

for element in filtered:
    x_axis.append(element[0])
    y_axis.append(element[1])

trace = go.Bar(
                x=x_axis,
                y=y_axis,
                name="Top anios",
                marker=dict(color='rgba(255, 174, 255, 0.5)',
                            line=dict(color='rgb(0,0,0)', width=1.5)),
                text="Anio vs cantidad de muertes")

data = [trace]
layout = go.Layout(barmode="group")
fig = go.Figure(data=data, layout=layout)

fig.show()
