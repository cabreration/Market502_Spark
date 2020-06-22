from pyspark import SparkContext
from pyspark.sql import SparkSession
import plotly.graph_objs as go

sc = SparkContext("local", "reporte 7")
spark = SparkSession.builder \
                    .master('local[*]') \
                    .appName('reporte 7') \
                    .getOrCreate()

sdfData = spark.read.csv("C:\Users\jacab\Documents\Seminario 2\Lab\Practica1\police_killings.csv", header=True, sep=",")
raw_killings = sdfData.rdd.map(tuple)

filtered = raw_killings.map(lambda v: (v[3], 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda k: k[1], False).take(3)

x_axis = []
y_axis = []

for element in filtered:
    x_axis.append(element[0])
    y_axis.append(element[1])

trace = go.Bar(
                x=x_axis,
                y=y_axis,
                name="top razas",
                marker=dict(color='rgba(255, 174, 255, 0.5)',
                            line=dict(color='rgb(0,0,0)', width=1.5)),
                text="razas vs cantidad de muertes")

data = [trace]
layout = go.Layout(barmode="group")
fig = go.Figure(data=data, layout=layout)

fig.show()

