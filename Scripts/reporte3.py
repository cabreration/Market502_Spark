from pyspark import SparkContext
from pyspark.sql import SparkSession
import plotly.graph_objs as go
import csv

sc = SparkContext("local", "reporte 3")
spark = SparkSession.builder \
                    .master('local[*]') \
                    .appName('reporte 3') \
                    .getOrCreate()

sdfData = spark.read.csv("C:\Users\jacab\Documents\Seminario 2\Lab\Practica1\\video_games_sales.csv", header=True, sep=",")
raw_vgsales = sdfData.rdd.map(tuple)

filtered = raw_vgsales.map(lambda v: (v[2], v[1])).distinct().map(lambda n: (n[0], 1))

counted = filtered.reduceByKey(lambda x, y: x + y).sortBy(lambda el: el[1], False).take(5)

x_axis = []
y_axis = []

for element in counted:
    x_axis.append(element[0])
    y_axis.append(element[1])

trace = go.Bar(
                x=x_axis,
                y=y_axis,
                name="plataforma",
                marker=dict(color='rgba(255, 174, 255, 0.5)',
                            line=dict(color='rgb(0,0,0)', width=1.5)),
                text="plataforma vs lanzamientos")

data = [trace]
layout = go.Layout(barmode="group")
fig = go.Figure(data=data, layout=layout)

fig.show()
