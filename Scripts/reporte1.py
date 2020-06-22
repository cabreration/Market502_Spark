from pyspark import SparkContext
from pyspark.sql import SparkSession
import plotly.graph_objs as go

sc = SparkContext("local", "reporte 1")
spark = SparkSession.builder \
                    .master('local[*]') \
                    .appName('reporte 1') \
                    .getOrCreate()

sdfData = spark.read.csv("C:\Users\jacab\Documents\Seminario 2\Lab\Practica1\\video_games_sales.csv", header=True, sep=",")
raw_vgsales = sdfData.rdd.map(tuple)

filtered = raw_vgsales.map(lambda v: (v[4], float(v[10])))
reduced = filtered.reduceByKey(lambda x, y: x + y)

x_axis = []
y_axis = []

for element in reduced.collect():
    if element[0] == 'Action' or element[0] == 'Sports' or element[0] == 'Fighting' or element[0] == 'Shooter' or \
            element[0] == 'Racing' or element[0] == 'Adventure' or element[0] == 'Strategy':
        x_axis.append(element[0])
        y_axis.append(element[1])

trace = go.Bar(
                x=x_axis,
                y=y_axis,
                name="categoria",
                marker=dict(color='rgba(255, 174, 255, 0.5)',
                            line=dict(color='rgb(0,0,0)', width=1.5)),
                text="categoria vs ingresos globales")

data = [trace]
layout = go.Layout(barmode="group")
fig = go.Figure(data=data, layout=layout)

fig.show()
