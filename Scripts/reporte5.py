from pyspark import SparkContext
from pyspark.sql import SparkSession
import plotly.graph_objs as go

sc = SparkContext("local", "reporte 5")
spark = SparkSession.builder \
                    .master('local[*]') \
                    .appName('reporte 5') \
                    .getOrCreate()

sdfData = spark.read.csv("C:\Users\jacab\Documents\Seminario 2\Lab\Practica1\sales.csv", header=True, sep=",")
raw_sales = sdfData.rdd.map(tuple)

filtered = raw_sales.filter(lambda element: element[1] == 'Guatemala').map(lambda v: (v[5].split('/')[2], int(v[8])))
totals = filtered.reduceByKey(lambda x, y: x + y)
desc = totals.sortBy(lambda x: x[1], False)

x_axis = [desc.take(1)[0][0]]
y_axis = [desc.take(1)[0][1]]

trace = go.Bar(
                x=x_axis,
                y=y_axis,
                name="Ventas por Anio",
                marker=dict(color='rgba(255, 174, 255, 0.5)',
                            line=dict(color='rgb(0,0,0)', width=1.5)),
                text="Unidades Vendidas vs Anio en Guatemala")

data = [trace]
layout = go.Layout(barmode="group")
fig = go.Figure(data=data, layout=layout)

fig.show()
