from pyspark import SparkContext
from pyspark.sql import SparkSession
import plotly.graph_objs as go

sc = SparkContext("local", "reporte 6")
spark = SparkSession.builder \
                    .master('local[*]') \
                    .appName('reporte 6') \
                    .getOrCreate()

sdfData = spark.read.csv("C:\Users\jacab\Documents\Seminario 2\Lab\Practica1\sales.csv", header=True, sep=",")
raw_sales = sdfData.rdd.map(tuple)

filtered = raw_sales.filter(lambda v: v[3] == 'Online' and v[5].split('/')[2] == '2010')

revenue = filtered.map(lambda v: (1, float(v[11]))).reduceByKey(lambda x, y: x + y)
cost = filtered.map(lambda v: (2, float(v[12]))).reduceByKey(lambda x, y: x + y)
profit = filtered.map(lambda v: (3, float(v[13]))).reduceByKey(lambda x, y: x + y)

print(revenue.collect())
print(cost.collect())
print(profit.collect())

x_axis1 = ['Ingresos']
y_axis1 = [revenue.collect()[0][1]]

trace1 = go.Bar(
                x=x_axis1,
                y=y_axis1,
                name="ingresos",
                marker=dict(color='rgba(255, 174, 255, 0.5)',
                            line=dict(color='rgb(0,0,0)', width=1.5)),
                text="ingresos")

x_axis2 = ['Costos']
y_axis2 = [cost.collect()[0][1]]

trace2 = go.Bar(
                x=x_axis2,
                y=y_axis2,
                name="costos",
                marker=dict(color='rgba(255, 255, 128, 0.5)',
                            line=dict(color='rgb(0,0,0)', width=1.5)),
                text="costos")

x_axis3 = ['Ganancias']
y_axis3 = [profit.collect()[0][1]]

trace3 = go.Bar(
                x=x_axis3,
                y=y_axis3,
                name="ganancias",
                marker=dict(color='rgba(128, 255, 128, 0.5)',
                            line=dict(color='rgb(0,0,0)', width=1.5)),
                text="ganancias")

data = [trace1, trace2, trace3]
layout = go.Layout(barmode="group")
fig = go.Figure(data=data, layout=layout)

fig.show()
