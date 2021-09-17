from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DateType
from pyspark import SparkContext
from urllib.request import urlopen
import json

#Iniciando la sesion de spark
spark = SparkSession.builder.appName('favoTest').getOrCreate()

#Leyendo Json de sellers y pasandolo a un df de spark
sellers_url = "https://favo-data-test.s3.amazonaws.com/TestFolder/sellers.json"
sellers_data = urlopen(sellers_url).read().decode('utf-8').replace("\r\n","|").split("|")
sellers_rdd = spark.sparkContext.parallelize(sellers_data)
df_sellers = spark.read.json(sellers_rdd)
df_sellers = df_sellers.withColumn("start",F.from_unixtime('start').cast(DateType()))
df_sellers.show()

#Leyendo Json de buyers  y pasandolo a un df de spark
buyers_url = "https://favo-data-test.s3.amazonaws.com/TestFolder/buyers.json"
buyers_data = urlopen(buyers_url).read().decode('utf-8').replace("\r\n","|").replace(".","").split("|")
buyers_rdd = spark.sparkContext.parallelize(buyers_data)
df_buyers = spark.read.json(buyers_rdd)
df_buyers = df_buyers.withColumn("start",F.from_unixtime('start').cast(DateType()))
df_buyers.show()

#Leyendo json de products  y pasandolo a un df de spark
products_url = "https://favo-data-test.s3.amazonaws.com/TestFolder/products.json"
products_data = urlopen(products_url).read().decode('utf-8').replace("\r\n","|").split("|")
products_rdd = spark.sparkContext.parallelize(products_data)
df_products = spark.read.json(products_rdd)
df_products.show()

#Leyendo json de orders  y pasandolo a un df de spark
orders_url = "https://favo-data-test.s3.amazonaws.com/TestFolder/orders.json"
orders_data = urlopen(orders_url).read().decode('utf-8').replace("\r\n","|").split("|")
orders_rdd = spark.sparkContext.parallelize(orders_data)
df_orders = spark.read.json(orders_rdd)
aux = df_orders.select(df_orders.order_id, df_orders.buyer_id, df_orders.seller_id, df_orders.date, F.explode(df_orders.product).alias("product"))
aux = aux.withColumn("sku", F.col("product").getItem("sku")).withColumn("qt", F.col("product").getItem("qt")).withColumn("date",F.from_unixtime('date').cast(DateType()))
df_orders = aux.select(aux.order_id, aux.buyer_id, aux.seller_id, aux.date, aux.sku, aux.qt)
df_orders.show()

#1. Archivo enriquecido con todo el detalle de buyers, sellers, products y orders
df_orders.createOrReplaceTempView("orders")
df_sellers.createOrReplaceTempView("sellers")
df_buyers.createOrReplaceTempView("buyers")
df_products.createOrReplaceTempView("products")
df_final = spark.sql("select o.order_id, o.buyer_id, b.buyer, b.phone as buyer_phone, b.start as buyer_start, s.seller_id, s.seller, s.phone as seller_phone, s.start as seller_start, p.id as product_id, p.name as product_name, o.qt as quantity, p.price as product_price, round(o.qt*p.price,2) as amount, o.date as order_date from orders o left join buyers b on o.buyer_id = b.buyer_id left join sellers s on o.seller_id=s.seller_id left join products p on o.sku=p.sku")
df_final.show()

#2. Diferencia de ventas por semana
df_final = df_final.withColumn('order_week', F.weekofyear(F.col('order_date'))).withColumn('order_date_add', F.add_months(F.col('order_date'),1))
df_final = df_final.withColumn('order_week', F.concat(F.year(F.col('order_date')),F.lit('-W'),F.lpad(F.col('order_week'),2,'0')))
df_final.createOrReplaceTempView("orders_detail")
df_difference_week = spark.sql("select o1.buyer_id, o1.buyer, o1.buyer_phone, o1.buyer_start, o1.order_week, round(sum(o1.amount)) - ifnull(round(sum(o2.amount)),0) as difference_week  from orders_detail o1 left join orders_detail o2 on o1.buyer_id=o2.buyer_id and MONTH(o1.order_date)=MONTH(o2.order_date_add) group by o1.buyer_id, o1.buyer, o1.buyer_phone, o1.buyer_start, o1.order_week order by o1.order_week desc")
df_difference_week.show()

#3. Porcentaje de contribución
totalventas = df_final.agg(F.sum("amount")).collect()[0][0]
df_contribution = df_final.groupBy('seller_id','seller','seller_phone','seller_start').agg(F.sum(F.col("amount")).alias("ventas"))
df_contribution = df_contribution.withColumn("percent",F.col("ventas")/totalventas*100)
df_contribution.show()

#4. Posibles buyers para buyers con valor null
#la lógica utilizada es hacer un cruce del df final con él mismo por medio del campo order_week 
#teniendo en cuenta solo los valores nulos de seller en el df de la izquierda y los valores no nulos del df de la derecha
#la idea de hacer el cruce por el campo order_week es que posiblemente las personas que hayan realizado pedidos en la misma semana hayan realizado diferentes pedidos
#entonces se van a asociar a los buyers con valor null todos los buyers que hayan realizado compras en la misma semana, estas serán los posibles personas que hayan realizado alguna orden de compra
df_pos_buyers = spark.sql("select distinct(o1.order_id), o1.product_name, o1.buyer, o2.buyer as pos_buyer, o1.order_week, o1.order_date from orders_detail o1 left join orders_detail o2 on o1.order_week=o2.order_week where o1.buyer is null and o2.buyer is not null")
df_pos_buyers.show()
