# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Silver — Limpeza, Padronização e Enriquecimento
# MAGIC
# MAGIC **Objetivo:** a partir das tabelas Bronze (dado bruto, tipado como string/inferido), aplicar:
# MAGIC 1. Remoção de duplicatas por chave primária de negócio;
# MAGIC 2. Tratamento de nulos em campos críticos;
# MAGIC 3. Conversão de tipos (strings → `timestamp`, `int`, `decimal`);
# MAGIC 4. Enriquecimento básico (colunas derivadas de tempo de entrega e atraso);
# MAGIC 5. Persistência em Delta na camada Silver, com regras simples de qualidade.

# COMMAND ----------

dbutils.widgets.text("catalog", "olist_lakehouse", "Catálogo Unity Catalog")
catalog = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `silver.customers`
# MAGIC Chave primária de negócio: `customer_id`. Padroniza texto (trim/upper em UF) e remove duplicatas exatas.

# COMMAND ----------

bronze_customers = spark.table(f"{catalog}.bronze.customers_raw")

silver_customers = (
    bronze_customers
    .dropDuplicates(["customer_id"])
    .filter(F.col("customer_id").isNotNull())
    .select(
        F.col("customer_id"),
        F.col("customer_unique_id"),
        F.col("customer_zip_code_prefix").cast("int").alias("customer_zip_code_prefix"),
        F.trim(F.col("customer_city")).alias("customer_city"),
        F.upper(F.trim(F.col("customer_state"))).alias("customer_state"),
    )
)

(
    silver_customers.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.silver.customers")
)
print(f"silver.customers: {spark.table(f'{catalog}.silver.customers').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `silver.products`
# MAGIC Chave primária: `product_id`. Nulos em `product_category_name` viram `'nao_informado'`
# MAGIC (regra de negócio: preservar a linha do produto mesmo sem categoria, evitando perda de fato de venda).

# COMMAND ----------

bronze_products = spark.table(f"{catalog}.bronze.products_raw")

silver_products = (
    bronze_products
    .dropDuplicates(["product_id"])
    .filter(F.col("product_id").isNotNull())
    .withColumn(
        "product_category_name",
        F.coalesce(F.col("product_category_name"), F.lit("nao_informado")),
    )
    .select(
        F.col("product_id"),
        F.col("product_category_name"),
        F.col("product_weight_g").cast("double").alias("product_weight_g"),
        F.col("product_length_cm").cast("double").alias("product_length_cm"),
        F.col("product_height_cm").cast("double").alias("product_height_cm"),
        F.col("product_width_cm").cast("double").alias("product_width_cm"),
    )
    # volume cúbico do produto, usado na dim_produto da camada Gold
    .withColumn(
        "product_volume_cm3",
        F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm"),
    )
)

(
    silver_products.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.silver.products")
)
print(f"silver.products: {spark.table(f'{catalog}.silver.products').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `silver.orders`
# MAGIC Chave primária: `order_id`. Converte as 5 colunas de data/hora (string) para `timestamp` e
# MAGIC deriva `tempo_entrega_dias` e `atraso_entrega_dias` — pilares das perguntas de negócio 1 e 2.
# MAGIC Regra de qualidade: descarta linhas sem `order_id` ou `customer_id` (não são "pedidos" válidos).

# COMMAND ----------

bronze_orders = spark.table(f"{catalog}.bronze.orders_raw")

silver_orders = (
    bronze_orders
    .dropDuplicates(["order_id"])
    .filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())
    .select(
        "order_id",
        "customer_id",
        F.lower(F.trim(F.col("order_status"))).alias("order_status"),
        F.to_timestamp("order_purchase_timestamp").alias("order_purchase_timestamp"),
        F.to_timestamp("order_approved_at").alias("order_approved_at"),
        F.to_timestamp("order_delivered_carrier_date").alias("order_delivered_carrier_date"),
        F.to_timestamp("order_delivered_customer_date").alias("order_delivered_customer_date"),
        F.to_timestamp("order_estimated_delivery_date").alias("order_estimated_delivery_date"),
    )
    .withColumn(
        "tempo_entrega_dias",
        F.datediff("order_delivered_customer_date", "order_purchase_timestamp"),
    )
    .withColumn(
        # positivo = atrasado em relação à estimativa; negativo/zero = dentro do prazo
        "atraso_entrega_dias",
        F.datediff("order_delivered_customer_date", "order_estimated_delivery_date"),
    )
    .withColumn(
        "entregue_no_prazo",
        F.when(F.col("order_delivered_customer_date").isNull(), F.lit(None).cast("boolean"))
         .otherwise(F.col("order_delivered_customer_date") <= F.col("order_estimated_delivery_date")),
    )
)

(
    silver_orders.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.silver.orders")
)
print(f"silver.orders: {spark.table(f'{catalog}.silver.orders').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `silver.order_items`
# MAGIC Grão: item do pedido (`order_id` + `order_item_id`). Converte `price`/`freight_value` para `decimal(10,2)`
# MAGIC e descarta itens com preço nulo/negativo (regra de qualidade: item sem valor não é um item de venda válido).

# COMMAND ----------

bronze_items = spark.table(f"{catalog}.bronze.order_items_raw")

silver_order_items = (
    bronze_items
    .dropDuplicates(["order_id", "order_item_id"])
    .filter(F.col("order_id").isNotNull() & F.col("product_id").isNotNull())
    .withColumn("price", F.col("price").cast(DecimalType(10, 2)))
    .withColumn("freight_value", F.col("freight_value").cast(DecimalType(10, 2)))
    .filter((F.col("price").isNotNull()) & (F.col("price") >= 0))
    .select(
        "order_id",
        F.col("order_item_id").cast("int").alias("order_item_id"),
        "product_id",
        "seller_id",
        F.to_timestamp("shipping_limit_date").alias("shipping_limit_date"),
        "price",
        "freight_value",
    )
)

(
    silver_order_items.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.silver.order_items")
)
print(f"silver.order_items: {spark.table(f'{catalog}.silver.order_items').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `silver.order_reviews`
# MAGIC Chave primária: `review_id`. Um pedido pode ter mais de uma avaliação ao longo do tempo;
# MAGIC mantemos a **mais recente** por `order_id` (`review_answer_timestamp` mais alto) para uso 1:1 no fato.

# COMMAND ----------

from pyspark.sql.window import Window

bronze_reviews = spark.table(f"{catalog}.bronze.order_reviews_raw")

reviews_typed = (
    bronze_reviews
    .filter(F.col("order_id").isNotNull() & F.col("review_score").isNotNull())
    .withColumn("review_score", F.col("review_score").cast("int"))
    .filter(F.col("review_score").between(1, 5))
    .withColumn("review_creation_date", F.to_timestamp("review_creation_date"))
    .withColumn("review_answer_timestamp", F.to_timestamp("review_answer_timestamp"))
)

w = Window.partitionBy("order_id").orderBy(F.col("review_answer_timestamp").desc_nulls_last())

silver_order_reviews = (
    reviews_typed
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .select(
        "review_id",
        "order_id",
        "review_score",
        "review_creation_date",
        "review_answer_timestamp",
    )
)

(
    silver_order_reviews.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.silver.order_reviews")
)
print(f"silver.order_reviews: {spark.table(f'{catalog}.silver.order_reviews').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `silver.order_payments`
# MAGIC Grão original: parcela de pagamento (`order_id` + `payment_sequential`) — um pedido pode ter
# MAGIC múltiplas formas de pagamento. Mantemos o grão de parcela aqui; a agregação por pedido
# MAGIC (`valor_total_pago`, forma predominante) acontece na `dim_pagamento` da camada Gold.

# COMMAND ----------

bronze_payments = spark.table(f"{catalog}.bronze.order_payments_raw")

silver_order_payments = (
    bronze_payments
    .dropDuplicates(["order_id", "payment_sequential"])
    .filter(F.col("order_id").isNotNull())
    .withColumn("payment_sequential", F.col("payment_sequential").cast("int"))
    .withColumn("payment_installments", F.col("payment_installments").cast("int"))
    .withColumn("payment_value", F.col("payment_value").cast(DecimalType(10, 2)))
    .withColumn("payment_type", F.lower(F.trim(F.col("payment_type"))))
    .select(
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
    )
)

(
    silver_order_payments.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.silver.order_payments")
)
print(f"silver.order_payments: {spark.table(f'{catalog}.silver.order_payments').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Relatório de qualidade — linhas Bronze x Silver (evidência para a seção 5 do README)

# COMMAND ----------

pairs = [
    ("customers_raw", "customers"),
    ("products_raw", "products"),
    ("orders_raw", "orders"),
    ("order_items_raw", "order_items"),
    ("order_reviews_raw", "order_reviews"),
    ("order_payments_raw", "order_payments"),
]

rows = []
for bronze_tbl, silver_tbl in pairs:
    b_cnt = spark.table(f"{catalog}.bronze.{bronze_tbl}").count()
    s_cnt = spark.table(f"{catalog}.silver.{silver_tbl}").count()
    rows.append((bronze_tbl, b_cnt, silver_tbl, s_cnt, b_cnt - s_cnt))

quality_df = spark.createDataFrame(
    rows, ["tabela_bronze", "linhas_bronze", "tabela_silver", "linhas_silver", "linhas_removidas"]
)
display(quality_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Checagem de nulos em campos-chave (silver.orders)

# COMMAND ----------

display(
    spark.table(f"{catalog}.silver.orders").select(
        F.count(F.when(F.col("order_purchase_timestamp").isNull(), 1)).alias("null_purchase_ts"),
        F.count(F.when(F.col("order_delivered_customer_date").isNull(), 1)).alias("null_delivered_date"),
        F.count(F.when(F.col("order_estimated_delivery_date").isNull(), 1)).alias("null_estimated_date"),
        F.count("*").alias("total_linhas"),
    )
)
