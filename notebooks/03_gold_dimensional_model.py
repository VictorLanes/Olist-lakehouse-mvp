# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Gold — Modelo Dimensional (Star Schema)
# MAGIC
# MAGIC **Objetivo:** consolidar as tabelas Silver em um modelo dimensional (esquema estrela) otimizado
# MAGIC para consultas analíticas, respondendo diretamente às 3 perguntas de negócio do MVP.
# MAGIC
# MAGIC ```
# MAGIC                         ┌──────────────────┐
# MAGIC                         │   dim_tempo       │
# MAGIC                         │ (data pedido)     │
# MAGIC                         └────────┬──────────┘
# MAGIC                                  │
# MAGIC ┌──────────────┐        ┌────────▼──────────┐        ┌──────────────┐
# MAGIC │ dim_cliente   │◄──────►│ fato_pedidos_itens │◄──────►│ dim_produto   │
# MAGIC │ (customer_id) │        │  (grão: item do   │        │ (product_id)  │
# MAGIC └──────────────┘        │       pedido)      │        └──────────────┘
# MAGIC                         └────────┬──────────┘
# MAGIC                                  │
# MAGIC                        ┌─────────▼─────────┐
# MAGIC                         │  dim_pagamento     │
# MAGIC                         │   (order_id)       │
# MAGIC                         └────────────────────┘
# MAGIC ```

# COMMAND ----------

dbutils.widgets.text("catalog", "olist_lakehouse", "Catálogo Unity Catalog")
catalog = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

silver_orders = spark.table(f"{catalog}.silver.orders")
silver_items = spark.table(f"{catalog}.silver.order_items")
silver_products = spark.table(f"{catalog}.silver.products")
silver_customers = spark.table(f"{catalog}.silver.customers")
silver_reviews = spark.table(f"{catalog}.silver.order_reviews")
silver_payments = spark.table(f"{catalog}.silver.order_payments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `dim_tempo`
# MAGIC Uma linha por data corrida, cobrindo do menor `order_purchase_timestamp` ao maior
# MAGIC `order_estimated_delivery_date` observado nos dados — garante que toda data usada como FK no fato
# MAGIC (compra, entrega ou estimativa) exista na dimensão. Chave substituta: `data_key` no formato `yyyyMMdd`.

# COMMAND ----------

date_bounds = silver_orders.select(
    F.min("order_purchase_timestamp").alias("min_ts"),
    F.greatest(
        F.max("order_estimated_delivery_date"),
        F.max("order_delivered_customer_date"),
    ).alias("max_ts"),
).first()

min_date = date_bounds["min_ts"].date()
max_date = date_bounds["max_ts"].date()

dim_tempo = (
    spark.sql(f"SELECT explode(sequence(to_date('{min_date}'), to_date('{max_date}'), interval 1 day)) AS data")
    .withColumn("data_key", F.date_format("data", "yyyyMMdd").cast("int"))
    .withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumn("dia", F.dayofmonth("data"))
    .withColumn("dia_da_semana", F.date_format("data", "EEEE"))
    .withColumn("trimestre", F.quarter("data"))
    .select("data_key", "data", "ano", "mes", "dia", "dia_da_semana", "trimestre")
)

(
    dim_tempo.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.dim_tempo")
)
print(f"gold.dim_tempo: {spark.table(f'{catalog}.gold.dim_tempo').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `dim_cliente`

# COMMAND ----------

dim_cliente = silver_customers.select(
    F.col("customer_id"),
    F.col("customer_unique_id"),
    F.col("customer_city").alias("cidade"),
    F.col("customer_state").alias("estado"),
)

(
    dim_cliente.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.dim_cliente")
)
print(f"gold.dim_cliente: {spark.table(f'{catalog}.gold.dim_cliente').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `dim_produto`

# COMMAND ----------

dim_produto = silver_products.select(
    F.col("product_id"),
    F.col("product_category_name").alias("categoria"),
    F.col("product_weight_g").alias("peso_g"),
    F.col("product_volume_cm3").alias("volume_cm3"),
)

(
    dim_produto.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.dim_produto")
)
print(f"gold.dim_produto: {spark.table(f'{catalog}.gold.dim_produto').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `dim_pagamento`
# MAGIC Grão: **pedido** (`order_id`). Um pedido pode ter mais de uma forma de pagamento (ex.: voucher + cartão);
# MAGIC a forma "predominante" é a de maior `payment_value`, e `valor_total_pago` soma todas as parcelas do pedido.

# COMMAND ----------

from pyspark.sql.window import Window

w_pag = Window.partitionBy("order_id").orderBy(F.col("payment_value").desc_nulls_last())

pagamento_predominante = (
    silver_payments
    .withColumn("_rn", F.row_number().over(w_pag))
    .filter(F.col("_rn") == 1)
    .select(
        "order_id",
        F.col("payment_type").alias("tipo_pagamento"),
        F.col("payment_installments").alias("parcelas"),
    )
)

valor_total_por_pedido = silver_payments.groupBy("order_id").agg(
    F.sum("payment_value").cast(DecimalType(10, 2)).alias("valor_total_pago")
)

dim_pagamento = pagamento_predominante.join(valor_total_por_pedido, "order_id", "inner")

(
    dim_pagamento.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.dim_pagamento")
)
print(f"gold.dim_pagamento: {spark.table(f'{catalog}.gold.dim_pagamento').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `fato_pedidos_itens`
# MAGIC **Grão:** um item de um pedido (`order_id` + `order_item_id`).
# MAGIC Junta `order_items` (silver) com `orders` (silver) e `order_reviews` (silver, 1 review por pedido).
# MAGIC `review_score` e as métricas de entrega/atraso são herdadas do pedido para cada item (desnormalização
# MAGIC intencional — modelo estrela otimizado para leitura, não para 3FN).

# COMMAND ----------

fato_pedidos_itens = (
    silver_items.alias("i")
    .join(silver_orders.alias("o"), "order_id", "inner")
    .join(silver_reviews.alias("r"), "order_id", "left")
    .select(
        F.col("i.order_id"),
        F.col("i.order_item_id"),
        F.col("o.customer_id"),
        F.col("i.product_id"),
        F.date_format(F.col("o.order_purchase_timestamp"), "yyyyMMdd").cast("int").alias("data_pedido_key"),
        F.col("i.price").alias("valor_produto"),
        F.col("i.freight_value").alias("valor_frete"),
        F.col("o.tempo_entrega_dias"),
        F.col("o.atraso_entrega_dias"),
        F.col("r.review_score"),
        F.col("o.order_status").alias("status_pedido"),
    )
)

(
    fato_pedidos_itens.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.fato_pedidos_itens")
)
print(f"gold.fato_pedidos_itens: {spark.table(f'{catalog}.gold.fato_pedidos_itens').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Chaves primárias/estrangeiras (constraints informativas do Unity Catalog)
# MAGIC O Unity Catalog permite declarar PK/FK **informativas** (não são enforced no momento da escrita,
# MAGIC mas documentam o modelo e são usadas por ferramentas de BI/linhagem, ex. Databricks Genie/AI/BI).

# COMMAND ----------

pk_statements = [
    f"ALTER TABLE {catalog}.gold.dim_tempo     ADD CONSTRAINT pk_dim_tempo     PRIMARY KEY (data_key)",
    f"ALTER TABLE {catalog}.gold.dim_cliente    ADD CONSTRAINT pk_dim_cliente    PRIMARY KEY (customer_id)",
    f"ALTER TABLE {catalog}.gold.dim_produto    ADD CONSTRAINT pk_dim_produto    PRIMARY KEY (product_id)",
    f"ALTER TABLE {catalog}.gold.dim_pagamento  ADD CONSTRAINT pk_dim_pagamento  PRIMARY KEY (order_id)",
    f"ALTER TABLE {catalog}.gold.fato_pedidos_itens ADD CONSTRAINT pk_fato_pedidos_itens PRIMARY KEY (order_id, order_item_id)",
]

fk_statements = [
    f"""ALTER TABLE {catalog}.gold.fato_pedidos_itens
        ADD CONSTRAINT fk_fato_cliente FOREIGN KEY (customer_id) REFERENCES {catalog}.gold.dim_cliente""",
    f"""ALTER TABLE {catalog}.gold.fato_pedidos_itens
        ADD CONSTRAINT fk_fato_produto FOREIGN KEY (product_id) REFERENCES {catalog}.gold.dim_produto""",
    f"""ALTER TABLE {catalog}.gold.fato_pedidos_itens
        ADD CONSTRAINT fk_fato_tempo FOREIGN KEY (data_pedido_key) REFERENCES {catalog}.gold.dim_tempo""",
    f"""ALTER TABLE {catalog}.gold.fato_pedidos_itens
        ADD CONSTRAINT fk_fato_pagamento FOREIGN KEY (order_id) REFERENCES {catalog}.gold.dim_pagamento""",
]

for stmt in pk_statements + fk_statements:
    try:
        spark.sql(stmt)
        print(f"[OK] {stmt.strip().splitlines()[0][:80]}...")
    except Exception as e:
        # Em alguns workspaces do Free Edition constraints podem já existir de uma execução anterior,
        # ou a feature pode estar indisponível no tier gratuito — não interrompe o pipeline.
        print(f"[AVISO] Constraint não aplicada ({e.__class__.__name__}): {stmt.strip().splitlines()[0][:80]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validação final — evidências para o README

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {catalog}.gold"))

# COMMAND ----------

for t in ["dim_tempo", "dim_cliente", "dim_produto", "dim_pagamento", "fato_pedidos_itens"]:
    cnt = spark.table(f"{catalog}.gold.{t}").count()
    print(f"gold.{t:<20} {cnt:>8} linhas")

# COMMAND ----------

display(spark.table(f"{catalog}.gold.fato_pedidos_itens").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Confira também no **Catalog Explorer** (`{catalog}` → `gold`) o diagrama de linhagem gerado
# MAGIC automaticamente pelo Unity Catalog (aba **Lineage**) — tire um print para a seção 3 do README.
