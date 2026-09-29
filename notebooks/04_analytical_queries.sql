-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Camada de Análise — Consultas SQL às 3 Perguntas de Negócio
-- MAGIC
-- MAGIC Todas as consultas leem exclusivamente das tabelas **Gold** (`fato_pedidos_itens` + dimensões).
-- MAGIC Ajuste o nome do catálogo abaixo se você usou um valor diferente de `olist_lakehouse` nos
-- MAGIC notebooks 01–03 (por exemplo, `workspace` no Free Edition).

-- COMMAND ----------

USE CATALOG olist_lakehouse;
USE SCHEMA gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pergunta 1
-- MAGIC ### Quais categorias de produtos concentram o maior índice de atrasos na entrega em relação ao prazo estimado?
-- MAGIC
-- MAGIC **Lógica:** considera apenas itens de pedidos efetivamente **entregues**
-- MAGIC (`status_pedido = 'delivered'` e `atraso_entrega_dias` não nulo). Um item está atrasado quando
-- MAGIC `atraso_entrega_dias > 0` (entregue depois da data estimada). Categorias com poucos pedidos
-- MAGIC (< 30) são filtradas para evitar taxas de atraso não representativas (ruído estatístico).

-- COMMAND ----------

SELECT
    COALESCE(p.categoria, 'nao_informado')          AS categoria_produto,
    COUNT(*)                                         AS total_itens_entregues,
    SUM(CASE WHEN f.atraso_entrega_dias > 0 THEN 1 ELSE 0 END) AS itens_atrasados,
    ROUND(
        100.0 * SUM(CASE WHEN f.atraso_entrega_dias > 0 THEN 1 ELSE 0 END) / COUNT(*),
        2
    )                                                 AS taxa_atraso_pct,
    ROUND(AVG(f.atraso_entrega_dias), 1)              AS atraso_medio_dias
FROM fato_pedidos_itens f
JOIN dim_produto p ON f.product_id = p.product_id
WHERE f.status_pedido = 'delivered'
  AND f.atraso_entrega_dias IS NOT NULL
GROUP BY COALESCE(p.categoria, 'nao_informado')
HAVING COUNT(*) >= 30
ORDER BY taxa_atraso_pct DESC
LIMIT 15;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Interpretação (preencher após executar):** liste aqui as 3 categorias com maior `taxa_atraso_pct`,
-- MAGIC o volume de itens envolvido e uma hipótese de causa (ex.: produtos volumosos/pesados, categorias
-- MAGIC concentradas em poucos estados distantes dos centros de distribuição etc.).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pergunta 2
-- MAGIC ### O custo do frete ou o tempo total de frete apresentam correlação direta com a nota de avaliação (review score) do cliente?
-- MAGIC
-- MAGIC **Lógica:** calcula o coeficiente de correlação de Pearson (`corr`, entre -1 e 1) entre
-- MAGIC `review_score` e, respectivamente, `valor_frete` e `tempo_entrega_dias`. Em seguida, quebra a
-- MAGIC nota média por faixas (quartis) de cada variável para uma leitura mais intuitiva que o
-- MAGIC coeficiente isolado.

-- COMMAND ----------

SELECT
    ROUND(CORR(valor_frete, review_score), 4)        AS correlacao_frete_review,
    ROUND(CORR(tempo_entrega_dias, review_score), 4)  AS correlacao_tempo_entrega_review,
    ROUND(CORR(atraso_entrega_dias, review_score), 4) AS correlacao_atraso_review
FROM fato_pedidos_itens
WHERE review_score IS NOT NULL
  AND status_pedido = 'delivered';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Detalhamento: nota média por faixa (quartil) de tempo de entrega

-- COMMAND ----------

WITH base AS (
    SELECT tempo_entrega_dias, review_score
    FROM fato_pedidos_itens
    WHERE review_score IS NOT NULL AND tempo_entrega_dias IS NOT NULL AND status_pedido = 'delivered'
),
quartis AS (
    SELECT
        base.*,
        NTILE(4) OVER (ORDER BY tempo_entrega_dias) AS quartil_tempo_entrega
    FROM base
)
SELECT
    quartil_tempo_entrega,
    MIN(tempo_entrega_dias) AS min_dias,
    MAX(tempo_entrega_dias) AS max_dias,
    COUNT(*)                AS total_itens,
    ROUND(AVG(review_score), 2) AS review_score_medio
FROM quartis
GROUP BY quartil_tempo_entrega
ORDER BY quartil_tempo_entrega;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Detalhamento: nota média por faixa (quartil) de valor de frete

-- COMMAND ----------

WITH base AS (
    SELECT valor_frete, review_score
    FROM fato_pedidos_itens
    WHERE review_score IS NOT NULL AND valor_frete IS NOT NULL AND status_pedido = 'delivered'
),
quartis AS (
    SELECT
        base.*,
        NTILE(4) OVER (ORDER BY valor_frete) AS quartil_frete
    FROM base
)
SELECT
    quartil_frete,
    ROUND(MIN(valor_frete), 2) AS min_frete,
    ROUND(MAX(valor_frete), 2) AS max_frete,
    COUNT(*)                   AS total_itens,
    ROUND(AVG(review_score), 2) AS review_score_medio
FROM quartis
GROUP BY quartil_frete
ORDER BY quartil_frete;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Interpretação (preencher após executar):** registre o sinal e a magnitude das correlações
-- MAGIC (ex.: "correlação fraca/negativa de -0.XX entre tempo de entrega e nota" indica que entregas mais
-- MAGIC longas tendem a puxar a nota para baixo, mas o efeito é [fraco/moderado/forte]) e compare com a
-- MAGIC tendência observada nas tabelas de quartis.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pergunta 3
-- MAGIC ### Quais estados brasileiros geram maior receita total e qual é o método de pagamento predominante em cada um deles?
-- MAGIC
-- MAGIC **Lógica:** receita = soma de `valor_produto + valor_frete` por item, agregada por estado do
-- MAGIC cliente (`dim_cliente.estado`). O método de pagamento predominante por estado é obtido contando
-- MAGIC pedidos distintos (via `dim_pagamento.tipo_pagamento`, já resolvido para 1 linha por pedido) e
-- MAGIC pegando o tipo mais frequente com `ROW_NUMBER`.

-- COMMAND ----------

WITH receita_por_estado AS (
    SELECT
        c.estado,
        ROUND(SUM(f.valor_produto + f.valor_frete), 2) AS receita_total,
        COUNT(DISTINCT f.order_id)                       AS total_pedidos
    FROM fato_pedidos_itens f
    JOIN dim_cliente c ON f.customer_id = c.customer_id
    GROUP BY c.estado
),
pagamentos_por_estado AS (
    SELECT
        c.estado,
        pg.tipo_pagamento,
        COUNT(DISTINCT pg.order_id) AS qtd_pedidos_pagamento
    FROM dim_pagamento pg
    JOIN fato_pedidos_itens f ON f.order_id = pg.order_id
    JOIN dim_cliente c ON f.customer_id = c.customer_id
    GROUP BY c.estado, pg.tipo_pagamento
),
metodo_predominante AS (
    SELECT estado, tipo_pagamento, qtd_pedidos_pagamento,
           ROW_NUMBER() OVER (PARTITION BY estado ORDER BY qtd_pedidos_pagamento DESC) AS rn
    FROM pagamentos_por_estado
)
SELECT
    r.estado,
    r.receita_total,
    r.total_pedidos,
    ROUND(r.receita_total / r.total_pedidos, 2) AS ticket_medio_pedido,
    m.tipo_pagamento                             AS metodo_pagamento_predominante,
    m.qtd_pedidos_pagamento                      AS pedidos_no_metodo_predominante
FROM receita_por_estado r
JOIN metodo_predominante m ON r.estado = m.estado AND m.rn = 1
ORDER BY r.receita_total DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Interpretação (preencher após executar):** destaque os 3 estados de maior receita
-- MAGIC (tipicamente concentrados no Sudeste dado o perfil do dataset Olist), o ticket médio e se o
-- MAGIC método de pagamento predominante (`cartao_credito`, em geral) se mantém constante entre eles ou
-- MAGIC varia por região.
