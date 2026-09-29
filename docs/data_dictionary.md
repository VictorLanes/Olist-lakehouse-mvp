# Dicionário de Dados / Catálogo de Dados

Catálogo Unity Catalog: `olist_lakehouse` (ou `workspace`, se você reutilizou o catálogo padrão do
Free Edition). Schemas: `bronze`, `silver`, `gold`.

---

## 1. Camada Bronze

Dado bruto, sem transformação de conteúdo. Todas as tabelas recebem 3 colunas de metadados de
ingestão além das colunas originais do CSV (lidas com `inferSchema`, portanto os tipos abaixo são os
inferidos automaticamente pelo Spark).

Colunas de metadados comuns a todas as tabelas Bronze:

| Campo | Tipo | Descrição |
|---|---|---|
| `_source_file` | string | Nome do arquivo CSV de origem |
| `_ingestion_timestamp` | timestamp | Data/hora exata da ingestão |
| `_ingestion_date` | date | Data da ingestão (partição lógica) |

| Tabela Bronze | Arquivo de origem |
|---|---|
| `bronze.orders_raw` | `olist_orders_dataset.csv` |
| `bronze.order_items_raw` | `olist_order_items_dataset.csv` |
| `bronze.products_raw` | `olist_products_dataset.csv` |
| `bronze.customers_raw` | `olist_customers_dataset.csv` |
| `bronze.order_reviews_raw` | `olist_order_reviews_dataset.csv` |
| `bronze.order_payments_raw` | `olist_order_payments_dataset.csv` |

---

## 2. Camada Silver

### `silver.customers` — grão: 1 linha por cliente (`customer_id`)

| Campo | Tipo | Domínio / Regra | Descrição |
|---|---|---|---|
| `customer_id` | string | não nulo, único | Identificador do cliente **por pedido** (chave técnica) |
| `customer_unique_id` | string | não nulo | Identificador único do cliente ao longo do tempo (permite identificar recompra) |
| `customer_zip_code_prefix` | int | 5 dígitos | Prefixo do CEP do cliente |
| `customer_city` | string | trim aplicado | Cidade do cliente |
| `customer_state` | string | 2 letras, UF maiúscula | Estado (UF) do cliente |

### `silver.products` — grão: 1 linha por produto (`product_id`)

| Campo | Tipo | Domínio / Regra | Descrição |
|---|---|---|---|
| `product_id` | string | não nulo, único | Identificador do produto |
| `product_category_name` | string | `'nao_informado'` se nulo | Categoria do produto (em português) |
| `product_weight_g` | double | ≥ 0 | Peso do produto em gramas |
| `product_length_cm` / `product_height_cm` / `product_width_cm` | double | ≥ 0 | Dimensões físicas em cm |
| `product_volume_cm3` | double | derivado: `length * height * width` | Volume cúbico do produto |

### `silver.orders` — grão: 1 linha por pedido (`order_id`)

| Campo | Tipo | Domínio / Regra | Descrição |
|---|---|---|---|
| `order_id` | string | não nulo, único | Identificador do pedido |
| `customer_id` | string | não nulo, FK → `silver.customers` | Cliente do pedido |
| `order_status` | string | `delivered`, `shipped`, `canceled`, `unavailable`, `invoiced`, `processing`, `created`, `approved` | Status do pedido, minúsculo |
| `order_purchase_timestamp` | timestamp | não nulo | Data/hora da compra |
| `order_approved_at` | timestamp | nullable | Data/hora da aprovação do pagamento |
| `order_delivered_carrier_date` | timestamp | nullable | Data de postagem/entrega à transportadora |
| `order_delivered_customer_date` | timestamp | nullable | Data efetiva de entrega ao cliente |
| `order_estimated_delivery_date` | timestamp | não nulo | Data estimada de entrega |
| `tempo_entrega_dias` | int | derivado | `order_delivered_customer_date − order_purchase_timestamp`, em dias |
| `atraso_entrega_dias` | int | derivado | `order_delivered_customer_date − order_estimated_delivery_date`, em dias (positivo = atrasado) |
| `entregue_no_prazo` | boolean | derivado, nullable | `true` se entregue até a data estimada |

### `silver.order_items` — grão: 1 linha por item de pedido (`order_id` + `order_item_id`)

| Campo | Tipo | Domínio / Regra | Descrição |
|---|---|---|---|
| `order_id` | string | FK → `silver.orders` | Pedido ao qual o item pertence |
| `order_item_id` | int | ≥ 1 | Número sequencial do item dentro do pedido |
| `product_id` | string | FK → `silver.products` | Produto do item |
| `seller_id` | string | não nulo | Vendedor responsável pelo item |
| `shipping_limit_date` | timestamp | — | Prazo limite para envio pelo vendedor |
| `price` | decimal(10,2) | ≥ 0 | Preço do item (sem frete) |
| `freight_value` | decimal(10,2) | ≥ 0 | Valor do frete do item |

### `silver.order_reviews` — grão: 1 linha por pedido (`order_id`, deduplicado pela review mais recente)

| Campo | Tipo | Domínio / Regra | Descrição |
|---|---|---|---|
| `review_id` | string | não nulo | Identificador da avaliação |
| `order_id` | string | FK → `silver.orders` | Pedido avaliado |
| `review_score` | int | 1 a 5 | Nota da avaliação |
| `review_creation_date` | timestamp | — | Data de criação da pesquisa de satisfação |
| `review_answer_timestamp` | timestamp | — | Data da resposta do cliente |

### `silver.order_payments` — grão: 1 linha por parcela de pagamento (`order_id` + `payment_sequential`)

| Campo | Tipo | Domínio / Regra | Descrição |
|---|---|---|---|
| `order_id` | string | FK → `silver.orders` | Pedido pago |
| `payment_sequential` | int | ≥ 1 | Sequência do meio de pagamento dentro do pedido |
| `payment_type` | string | `credit_card`, `boleto`, `voucher`, `debit_card`, `not_defined` | Forma de pagamento, minúscula |
| `payment_installments` | int | ≥ 0 | Número de parcelas |
| `payment_value` | decimal(10,2) | ≥ 0 | Valor pago nessa parcela/transação |

---

## 3. Camada Gold — Modelo Dimensional (Star Schema)

### `gold.fato_pedidos_itens` (tabela fato — grão: item do pedido)

| Campo | Tipo | Chave | Descrição |
|---|---|---|---|
| `order_id` | string | PK (composta), FK → `dim_pagamento` | Identificador do pedido |
| `order_item_id` | int | PK (composta) | Número do item dentro do pedido |
| `customer_id` | string | FK → `dim_cliente` | Cliente do pedido |
| `product_id` | string | FK → `dim_produto` | Produto do item |
| `data_pedido_key` | int (yyyyMMdd) | FK → `dim_tempo` | Data da compra |
| `valor_produto` | decimal(10,2) | métrica | Preço do item |
| `valor_frete` | decimal(10,2) | métrica | Valor do frete do item |
| `tempo_entrega_dias` | int | métrica | Dias entre compra e entrega |
| `atraso_entrega_dias` | int | métrica | Dias de atraso vs. estimativa (positivo = atrasado) |
| `review_score` | int | métrica | Nota do pedido (1–5), herdada do pedido |
| `status_pedido` | string | atributo degenerado | Status do pedido no momento da carga |

### `gold.dim_cliente` (grão: cliente)

| Campo | Tipo | Chave | Descrição |
|---|---|---|---|
| `customer_id` | string | PK | Identificador do cliente (por pedido) |
| `customer_unique_id` | string | — | Identificador único do cliente |
| `cidade` | string | — | Cidade do cliente |
| `estado` | string | — | UF do cliente |

### `gold.dim_produto` (grão: produto)

| Campo | Tipo | Chave | Descrição |
|---|---|---|---|
| `product_id` | string | PK | Identificador do produto |
| `categoria` | string | — | Categoria do produto |
| `peso_g` | double | — | Peso em gramas |
| `volume_cm3` | double | — | Volume cúbico (L × A × C) |

### `gold.dim_tempo` (grão: dia calendário)

| Campo | Tipo | Chave | Descrição |
|---|---|---|---|
| `data_key` | int | PK | Chave substituta `yyyyMMdd` |
| `data` | date | — | Data completa |
| `ano` | int | — | Ano |
| `mes` | int | — | Mês (1–12) |
| `dia` | int | — | Dia do mês |
| `dia_da_semana` | string | — | Nome do dia da semana (ex.: `Monday`) |
| `trimestre` | int | — | Trimestre (1–4) |

### `gold.dim_pagamento` (grão: pedido)

| Campo | Tipo | Chave | Descrição |
|---|---|---|---|
| `order_id` | string | PK | Identificador do pedido |
| `tipo_pagamento` | string | — | Forma de pagamento predominante (maior `payment_value`) |
| `parcelas` | int | — | Número de parcelas da forma predominante |
| `valor_total_pago` | decimal(10,2) | — | Soma de todas as parcelas/formas de pagamento do pedido |

---

## 4. Regras de negócio aplicadas (resumo)

1. Linhas sem chave primária de negócio (`order_id`, `customer_id`, `product_id`) são descartadas na Silver.
2. Duplicatas exatas são removidas por chave primária de negócio em cada tabela Silver.
3. `product_category_name` nulo é padronizado para `'nao_informado'` em vez de descartar o produto.
4. `atraso_entrega_dias > 0` define "entrega atrasada"; pedidos ainda não entregues (`order_delivered_customer_date`
   nulo) são excluídos das análises de atraso, mas mantidos no fato para outras métricas.
5. Quando um pedido tem múltiplas avaliações, mantém-se a mais recente (`review_answer_timestamp` máximo).
6. Quando um pedido tem múltiplas formas de pagamento, `dim_pagamento` reporta a de maior valor como
   "predominante" e soma todas as parcelas em `valor_total_pago`.
