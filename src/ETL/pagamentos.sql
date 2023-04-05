-- Databricks notebook source
SELECT * FROM silver.olist.pagamento_pedido


-- COMMAND ----------

WITH tb_join AS (
SELECT t2.*,
t3.idVendedor

FROM silver.olist.pedido AS t1
LEFT JOIN silver.olist.pagamento_pedido AS t2
ON t1.idPedido = t2.idPedido
LEFT JOIN silver.olist.item_pedido AS t3
ON t1.idPedido = t3.idPedido
WHERE t1.dtPedido <'2018-01-01' 
AND t1.dtPedido >= add_months('2018-01-01',-6)
AND idVendedor IS NOT NULL
),

tb_group AS (

SELECT idVendedor, 
descTipoPagamento, 
count(distinct idPedido) AS qtdPedidoMeioPagamento,
sum(vlPagamento) as vlPedidoMeioPagamento
FROM tb_join
GROUP BY idVendedor,descTipoPagamento
ORDER BY idVendedor,descTipoPagamento
),

tb_summary AS (
SELECT idVendedor,
SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtdPedidoMeioPagamento ELSE 0 END) AS qtd_boletoPedido,
SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtdPedidoMeioPagamento ELSE 0 END) AS qtd_credit_cardPedido,
SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtdPedidoMeioPagamento ELSE 0 END) AS qtd_voucherPedido,
SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtdPedidoMeioPagamento ELSE 0 END) AS qtd_debit_cardPedido,

SUM(CASE WHEN descTipoPagamento = 'boleto' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_boletoPedido,
SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_credit_cardPedido,
SUM(CASE WHEN descTipoPagamento = 'voucher' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_voucherPedido,
SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_debit_cardPedido,

SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtdPedidoMeioPagamento ELSE 0 END) / sum(qtdPedidoMeioPagamento) AS pct_boletoPedido,
SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtdPedidoMeioPagamento ELSE 0 END) / sum(qtdPedidoMeioPagamento) AS pct_credit_cardPedido,
SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtdPedidoMeioPagamento ELSE 0 END) / sum(qtdPedidoMeioPagamento) AS pct_voucherPedido,
SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtdPedidoMeioPagamento ELSE 0 END) / sum(qtdPedidoMeioPagamento) AS pct_debit_cardPedido,

SUM(CASE WHEN descTipoPagamento = 'boleto' THEN vlPedidoMeioPagamento ELSE 0 END) / sum(vlPedidoMeioPagamento) AS pct_valor_boletoPedido,
SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPedidoMeioPagamento ELSE 0 END) / sum(vlPedidoMeioPagamento) AS pct_valor_credit_cardPedido,
SUM(CASE WHEN descTipoPagamento = 'voucher' THEN vlPedidoMeioPagamento ELSE 0 END) / sum(vlPedidoMeioPagamento) AS pct_valor_voucherPedido,
SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN vlPedidoMeioPagamento ELSE 0 END) / sum(vlPedidoMeioPagamento) AS pct_valor_debit_cardPedido

FROM tb_group

GROUP BY 1
),

tb_cartao as (

  SELECT idVendedor,
         AVG(nrParcelas) AS avgQtdeParcelas,
         PERCENTILE(nrParcelas, 0.5) AS medianQtdeParcelas,
         MAX(nrParcelas) AS maxQtdeParcelas,
         MIN(nrParcelas) AS minQtdeParcelas

  FROM tb_join

  WHERE descTipoPagamento = 'credit_card'

  GROUP BY idVendedor

)

SELECT 
       '2018-01-01' AS dtReference,
       t1.*,
       t2.avgQtdeParcelas,
       t2.medianQtdeParcelas,
       t2.maxQtdeParcelas,
       t2.minQtdeParcelas

FROM tb_summary as t1

LEFT JOIN tb_cartao as t2
ON t1.idVendedor = t2.idVendedor

-- COMMAND ----------

 
