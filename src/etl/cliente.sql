-- Cruzar id do pedido com id do cliente

WITH tb_join AS ( -- join para calcular estados de venda

    SELECT DISTINCT
           t1.idPedido,
           t1.idCliente,
           t2.idVendedor,
           t3.descUF

    FROM silver.olist.pedido as t1

    LEFT JOIN silver.olist.item_pedido as t2
    ON t1.idPedido = t2.idPedido

    LEFT JOIN silver.olist.cliente as t3
    ON t1.idCliente = t3.idCliente

    WHERE dtPedido <  '{date}'
    AND dtPedido >= add_months('{date}', -6)
    AND idVendedor IS NOT NULL

),

-- Query para puxar todos os estados unicos (rodar uma vez só)
-- SELECT DISTINCT descUF
-- FROM tb_join

tb_group AS 

(

    SELECT 

    idVendedor,
    count(distinct descUF) as qtdUFsPedidos,
    -- PERCENTUAL DE VENDAS EM CADA ESTADO POR VENDEDOR
    COUNT( DISTINCT CASE WHEN descUF = 'AC' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoAC,
    COUNT( DISTINCT CASE WHEN descUF = 'AL' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoAL,
    COUNT( DISTINCT CASE WHEN descUF = 'AM' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoAM,
    COUNT( DISTINCT CASE WHEN descUF = 'AP' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoAP,
    COUNT( DISTINCT CASE WHEN descUF = 'BA' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoBA,
    COUNT( DISTINCT CASE WHEN descUF = 'CE' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoCE,
    COUNT( DISTINCT CASE WHEN descUF = 'DF' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoDF,
    COUNT( DISTINCT CASE WHEN descUF = 'ES' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoES,
    COUNT( DISTINCT CASE WHEN descUF = 'GO' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoGO,
    COUNT( DISTINCT CASE WHEN descUF = 'MA' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoMA,
    COUNT( DISTINCT CASE WHEN descUF = 'MG' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoMG,
    COUNT( DISTINCT CASE WHEN descUF = 'MS' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoMS,
    COUNT( DISTINCT CASE WHEN descUF = 'MT' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoMT,
    COUNT( DISTINCT CASE WHEN descUF = 'PA' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoPA,
    COUNT( DISTINCT CASE WHEN descUF = 'PB' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoPB,
    COUNT( DISTINCT CASE WHEN descUF = 'PE' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoPE,
    COUNT( DISTINCT CASE WHEN descUF = 'PI' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoPI,
    COUNT( DISTINCT CASE WHEN descUF = 'PR' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoPR,
    COUNT( DISTINCT CASE WHEN descUF = 'RJ' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoRJ,
    COUNT( DISTINCT CASE WHEN descUF = 'RN' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoRN,
    COUNT( DISTINCT CASE WHEN descUF = 'RO' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoRO,
    COUNT( DISTINCT CASE WHEN descUF = 'RR' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoRR,
    COUNT( DISTINCT CASE WHEN descUF = 'RS' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoRS,
    COUNT( DISTINCT CASE WHEN descUF = 'SC' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoSC,
    COUNT( DISTINCT CASE WHEN descUF = 'SE' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoSE,
    COUNT( DISTINCT CASE WHEN descUF = 'SP' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoSP,
    COUNT( DISTINCT CASE WHEN descUF = 'TO' then idPedido end) / COUNT(DISTINCT idPedido) AS pctPedidoTO

    FROM tb_join

    GROUP BY 1
)

SELECT 
      '{date}' AS dtReference,
      NOW() as dfIngestion,
      *
      
FROM tb_group

