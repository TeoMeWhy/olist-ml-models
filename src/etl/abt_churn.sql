-- Databricks notebook source
WITH tbl_active AS (
SELECT
      t2.idVendedor, 
      MIN(DATE(t1.dtPedido)) AS dtPedido 
FROM 
      silver.olist.pedido as t1 
LEFT JOIN 
      silver.olist.item_pedido as t2 
            ON t1.idPedido = t2.idPedido 
WHERE 
      t1.dtPedido >= '2018-01-01' 
AND 
      t1.dtPedido <= DATE_ADD('2018-01-01', 45)
AND 
      t2.idVendedor IS NOT NULL 
GROUP BY 
      t2.idVendedor 
) 

SELECT 
      t1.*, 
      t2.* EXCEPT (t2.idVendedor),
      t3.* EXCEPT (t3.idVendedor),
      t4.* EXCEPT (t4.idVendedor),
      t5.* EXCEPT (t5.idVendedor),
      t6.* EXCEPT (t6.idVendedor),
      CASE WHEN t7.idVendedor IS NULL THEN 1 ELSE 0 END AS IsChurn 
FROM  
      sandbox.analystics_churn_model.fs_vendedor_vendas t1 
LEFT JOIN 
      sandbox.analystics_churn_model.fs_vendedor_avaliacao t2
      ON t1.idVendedor = t2.idVendedor
      AND t1.DateRef = t2.DateRef
LEFT JOIN 
      sandbox.analystics_churn_model.fs_vendedor_produto t3
      ON t1.idVendedor = t3.idVendedor 
      AND t1.DateRef = t3.DateRef
LEFT JOIN 
      sandbox.analystics_churn_model.fs_vendedor_entrega  t4
      ON t1.idVendedor = t4.idVendedor
      AND t1.DateRef = t4.DateRef
LEFT JOIN 
      sandbox.analystics_churn_model.fs_vendedor_cliente t5
      ON t1.idVendedor = t5.idVendedor
      AND t1.DateRef = t5.DateRef
LEFT JOIN 
      sandbox.analystics_churn_model.fs_vendedor_pagamento t6
      ON t1.idVendedor = t6.idVendedor
      AND t1.DateRef = t6.DateRef
LEFT JOIN 
      tbl_active  t7
            ON t1.idVendedor = t7.idVendedor
            AND datediff(t7.dtPedido, t1.DateRef) <= 45 
WHERE QntRecencia <=45


-- COMMAND ----------


SELECT * FROM  sandbox.analystics_churn_model.fs_vendedor_pagamento
WHERE QntRecencia <=45
