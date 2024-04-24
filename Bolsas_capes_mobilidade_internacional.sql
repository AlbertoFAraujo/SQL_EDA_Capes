-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Bolsas Capes de programas de Mobilidade Internacional.
-- MAGIC
-- MAGIC **Sobre a base de Dados**
-- MAGIC
-- MAGIC Divulgação das atividades de fomento a bolsas de estudos no Brasil e no exterior de programas de mobilidade internacional, registradas em sistemas de pagamentos informatizados da Capes a partir de 1984. O acervo de dados disponibilizado apresenta possibilidade de recortes por variáveis geográficas, perfil dos bolsistas, áreas de conhecimento e evolução dos valores pagos ao longo da série histórica.
-- MAGIC
-- MAGIC **Fonte:** https://dadosabertos.capes.gov.br/group/bolsas-ativas-em-programas-de-mobilidade-internacional

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Objetivo:**
-- MAGIC
-- MAGIC O objetivo desta análise exploratória é identificar padrões e tendências nas atividades de fomento a bolsas de estudos no Brasil e no exterior, promovidas pela Capes desde 2005. Utilizando os dados disponibilizados nos sistemas de pagamentos informatizados da Capes, pretendemos realizar recortes por variáveis geográficas, perfil dos bolsistas, áreas de conhecimento e evolução dos valores pagos ao longo da série histórica. O propósito é fornecer insights para otimizar a alocação de recursos, identificar áreas de maior demanda e avaliar o impacto das políticas de fomento à mobilidade internacional no desenvolvimento acadêmico e científico do país.

-- COMMAND ----------

-- Esta consulta seleciona todos os registros da tabela "bolsas_capes_csv" no banco de dados padrão (default) e os exibe.
SELECT
  *
FROM
  default.bolsas_capes_csv AS capes;


-- COMMAND ----------

-- Esta consulta calcula o total de bolsas concedidas, contando o número de registros na tabela "bolsas_capes_csv".

SELECT
  COUNT(*) AS `Total Bolsas`
FROM
  bolsas_capes_csv AS capes;

-- COMMAND ----------

-- Esta consulta calcula a quantidade total de beneficiários distintos presentes na tabela "bolsas_capes_csv".

SELECT
  COUNT(DISTINCT(capes.beneficiario)) AS `Total Beneficiários`
FROM
  bolsas_capes_csv AS capes;


-- COMMAND ----------

-- Este bloco de código calcula a quantidade de beneficiários por ano e cria uma tabela temporária chamada quantidade_beneficiarios.

WITH quantidade_beneficiarios AS (
  -- Esta subconsulta calcula a quantidade de beneficiários por ano e mês.
  SELECT
    capes.ano_inicial AS Ano,
    capes.mes_inicial AS Mes,
    count(DISTINCT(capes.beneficiario)) AS `Total Beneficiários`
  FROM
    bolsas_capes_csv AS capes
  GROUP BY
    capes.ano_inicial,
    capes.mes_inicial
)

-- Esta consulta principal utiliza a tabela temporária quantidade_beneficiarios para calcular o total acumulado de beneficiários, bem como a variação percentual ano a ano (YoY).

SELECT
  Ano,
  Mes,
  `Total Beneficiários`,
  -- Esta expressão calcula o total acumulado de beneficiários até o momento atual.
  sum(`Total Beneficiários`) OVER (
    ORDER BY
      Ano ROWS BETWEEN UNBOUNDED PRECEDING
      AND CURRENT ROW
  ) AS `Total Acumulado`,
  -- Esta expressão calcula a variação percentual ano a ano (YoY) no número de beneficiários.
  format_number(
    (
      `Total Beneficiários` - lag(`Total Beneficiários`) OVER (
        ORDER BY
          Ano
      )
    ) / lag(`Total Beneficiários`) OVER (
      ORDER BY
        Ano
    ),
    "0.00%"
  ) AS YoY
FROM
  quantidade_beneficiarios
ORDER BY
  Ano ASC,
  Mes ASC;

-- COMMAND ----------

-- Esta consulta cria ou altera uma view chamada "vw_bolsas_valores" que contém informações sobre beneficiários com mais de uma bolsa e o valor total das bolsas, considerando conversão de moeda, se disponível.
ALTER VIEW vw_bolsas_valores AS(
  SELECT
    capes.beneficiario AS `Beneficiário`,
    COUNT(*) AS `Quantidade de Bolsas`,
    CASE
      WHEN COUNT(*) = 1 THEN '1'
      WHEN COUNT(*) >= 2
      AND COUNT(*) <= 4 THEN '2-4'
      ELSE '5-6'
    END AS `Faixa Bolsas`,
    ROUND(
      SUM(
        IF(
          capes.sigla_moeda = 'BRL',
          capes.valor_recebido_total,
          capes.valor_recebido_total * moeda.`Fator conversao`
        )
      ),
      2
    ) AS `Valor das Bolsas com conversão`
  FROM
    bolsas_capes_csv AS capes
    LEFT JOIN conversao_moeda_1_csv moeda ON capes.sigla_moeda = moeda.Moeda
  GROUP BY
    capes.beneficiario
);
-- Esta consulta seleciona os dados da view "vw_bolsas_valores" e agrupa por faixa de bolsas, ou seja, quantas pessoas possuem até 6 bolsas registradas em seu nome
-- A coluna Quantidade Bolsista informa o número por cpf distinto
SELECT
  `Faixa Bolsas`,
  COUNT(*) AS Total,
  format_number(
    COUNT(*) /(
      SELECT
        COUNT(*)
      FROM
        vw_bolsas_valores
    ),
    "0.00%"
  ) AS Percent_Total
FROM
  vw_bolsas_valores AS capes
GROUP BY
  `Faixa Bolsas`
ORDER BY
  `Faixa Bolsas` ASC

-- COMMAND ----------

-- Esta consulta seleciona os dados da view "vw_bolsas_valores" e os ordena pelo valor total das bolsas sem conversão em ordem decrescente, limitando os resultados às 5 primeiras linhas.

SELECT
  capes.`Faixa Bolsas`,
  capes.`Valor das Bolsas com conversão`
FROM
  vw_bolsas_valores AS capes
ORDER BY
  `Valor das Bolsas com conversão` DESC
LIMIT
  5;

-- COMMAND ----------

-- Esta consulta calcula a duração em anos de cada bolsa, o total de bolsas para cada duração e a porcentagem de bolsas em relação ao total de bolsas.

SELECT
  (capes.ano_final - capes.ano_inicial) AS `Duração (anos)`,
  count(*) AS `Total Bolsas`,
  format_number(
    COUNT(*) / (
      SELECT
        COUNT(*)
      FROM
        bolsas_capes_csv
    ),
    "0.00%"
  ) AS Percent_Bolsas
FROM
  bolsas_capes_csv AS capes
GROUP BY
  `Duração (anos)`
ORDER BY
  `Total Bolsas` DESC;


-- COMMAND ----------

-- Esta consulta seleciona os valores totais das bolsas convertidos para a moeda local, se aplicável, e remove valores duplicados.

SELECT
  DISTINCT(
    IF(
      capes.sigla_moeda = 'BRL',
      capes.valor_recebido_total * 1,
      capes.valor_recebido_total * moeda.`Fator conversao`
    )
  ) AS Valores
FROM
  bolsas_capes_csv AS capes
  LEFT JOIN conversao_moeda_1_csv AS moeda ON capes.sigla_moeda = moeda.Moeda
WHERE
  capes.valor_recebido_total > 0
ORDER BY
  Valores DESC;

-- COMMAND ----------

-- Esta consulta seleciona informações sobre a bolsa com o maior valor recebido convertido para a moeda local, se disponível.

SELECT
  capes.ano_inicial,
  capes.ano_final,
  capes.beneficiario,
  capes.programa_capes,
  capes.pais_destino,
  capes.sigla_moeda,
  capes.grande_area_conhecimento,
  capes.nivel_ensino,
  capes.valor_recebido_total,
  moeda.`Fator conversao`,
  capes.valor_recebido_total * moeda.`Fator conversao` AS valor_recebido_convertido
FROM
  bolsas_capes_csv AS capes
  LEFT JOIN conversao_moeda_1_csv AS moeda ON capes.sigla_moeda = moeda.Moeda
ORDER BY
  valor_recebido_convertido DESC
LIMIT
  1;

-- COMMAND ----------

-- Esta consulta calcula a quantidade de bolsas por programa da Capes.

SELECT
  capes.programa_capes,
  COUNT(*) AS Total,
  format_number(
    COUNT(*) / (
      SELECT
        COUNT(*)
      FROM
        bolsas_capes_csv
    ),
    "0.00%"
  ) AS Percent_Bolsas
FROM
  bolsas_capes_csv AS capes
GROUP BY
  capes.programa_capes
ORDER BY
  Total DESC;

-- COMMAND ----------

-- Esta consulta calcula a quantidade de bolsas por país de destino, mostrando apenas os 10 principais países.

SELECT
  temp.pais_destino,
  COUNT(*) AS Quantidade,
  FORMAT_NUMBER(
    COUNT(*) / (
      SELECT
        COUNT(*)
      FROM
        (
          SELECT
            DISTINCT(capes.cpf),
            capes.pais_destino
          FROM
            bolsas_capes_csv AS capes
        )
    ),
    "0.00%"
  ) AS Percent_total
FROM
  (
    SELECT
      DISTINCT(capes.cpf),
      capes.pais_destino
    FROM
      bolsas_capes_csv AS capes
  ) AS temp
GROUP BY
  temp.pais_destino
ORDER BY
  Quantidade DESC
LIMIT
  10;


-- COMMAND ----------

-- Esta consulta cria uma nova view chamada "vw_capes_unique" para restringir as bolsas apenas por CPF.

ALTER VIEW vw_capes_unique AS (
  SELECT
    DISTINCT(capes.cpf) AS temp,
    *
  FROM
    bolsas_capes_csv AS capes
);


-- COMMAND ----------

-- Quantidade de bolsas por Área do Programa
SELECT
  coalesce(temp.grande_area_conhecimento, 'NÃO INFORMADO') AS `Grande Área`,
  count(*) AS Total,
  format_number(
    COUNT(*) /(
      SELECT
        COUNT(*)
      FROM
        vw_capes_unique
    ),
    "0.00%"
  ) AS Percent_total
FROM
  vw_capes_unique AS temp
GROUP BY
  temp.grande_area_conhecimento
ORDER BY
  Total DESC

-- COMMAND ----------

-- Esta consulta calcula o número de bolsas por grande área de conhecimento.

SELECT
  COALESCE(capes.area_conhecimento, 'NÃO INFORMADO') AS `Área Específica`,
  COUNT(*) AS Total,
  FORMAT_NUMBER(
    COUNT(*) / (
      SELECT
        COUNT(*)
      FROM
        vw_capes_unique
    ),
    "0.00%"
  ) AS Percent_total
FROM
  vw_capes_unique AS capes
GROUP BY
  capes.area_conhecimento
ORDER BY
  Total DESC;

-- COMMAND ----------

-- Esta consulta calcula o número de bolsas por nível de ensino.

SELECT 
  COALESCE(capes.nivel_ensino,'NÃO INFORMADO') AS `Nível Ensino`, 
  COUNT(*) AS Total,
  FORMAT_NUMBER(COUNT(*) / (SELECT COUNT(*) FROM vw_capes_unique), "0.00%") AS Percent_total
FROM vw_capes_unique AS capes
GROUP BY capes.nivel_ensino
ORDER BY Total DESC
LIMIT 5;

-- COMMAND ----------

-- Esta consulta calcula o número de bolsas por estado de origem da instituição.

SELECT
  COALESCE(capes.uf_instituicao_origem, 'NÃO INFORMADO') AS `Estado de Origem da Instituição`,
  COUNT(*) AS Total,
  FORMAT_NUMBER(
    COUNT(*) / (
      SELECT
        COUNT(*)
      FROM
        vw_capes_unique
    ),
    "0.00%"
  ) AS Percent_total
FROM
  vw_capes_unique AS capes
GROUP BY
  capes.uf_instituicao_origem
ORDER BY
  Total DESC
LIMIT 5;

-- COMMAND ----------

-- Esta consulta calcula o número de bolsas por instituição de ensino de origem.

SELECT
  COALESCE(capes.instituicao_ensino_origem, 'NÃO INFORMADO') AS `Instituição de Ensino de Origem`,
  COUNT(*) AS Total,
  FORMAT_NUMBER(
    COUNT(*) / (
      SELECT
        COUNT(*)
      FROM
        vw_capes_unique
    ),
    "0.00%"
  ) AS Percent_total
FROM
  vw_capes_unique AS capes
GROUP BY
  capes.instituicao_ensino_origem
ORDER BY
  Total DESC
LIMIT 5;

-- COMMAND ----------

-- Esta consulta calcula os maiores e menores valores de bolsas por grande área de conhecimento, excluindo valores iguais a zero.

SELECT
  COALESCE(capes.grande_area_conhecimento, 'NÃO INFORMADO') AS `Grande Área`,
  ROUND(MAX(capes.valor_recebido_bolsa), 2) AS `Maior valor Bolsa`,
  ROUND(MIN(capes.valor_recebido_bolsa), 2) AS `Menor valor Bolsa`
FROM
  bolsas_capes_csv AS capes
WHERE
  capes.valor_recebido_bolsa > 0
GROUP BY
  capes.grande_area_conhecimento
ORDER BY
  `Maior valor Bolsa` DESC,
  `Menor valor Bolsa` DESC;
