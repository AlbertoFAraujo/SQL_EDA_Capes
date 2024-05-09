![image](https://github.com/AlbertoFAraujo/SQL_EDA_Capes/assets/105552990/bfbd333e-c110-48ba-801d-68a2e6708517)

**Link Databrick:** [EDA_Capes_databrick](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4181035651798210/16913316101068/4641171831932757/latest.html)

### Tecnologias utilizadas: 
| [<img align="center" alt="sql" height="60" width="60" src="https://github.com/AlbertoFAraujo/SQL_EDA_exercito2022/assets/105552990/805dfaf3-4725-47f9-86d5-241953a018ab">](https://learn.microsoft.com/en-us/sql/sql-server/?view=sql-server-ver16) | [<img align="center" alt="databrick" height="60" width="60" src="https://github.com/AlbertoFAraujo/SQL_EDA_exercito2022/assets/105552990/b188ba83-b87f-4f80-b79f-e91be05602af">](https://www.databricks.com/) | [<img align="center" alt="apache_spark" height="60" width="100" src="https://github.com/AlbertoFAraujo/SQL_EDA_exercito2022/assets/105552990/4b3cbec3-98da-499b-9d83-908ce9458d29">](https://spark.apache.org/docs/latest/)|
|:---:|:---:|:---:|
| SQL | Databrick  | Apache Spark |

- **SQL**: Linguagem padrão para consulta e manipulação de bancos de dados relacionais, permitindo operações como consulta, inserção, atualização e exclusão de dados.
- **Databricks**: Plataforma de análise de dados e aprendizado de máquina baseada no Apache Spark, oferecendo um ambiente unificado para processamento em larga escala e desenvolvimento colaborativo.
- **Apache Spark**: Framework open-source para processamento de big data, oferecendo uma API unificada para operações distribuídas em dados, com suporte a várias linguagens e módulos para processamento de streaming e machine learning.
<hr>

### Sobre a base de Dados

Divulgação das atividades de fomento a bolsas de estudos no Brasil e no exterior de programas de mobilidade internacional, registradas em sistemas de pagamentos informatizados da Capes a partir de 1984. O acervo de dados disponibilizado apresenta possibilidade de recortes por variáveis geográficas, perfil dos bolsistas, áreas de conhecimento e evolução dos valores pagos ao longo da série histórica.

**Fonte:** https://dadosabertos.capes.gov.br/group/bolsas-ativas-em-programas-de-mobilidade-internacional

<hr>

### Objetivo: 

O objetivo desta análise exploratória é identificar padrões e tendências nas atividades de fomento a bolsas de estudos no Brasil e no exterior, promovidas pela Capes desde 2005. Utilizando os dados disponibilizados nos sistemas de pagamentos informatizados da Capes, pretendemos realizar recortes por variáveis geográficas, perfil dos bolsistas, áreas de conhecimento e evolução dos valores pagos ao longo da série histórica. O propósito é fornecer insights para otimizar a alocação de recursos, identificar áreas de maior demanda e avaliar o impacto das políticas de fomento à mobilidade internacional no desenvolvimento acadêmico e científico do país.

<hr>

### Script SQL
```SQL
-- Esta consulta seleciona todos os registros da tabela "bolsas_capes_csv" no banco de dados padrão (default) e os exibe.
SELECT
  *
FROM
  default.bolsas_capes_csv AS capes;
```
```SQL
-- Esta consulta calcula o total de bolsas concedidas, contando o número de registros na tabela "bolsas_capes_csv".

SELECT
  COUNT(*) AS `Total Bolsas`
FROM
  bolsas_capes_csv AS capes;
```
| Total Bolsas |
|--------------|
|      18653   |

```SQL
-- Esta consulta calcula a quantidade total de beneficiários distintos presentes na tabela "bolsas_capes_csv".

SELECT
  COUNT(DISTINCT(capes.beneficiario)) AS `Total Beneficiários`
FROM
  bolsas_capes_csv AS capes;
```
| Total Beneficiários |
|---------------------|
|        13380        |

```SQL
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
```

| Ano  | Mes | Total Beneficiários | Total Acumulado | YoY    |
|------|-----|---------------------|-----------------|--------|
| 2005 |   4 |                   1 |            null |        |
| 2005 |   9 |                   1 |               2 | 0.00%  |
| 2006 |   3 |                  18 |              97 | -56.10%|
| 2006 |   4 |                  18 |             115 | 0.00%  |
| 2006 |   6 |                   1 |               8 | 0.00%  |
| 2006 |   7 |                   1 |               7 | -75.00%|
| 2006 |   8 |                  27 |             142 | 50.00% |
| 2006 |   9 |                  41 |              79 | 36.67% |
| 2006 |  10 |                  30 |              38 | 2900.00%|
| 2006 |  11 |                   8 |             150 | -70.37%|


Figura 1: Quantidade de beneficiários x Ano x Mês

![image](https://github.com/AlbertoFAraujo/SQL_EDA_Capes/assets/105552990/d7b06154-340e-45fe-8880-100b425675bf)

Figura 2: Acumulado x Ano

![image](https://github.com/AlbertoFAraujo/SQL_EDA_Capes/assets/105552990/20c5eb7c-92b8-49da-af2d-2e8ce41bf08a)


```SQL
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
```

| Faixa Bolsas | Total | Percent_Total |
|--------------|-------|---------------|
|            1 |  9217 |         68.89%|
|          2-4 |  4161 |         31.10%|
|          5-6 |     2 |          0.01%|


Figura 3: Total de bolsas x beneficiários

![image](https://github.com/AlbertoFAraujo/SQL_EDA_Capes/assets/105552990/ddf8aae9-9d28-47d7-b4cc-dc4879e7f6d1)

```SQL
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
```
| Faixa Bolsas | Valor das Bolsas com conversão |
|--------------|--------------------------------|
|          2-4 |                      1142276.9|
|          2-4 |                      1123586.51|
|          2-4 |                      1048089.4 |
|          2-4 |                      1017713.69|
|          2-4 |                      1003164.38|


```SQL
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
```

| Duração (anos) | Total Bolsas | Percent_Bolsas |
|----------------|--------------|----------------|
|              1 |         9271 |         49.70% |
|              2 |            0 |         36.71% |
|              3 |         1308 |          7.01% |
|              4 |          634 |          3.40% |
|              5 |          459 |          2.46% |
|              6 |           69 |          0.37% |
|              7 |           34 |          0.18% |
|              8 |           19 |          0.10% |
|              9 |           10 |          0.05% |
|             10 |            2 |          0.01% |


```SQL
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
```
Figura 4: Boxplot dos valores das bolsas
![image](https://github.com/AlbertoFAraujo/SQL_EDA_Capes/assets/105552990/56410606-5718-4e62-a833-c1e88f54c332)



```SQL
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
```

```SQL
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
```

| programa_capes                                                                                      | Total | Percent_Bolsas |
|-----------------------------------------------------------------------------------------------------|-------|----------------|
| PDSE - PROGRAMA DE DOUTORADO SANDUÍCHE NO EXTERIOR                                                  | 5890  | 31.58%         |
| GS/CSF - GRADUAÇÃO SANDUÍCHE - PROGRAMA CIÊNCIA SEM FRONTEIRAS                                       | 1394  | 7.47%          |
| PDEE - ESTÁGIO DE DOUTORANDO                                                                        | 1136  | 6.09%          |
| PPDE - PROGRAMA DE PÓS-DOUTORADO NO EXTERIOR                                                         | 969   | 5.19%          |
| BRAFITEC - BRASIL FRANÇA ENGENHARIA TECNOLOGIA                                                       | 833   | 4.47%          |
| DPE - PROGRAMA DE DOUTORADO PLENO NO EXTERIOR                                                        | 827   | 4.43%          |
| PROGRAMA ESTUDANTES CONVÊNIO DE PÓS-GRADUAÇÃO                                                        | 671   | 3.60%          |
| ES - PROGRAMA DE ESTÁGIO SÊNIOR NO EXTERIOR                                                          | 632   | 3.39%          |
| PDPI - PROGRAMA DE DESENVOLVIMENTO PROFISSIONAL PARA PROFESSORES DE LÍNGUA INGLESA NOS ESTADOS UNIDOS | 537   | 2.88%          |


```SQL
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
```

Figura 5: Top 10 países destino
![image](https://github.com/AlbertoFAraujo/SQL_EDA_Capes/assets/105552990/d68a0e86-e118-49b2-bd2a-ab8cbb899b1b)


```SQL
-- Esta consulta cria uma nova view chamada "vw_capes_unique" para restringir as bolsas apenas por CPF.

ALTER VIEW vw_capes_unique AS (
  SELECT
    DISTINCT(capes.cpf) AS temp,
    *
  FROM
    bolsas_capes_csv AS capes
);

```

```SQL
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
```

Figura 6: Área x distribuição de bolsas

![image](https://github.com/AlbertoFAraujo/SQL_EDA_Capes/assets/105552990/54d43a62-217c-4097-80f9-a4e77c63826f)


```SQL
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
```

| Área Específica       | Total | Percent_total |
|-----------------------|-------|---------------|
| NÃO INFORMADO         | 5957  | 31.95%        |
| EDUCAÇÃO              | 676   | 3.63%         |
| AGRONOMIA             | 623   | 3.34%         |
| ENGENHARIA ELÉTRICA   | 571   | 3.06%         |
| ENGENHARIA MECÂNICA   | 530   | 2.84%         |
| MEDICINA              | 529   | 2.84%         |
| CIÊNCIA DA COMPUTAÇÃO | 525   | 2.82%         |
| ENGENHARIA DE PRODUÇÃO| 380   | 2.04%         |
| LETRAS                | 375   | 2.01%         |
| QUÍMICA               | 344   | 1.84%         |


```SQL
-- Esta consulta calcula o número de bolsas por nível de ensino.

SELECT 
  COALESCE(capes.nivel_ensino,'NÃO INFORMADO') AS `Nível Ensino`, 
  COUNT(*) AS Total,
  FORMAT_NUMBER(COUNT(*) / (SELECT COUNT(*) FROM vw_capes_unique), "0.00%") AS Percent_total
FROM vw_capes_unique AS capes
GROUP BY capes.nivel_ensino
ORDER BY Total DESC
LIMIT 5;
```

| Nível Ensino                               | Total | Percent_total |
|--------------------------------------------|-------|---------------|
| DOUTORADO SANDUÍCHE                       | 8592  | 46.08%        |
| GRADUAÇÃO SANDUÍCHE                       | 3988  | 21.39%        |
| DOUTORADO PLENO                           | 1800  | 9.65%         |
| ESTÁGIO PÓS-DOUTORAL                      | 1633  | 8.76%         |
| CAPACITAÇÃO PROFESSORES DA EDUCAÇÃO BÁSICA| 684   | 3.67%         |



```SQL
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
```

Figura 7: Estados x Quantidades de bolsas
![image](https://github.com/AlbertoFAraujo/SQL_EDA_Capes/assets/105552990/2507505b-a97c-45a4-b84e-598e0ae1debd)



```SQL
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
```

| Instituição de Ensino de Origem           | Total | Percent_total |
|-------------------------------------------|-------|---------------|
| UNIVERSIDADE DE SÃO PAULO                 | 1814  | 9.73%         |
| NÃO INFORMADO                             | 986   | 5.29%         |
| UNIVERSIDADE FEDERAL DO RIO GRANDE DO SUL | 977   | 5.24%         |
| UNIVERSIDADE FEDERAL DE SANTA CATARINA    | 854   | 4.58%         |
| UNIVERSIDADE ESTADUAL DE CAMPINAS         | 846   | 4.54%         |


```SQL
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
```
| Grande Área                       | Maior valor Bolsa | Menor valor Bolsa |
|-----------------------------------|-------------------|-------------------|
| CIÊNCIAS AGRÁRIAS                 | 99000             | 10010             |
| CIÊNCIAS BIOLÓGICAS               | 99000             | 10010             |
| CIÊNCIAS DA SAÚDE                 | 99000             | 10010             |
| MULTIDISCIPLINAR                  | 99000             | 10010             |
| CIÊNCIAS HUMANAS                  | 9996              | 101000            |
| ENGENHARIAS                       | 9976              | 10005             |
| CIÊNCIAS SOCIAIS APLICADAS        | 9947              | 10010             |
| NÃO INFORMADO                     | 9900              | 10440             |
| LINGÜÍSTICA, LETRAS E ARTES       | 9900              | 10140             |
| CIÊNCIAS EXATAS E DA TERRA        | 9900              | 10005             |

****
### Parecer da Análise Exploratória:
- Entre 2005 e 2019 foram disponibilizadas cerca de 18653 bolsas Capes, podendo ser vinculada mais de uma bolsa por 	CPF;
- Foram registrados 13380 beneficiários das bolsas;
- A quantidade de bolsas concedidas, cresceu de forma constante de 2005 a 2010, um aumento de 500 bolsas. Em 2012 houve um salto de mais 400%, chegando a quase 1000 bolsas. Esse aumento pode estar relacionado a algum programa ou política de incentivo à mobilidade internacional nesse período;
- A quantidade de bolsas concedidas caiu de forma acentuada de 2012 a 2019, voltando aos níveis de 2005. Em 2019, foram concedidas apenas cerca de 120 bolsas. Essa queda pode estar relacionada a algum fator econômico, político ou social que afetou a disponibilidade ou a demanda por bolsas nesse período;
- A quantidade de bolsas concedidas variou nos anos intermediários, mas a tendência geral foi descendente após o pico de 2012. Alguns anos apresentaram aumentos ou quedas mais expressivos, como 2009, 2011, 2013 e 2018. Essas flutuações podem estar relacionadas a eventos específicos ou a mudanças pontuais nas condições de concessão das bolsas;
- 0.01% (2) dos beneficiários de bolsas CAPES possuem entrem 5 e 6 bolsas registradas em seu CPF. 31.10% (4161) de 2 a 4 e 68.89% (9217) possuem apenas o registro de 1 bolsa;
- O maior valor de bolsa já convertida em Real, considerando a soma das bolsas vinculadas ao mesmo beneficiário(mesmo CPF) foi de R$1.142.276,9 e está na faixa de 2 a 4 bolsas. Os demais 4 maiores valores de bolsas acumulativas também estão na faixa de 2-4 bolsas por CPF. Concluíndo que a quantidade de bolsas não é fator relacionado ao valor da bolsa;
- A bolsa com maior valor único, sem considerar acumulativa para o mesmo CPF foi registrada em R$887.150,80.
- Os valores únicos por bolsa, estão dentro do intervalo entre 37k e 120k. Alguns outliers foram identificados com valores de bolsas acima de 230k, sendo o maior valor de bolsa registrado em R$887.150,80 no programa de DPE - PROGRAMA DE DOUTORADO PLENO NO EXTERIOR com destino ao país da Holanda na área de Ciências da Saúde e nível de ensino em Doutorado Pleno;
- Das 18653 bolsas, 49,70% são para programas de 1 ano e 36,71% para programas inferiores a 1 ano, sendo esses provavelmente referentes a estágios, capacitações, itnercâmbios ou outras modalidades de curta duração;
- Os programas com duração de 10 anos são destinados ao programa da CAPES: UAB-MOÇAMBIQUE - PROGRAMA DE APOIO À EXPANSÃO DA EDUCAÇÃO SUPERIOR À DISTÂNCIA NA REPÚBLICA DE MOÇAMBIQUE, totalizando um valor de R$20170;
- O programa PDSE - Programa de Doutorando Sanduíche no Exterior é responsável por 31,58% (5890) das bolsas concedidas do Capes;
- Estados Unidos é o país com maior indicador de destino das bolsas capes dentre o período analisado, com 26,72% (Cerca de 3580 bolsas destinadas). Em sequência, França com 11,58% de programas destinados;
- Dentre as grandes áreas disponíveis ao programa de bolsas, 16,18% são destinadas à Ciências Humanas e em seguida 16,16% para Engenharias;
- Dentre as subáreas de conhecimento, 3,63% (676) das bolsas são destinadas à Educação, seguida de 3,34% (623) para Agronomia. 31,95% (5957) não foram informadas na base;
- Em relação ao nível de ensino, 46,08% (8592) destina-se ao Doutorado Sanduíche e 21,39% (3988) à Graduação Sanduíche;
- Ao dados demográficos das instituições de ensino, cerca de 25,51% (4756) são do estado de São Paulo e 11,31% (2108) do Rio de Janeiro;
- 1814 beneficiários das bolsas capes são da Universidade de São Paulo (USP).





