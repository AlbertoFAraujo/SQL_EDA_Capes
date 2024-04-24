![image](https://github.com/AlbertoFAraujo/SQL_EDA_exercito2022/assets/105552990/36c8a279-b478-40be-a2f0-4a49ca4f6a5a)

### Tecnologias utilizadas: 
| [<img align="center" alt="sql" height="60" width="60" src="https://github.com/AlbertoFAraujo/SQL_EDA_exercito2022/assets/105552990/805dfaf3-4725-47f9-86d5-241953a018ab">](https://learn.microsoft.com/en-us/sql/sql-server/?view=sql-server-ver16) | [<img align="center" alt="databrick" height="60" width="60" src="https://github.com/AlbertoFAraujo/SQL_EDA_exercito2022/assets/105552990/b188ba83-b87f-4f80-b79f-e91be05602af">](https://www.databricks.com/) | [<img align="center" alt="apache_spark" height="60" width="100" src="https://github.com/AlbertoFAraujo/SQL_EDA_exercito2022/assets/105552990/4b3cbec3-98da-499b-9d83-908ce9458d29">](https://spark.apache.org/docs/latest/)|
|:---:|:---:|:---:|
| SQL | Databrick  | Apache Spark |

- **SQL**: Linguagem padrão para consulta e manipulação de bancos de dados relacionais, permitindo operações como consulta, inserção, atualização e exclusão de dados.
- **Databricks**: Plataforma de análise de dados e aprendizado de máquina baseada no Apache Spark, oferecendo um ambiente unificado para processamento em larga escala e desenvolvimento colaborativo.
- **Apache Spark**: Framework open-source para processamento de big data, oferecendo uma API unificada para operações distribuídas em dados, com suporte a várias linguagens e módulos para processamento de streaming e machine learning.
<hr>

### Sobre a base de Dados

Dados dos cidadãos brasileiros residentes no Brasil e no exterior que se alistaram no Serviço Militar Obrigatório de 2022 e relação das Juntas de Serviço Militar (JSM). Estão incluídas em todas as tabelas de dados do Serviço Militar as seguintes informações: ano de nascimento, peso, altura, tamanho da cabeça, número do calçado, tamanho da cintura, religião, município, UF e país de nascimento, estado civil, sexo, escolaridade, ano de alistamento, se foi dispensado ou não, zona residencial, município, UF e país de residência, junta, município e UF da junta. Estão incluídas na tabela de dados das JSM as seguintes informações: código, nome, endereço, bairro, CEP, telefone, município e UF.

**Fonte:** https://dados.gov.br/dados/conjuntos-dados/servico-militar

<hr>

### Objetivo: 

O objetivo desta análise exploratória é examinar e compreender as características demográficas e físicas dos cidadãos brasileiros alistados no Serviço Militar Obrigatório do ano de 2022. Por meio da análise dos dados disponíveis, buscamos identificar padrões, tendências e insights relevantes relacionados à distribuição de idade, gênero, região geográfica, estado civil, dispensa militar, educação, entre outras variáveis. Este estudo visa fornecer uma visão abrangente do perfil dos alistados, contribuindo para uma melhor compreensão do contexto socioeconômico e demográfico das pessoas sujeitas ao serviço militar obrigatório no Brasil.

<hr>

### Script SQL
```SQL
-- Visualizando a base de dados para conhecimento das variáveis
SELECT * FROM alistamento_exercito2022 LIMIT 10
```
| ANO_NASCIMENTO | PESO | ALTURA | CABECA | CALCADO | CINTURA | MUN_NASCIMENTO | UF_NASCIMENTO |
|----------------|------|--------|--------|---------|---------|----------------|---------------|
| 1960           | 69   | 176    | 56     | 42      | null    | RIO DE JANEIRO | RJ            |
| 1995           | 79   | 181    | 56     | 41      | 88      | PALMEIRA DAS MISSOES | RS     |
| 1974           | 64   | 165    | 58     | 38      | 75      | PORTO ALEGRE   | RS            |
| 1998           | 55   | 180    | 53     | 41      | 74      | JANDIRA        | SP            |
| 1999           | 76   | 186    | 57     | 42      | 88      | CACERES        | MT            |
| 2000           | 76   | 184    | 58     | 42      | 78      | BRASILIA       | DF            |
| 2000           | 60   | 165    | 53     | 40      | 71      | OLINDA         | PE            |
| 2000           | 98   | 184    | 56     | 45      | 90      | OLINDA         | PE            |
| 2003           | 78   | 175    | 58     | 42      | 87      | BRASILIA       | DF            |
| 2003           | 80   | 170    | 54     | 40      | 79      | FORTALEZA      | CE            |

```SQL
-- Visualizando a base de dados para conhecimento das variáveis
SELECT * FROM estados_brasileiro
```
| Estado          | Sigla | Região     |
|----------------|-------|------------|
| Rondônia       | RO    | Norte      |
| Sergipe        | SE    | Nordeste   |
| Minas Gerais   | MG    | Sudeste    |
| Bahia          | BA    | Nordeste   |
| Mato Grosso    | MT    | Centro     |
| Rio de Janeiro | RJ    | Sudeste    |
| Paraná         | PR    | Sul        |
| Roraima        | RR    | Norte      |
| Ceará          | CE    | Nordeste   |
| Paraíba        | PB    | Nordeste   |

```SQL
-- Calculando o número total de alistados em 2022.
SELECT 
  count(*) AS Total_alistados
FROM alistamento_exercito2022
```
| Total_alistados |
|-----------------|
|      1020927    |

```SQL
-- Qual a distribuição de alistados por gênero? e o percentual?
SELECT
  EXE.SEXO AS Sexo,
  count(*) AS Total,
  format_number(count(*)/(SELECT count(*) FROM alistamento_exercito2022),"0.000%") AS Percentual
FROM alistamento_exercito2022 AS EXE
GROUP BY EXE.SEXO
```

| Sexo | Total   | Percentual |
|------|---------|------------|
| F    | 79      | 0.008%     |
| M    | 1020848 | 99.992%    |

Figura 1: Quantidade por Gênero

![newplot (2)](https://github.com/AlbertoFAraujo/SQL_EDA_exercito2022/assets/105552990/7c0a7d21-f2e0-4137-894b-1f135d7c3513)

```SQL
-- Distribuição de alistados por gênero x Região e total por região
SELECT 
    COALESCE(EST.`Região`, 'Exterior') AS Regiao,
    SUM(CASE WHEN EXE.SEXO = "F" THEN 1 ELSE 0 END) AS Feminino,
    format_number(SUM(CASE WHEN EXE.SEXO = "F" THEN 1 ELSE 0 END)/(SELECT count(*) FROM alistamento_exercito2022 WHERE SEXO = "F"),"0.00%") AS Percent_feminino,
    SUM(CASE WHEN EXE.SEXO = "M" THEN 1 ELSE 0 END) AS Masculino,
    format_number(SUM(CASE WHEN EXE.SEXO = "M" THEN 1 ELSE 0 END)/(SELECT count(*) FROM alistamento_exercito2022 WHERE SEXO = "M"),"0.00%") AS Percent_masculino,
    count(*) AS Total,
    format_number(count(*)/(SELECT COUNT(*) FROM alistamento_exercito2022),"0.00%") AS Percent_total
FROM alistamento_exercito2022 EXE
LEFT JOIN estados_brasileiro EST
ON EXE.UF_RESIDENCIA = EST.Sigla
GROUP BY EST.`Região`
ORDER BY Masculino DESC, Feminino DESC
```

| Regiao    | Feminino | Percent_feminino | Masculino | Percent_masculino | Total   | Percent_total |
|-----------|----------|------------------|-----------|-------------------|---------|---------------|
| Sudeste   | 23       | 29.11%           | 457822    | 44.85%            | 457845  | 44.85%        |
| Nordeste  | 29       | 36.71%           | 220340    | 21.58%            | 220369  | 21.59%        |
| Sul       | 17       | 21.52%           | 138259    | 13.54%            | 138276  | 13.54%        |
| Norte     | 4        | 5.06%            | 119888    | 11.74%            | 119892  | 11.74%        |
| Centro    | 6        | 7.59%            | 77140     | 7.56%             | 77146   | 7.56%         |
| Exterior  | 0        | 0.00%            | 7399      | 0.72%             | 7399    | 0.72%         |

Figura 2: Gênero x Região

![image](https://github.com/AlbertoFAraujo/SQL_EDA_exercito2022/assets/105552990/81acc877-c230-4fe5-a9fd-7c95e862609b)

Figura 3: Total por Região 

![image](https://github.com/AlbertoFAraujo/SQL_EDA_exercito2022/assets/105552990/97f1f336-b335-4852-a081-072e4de8561d)

Figura 4: Gênero Feminino x Região

![image](https://github.com/AlbertoFAraujo/SQL_EDA_exercito2022/assets/105552990/4e1c9466-632f-422c-a265-ac0823d5b7ce)

Figura 5: Gênero Masculino x Região

![image](https://github.com/AlbertoFAraujo/SQL_EDA_exercito2022/assets/105552990/5d369589-a6d7-4613-8eb6-6239760785bd)

```SQL
-- Top 5 estados com maiores alistados no ano de 2022
SELECT
  EST.Estado AS Estados,
  count(*) AS Total,
  format_number(count(*)/(SELECT count(*) FROM alistamento_exercito2022),"0.00%") AS Percent_total
FROM alistamento_exercito2022 EXE
LEFT JOIN estados_brasileiro EST
ON EXE.UF_RESIDENCIA = EST.Sigla
GROUP BY EST.Estado
ORDER BY Total DESC
LIMIT 5 
```

Figura 6: Top 5 estados x Gênero Masculino

![image](https://github.com/AlbertoFAraujo/SQL_EDA_exercito2022/assets/105552990/fae36237-1a66-41bd-99dd-20b0caeddf67)

```SQL
-- Quantos alistados da região Norte estão acima ou igual a média do número de calçado do brasileiro?

WITH media_calcado AS (SELECT avg(EXE.CALCADO) AS media_calcados FROM alistamento_exercito2022 EXE) --Calculando a média do calçado

SELECT 
    EST.`Região` AS Regiao,
    count(*) AS Total,
    format_number(count(*)/(SELECT COUNT(*) FROM alistamento_exercito2022 EXE INNER JOIN estados_brasileiro EST ON EXE.UF_RESIDENCIA = EST.Sigla WHERE EST.`Região` = 'Nordeste'),"0.00%") AS Percent_total
FROM alistamento_exercito2022 EXE
LEFT JOIN estados_brasileiro EST
ON EXE.UF_RESIDENCIA = EST.Sigla
CROSS JOIN media_calcado
WHERE EXE.CALCADO >= media_calcados AND EST.`Região` = 'Nordeste'
GROUP BY EST.`Região`
```

| Regiao   | Total | Percent_total |
|----------|-------|---------------|
| Nordeste | 31232 | 14.17%        |

```SQL
-- Quantos alistados da Região Sul possuem Ensino Superior Completo?
SELECT 
  EST.`Região`,
  (SELECT count(*) FROM alistamento_exercito2022 EXE INNER JOIN estados_brasileiro EST ON EXE.UF_RESIDENCIA = EST.Sigla WHERE EST.`Região` = 'Sudeste') AS Total_Sudeste,
  count(*) AS Total_Ensino_Sup,
  format_number(count(*)/(SELECT count(*) FROM alistamento_exercito2022 EXE INNER JOIN estados_brasileiro EST ON EXE.UF_RESIDENCIA = EST.Sigla WHERE EST.`Região` = 'Sudeste'),"0.00%") AS Percent_Ensino_Sup
FROM alistamento_exercito2022 EXE
  INNER JOIN estados_brasileiro EST
  ON EXE.UF_RESIDENCIA = EST.Sigla
WHERE EXE.ESCOLARIDADE = 'Ensino Superior Completo' AND EST.`Região` = 'Sudeste'
GROUP BY EST.`Região`
```

| Região   | Total_Sudeste | Total_Ensino_Sup | Percent_Ensino_Sup |
|----------|---------------|-------------------|--------------------|
| Sudeste  | 457845        | 684               | 0.15%              |

```SQL
-- Top 3 estados com maior número de alistados dentro do peso médio brasileiro
WITH var_peso_medio AS (SELECT avg(EXE.PESO) AS peso_medio FROM alistamento_exercito2022 EXE)

SELECT
  EST.Sigla AS Estados,
  count(*) AS Dentro_peso
FROM alistamento_exercito2022 EXE
INNER JOIN estados_brasileiro EST
ON EXE.UF_RESIDENCIA = EST.Sigla
CROSS JOIN var_peso_medio
WHERE EXE.PESO <= peso_medio
GROUP BY EST.Sigla
ORDER BY Dentro_peso DESC
LIMIT 3
```
Figura 7: Top 3 estados x Peso

![image](https://github.com/AlbertoFAraujo/SQL_EDA_exercito2022/assets/105552990/79b95d47-c789-4491-b202-6df98f8d4e26)

```SQL
-- Qual a distribuição de alistados por tipo de zona (Rural ou Urbana)?
SELECT
  EXE.ZONA_RESIDENCIAL AS Zona,
  count(*) AS Total,
  format_number(count(*)/(SELECT count(*) AS total_alistado FROM alistamento_exercito2022),"0.00%") AS Percent_total
FROM alistamento_exercito2022 EXE
GROUP BY EXE.ZONA_RESIDENCIAL
```
Figura 8: Zona x Alistados

![image](https://github.com/AlbertoFAraujo/SQL_EDA_exercito2022/assets/105552990/96cf6467-ae8e-477a-9251-9ab64aa0c98e)

```SQL
-- Analisar a faixa etária x gênero dos alistados

WITH ano_atual AS(SELECT year(getdate()) AS var_ano_atual)

SELECT
  CASE
    WHEN (var_ano_atual - EXE.ANO_NASCIMENTO) <= 28 THEN '18-28'
    WHEN (var_ano_atual - EXE.ANO_NASCIMENTO) > 28 AND (var_ano_atual - EXE.ANO_NASCIMENTO) <= 38 THEN '28-38'
    WHEN (var_ano_atual - EXE.ANO_NASCIMENTO) > 38 AND (var_ano_atual - EXE.ANO_NASCIMENTO) <= 48 THEN '38-48'
    WHEN (var_ano_atual - EXE.ANO_NASCIMENTO) > 48 AND (var_ano_atual - EXE.ANO_NASCIMENTO) <= 58 THEN '48-58'
    ELSE '58-100'
  END AS Faixa_etaria,
  SUM(CASE WHEN EXE.SEXO = "F" THEN 1 ELSE 0 END) AS Feminino,
  format_number(SUM(CASE WHEN EXE.SEXO = "F" THEN 1 ELSE 0 END) / (SELECT count(*) FROM alistamento_exercito2022 WHERE SEXO = "F"),"0.0000%") AS Perc_Feminino,
  SUM(CASE WHEN EXE.SEXO = "M" THEN 1 ELSE 0 END) AS Masculino,
  format_number(SUM(CASE WHEN EXE.SEXO = "M" THEN 1 ELSE 0 END) / (SELECT count(*) FROM alistamento_exercito2022 WHERE SEXO = "M"),"0.0000%") AS Perc_Masculino
FROM alistamento_exercito2022 EXE
CROSS JOIN ano_atual
GROUP BY Faixa_etaria
ORDER BY Faixa_etaria ASC
```
Figura 9: Faixa x Gênero

![newplot (3)](https://github.com/AlbertoFAraujo/SQL_EDA_exercito2022/assets/105552990/80e8f3bd-b7d0-4100-aaa0-04d605b4afd3)

```SQL
-- Qual a idade máxima dos alistados do gênero feminino?

SELECT
  max(year(getdate())- EXE.ANO_NASCIMENTO) AS Idade_Maxima
FROM alistamento_exercito2022 EXE
WHERE EXE.SEXO = "F"
```
| Idade_Maxima |
|--------------|
|       39     |

```SQL
-- Qual o peso médio dos alistados do gênero MASCULINO? e do FEMININO?
SELECT
  EXE.SEXO AS `Gênero`,
  round(avg(EXE.PESO),2) AS `Peso Médio`
FROM alistamento_exercito2022 EXE
GROUP BY EXE.SEXO
```
Figura 10: Peso Médio x Gênero

![image](https://github.com/AlbertoFAraujo/SQL_EDA_exercito2022/assets/105552990/e1b29fcb-983b-4014-b7a6-100c52fd1430)

```SQL
-- Dentre os pesos entre os gêneros, quantos estão acima do peso conforme o IMC?

/*
IMC:
Abaixo do peso: IMC abaixo de 18,5
Peso normal: IMC entre 18,5 e 24,9
Sobrepeso: IMC entre 25 e 29,9
Obesidade Classe I: IMC entre 30 e 34,9
Obesidade Classe II: IMC entre 35 e 39,9
Obesidade Classe III: IMC 40 ou acima
*/


SELECT
  CASE
    WHEN EXE.PESO/pow(EXE.ALTURA*0.01,2) <= 18.5 THEN 'Abaixo do peso'
    WHEN EXE.PESO/pow(EXE.ALTURA*0.01,2) > 18.5 AND EXE.PESO/pow(EXE.ALTURA*0.01,2) <= 24.9 THEN 'Peso Normal'
    WHEN EXE.PESO/pow(EXE.ALTURA*0.01,2) > 25 AND EXE.PESO/pow(EXE.ALTURA*0.01,2) <= 29.9 THEN 'Sobrepeso'
    WHEN EXE.PESO/pow(EXE.ALTURA*0.01,2) > 30 AND EXE.PESO/pow(EXE.ALTURA*0.01,2) <= 34.9 THEN 'Obsidade Classe 1'
    WHEN EXE.PESO/pow(EXE.ALTURA*0.01,2) > 35 AND EXE.PESO/pow(EXE.ALTURA*0.01,2) <= 39.9 THEN 'Obsidade Classe 2'
    ELSE 'Obsidade Classe 3'
  END AS indice_imc,
  count(*) AS Total,
  format_number(count(*)/(SELECT count(*) FROM alistamento_exercito2022),"0.00%") AS Percent_total
FROM alistamento_exercito2022 EXE
GROUP BY indice_imc
ORDER BY Total DESC
```
| indice_imc        | Total   | Percent_total |
|-------------------|---------|---------------|
| Obsidade Classe 3 | 724180  | 70.93%        |
| Peso Normal       | 206073  | 20.18%        |
| Sobrepeso         | 49725   | 4.87%         |
| Abaixo do peso    | 21907   | 2.15%         |
| Obsidade Classe 1 | 15396   | 1.51%         |
| Obsidade Classe 2 | 3646    | 0.36%         |

Figura 11: IMC (Índice de Massa Corporal)

![image](https://github.com/AlbertoFAraujo/SQL_EDA_exercito2022/assets/105552990/52ebf5c3-95e0-49a2-b94a-abef7ae3c73d)

```SQL
-- Relação entre Dispensa ou não com os gêneros:
SELECT
  EXE.DISPENSA AS Tipo,
  SUM(CASE WHEN EXE.SEXO = "F" THEN 1 ELSE 0 END) AS Feminino,
  format_number(SUM(CASE WHEN EXE.SEXO = "F" THEN 1 ELSE 0 END)/(SELECT count(*) FROM alistamento_exercito2022 EXE WHERE EXE.SEXO = "F"),"0.00%") AS Percent_fem,
  SUM(CASE WHEN EXE.SEXO = "M" THEN 1 ELSE 0 END) AS Masculino,
  format_number(SUM(CASE WHEN EXE.SEXO = "M" THEN 1 ELSE 0 END)/(SELECT count(*) FROM alistamento_exercito2022 EXE WHERE EXE.SEXO = "M"),"0.00%") AS Percent_mas
FROM alistamento_exercito2022 EXE
GROUP BY EXE.DISPENSA
```
Figura 12: Dispensa por Gênero

![image](https://github.com/AlbertoFAraujo/SQL_EDA_exercito2022/assets/105552990/95ab80fb-5e8d-4bec-a964-3ad46b721e05)

```SQL
-- Qual a altura média entre os alistados no geral?
SELECT round(avg(EXE.ALTURA),2) AS `Altura Média` FROM alistamento_exercito2022 EXE
```

| Altura Média |
|--------------|
|   174.41     |

```SQL
-- Altura média por Região
SELECT
  EST.`Região`,
  round(avg(EXE.ALTURA),2) AS `Altura Média`
FROM alistamento_exercito2022 EXE
LEFT JOIN estados_brasileiro EST
ON EXE.UF_RESIDENCIA = EST.Sigla
GROUP BY EST.`Região`
ORDER BY `Altura Média` DESC
```

Figura 13: Altura Média por Região

![image](https://github.com/AlbertoFAraujo/SQL_EDA_exercito2022/assets/105552990/6c21177f-f194-4d7e-802c-94de170b6a41)

```SQL
-- Distribuição de alistados x estado civil
SELECT
  EXE.ESTADO_CIVIL AS `Estado Civil`,
  count(*) AS Total,
  format_number(count(*)/(SELECT count(*) FROM alistamento_exercito2022),"0.000%") AS Percent_total
FROM alistamento_exercito2022 EXE
GROUP BY EXE.ESTADO_CIVIL
ORDER BY Total DESC
```
| Estado Civil           | Total   | Percent_total |
|------------------------|---------|---------------|
| Solteiro               | 1010987 | 99.026%       |
| Outros                 | 6627    | 0.649%        |
| Casado                 | 3184    | 0.312%        |
| Desquitado             | 42      | 0.004%        |
| Divorciado             | 40      | 0.004%        |
| Separado Judicialmente | 35      | 0.003%        |
| Viúvo                  | 12      | 0.001%        |

## PARECER FINAL DA EXPLORAÇÃO DOS DADOS

1. Em 2022 o número de alistados foi de 1.020.927 pessoas;
2. Desse total de alistados, 1.020.848 (99,992%) são do gênero masculino e apenas 79 (0,008%) do gênero feminino;
3. A distribuição do gênero feminino está mais concentrada na região Nordeste do Brasil, sendo 39,97% do total de 79 alistadas, em sequência o Sudeste com 30,38%;
4. A distribuição do gênero masculino está mais concentrada na região Sudeste do Brasil, sendo 44,04% do total de 1.020.848 alistados, em sequência o Nordeste com 23,26%;
5. A região do sudeste possui o maior número de alistados em 2022, equivalente a 44,85% do total geral alistado. A região Centro possui o menor número de alistados, equivalente a 7,56%;
6. O top 3 estados com maiores números de alistados são: RJ(32926), SP(24708) e RS(22344);
7. 14,17% dos alistados da região Nordeste possuem número de calçado maior ou igual a média do calçado brasileiro(40,95 cm);
8. 0,15% (684) do total alistados na região sudeste (457.845) possuem Ensino Superior Completo;
9. Os estados RJ, SP e RS possuem o maior número de alistados dentro do peso médio brasileiro;
10. A faixa etária predominante dos alistados é entre 18 e 28 anos, sendo equivalente a 98,73% no gênero feminino e 99,98 no gênero masculino. Alguns outliers foram identificados, somente no gênero masculino, como por exemplo pelo menos 6 alistados com faixa etária de 58 a 100 anos;
11. No gênero feminino, a maior idade registrada foi de 39 anos;
12. O peso médio do gênero masculino é de 70,13kg e do feminino 63,05kg;
13. Dentre os alistados, nota-se a predominância de IMC = Obsidade Classe 3 em 70,93% do total geral alistado;
14. Para o gênero feminino, 65,82% do total de 79 alistadas foram dispensadas, o restante 34,18% não;
15. Para o gênero masculino, 89,31% do total de 1.020.848 alistados foram dispensados, o restante 10,69% não;
16. A altura média dos alistados é de 174,41 cm;
17. Os alistados residentes no exterior possuem o maior registro de altura média, sendo de 182,78 cm. Já dentre as regiões do Brasil, o Sudeste apresenta o maior índice da altura média, equivalente a 175,21. O norte possui a menor média, 172,13cm;
18. Em relação ao estado civil dos alistados, foi registrado que 99,026% são solteiros.





