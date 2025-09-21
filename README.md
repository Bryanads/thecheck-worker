# Previsão de Ondas e Marés

Este repositório contém scripts para buscar, processar e armazenar dados de previsão de ondas e marés de múltiplos spots de surf.

---

## Como Executar o Script

Para buscar e inserir os dados de previsão no banco de dados, utilize o seguinte comando no seu terminal, passando os IDs dos spots desejados como argumentos.

**Comando:**
```bash
python3 -m src.forecast.fetch_and_insert_all <spot_id_1> <spot_id_2> ...
````

**Exemplo:**

```bash
python3 -m src.forecast.fetch_and_insert_all 1 2 3 4 5
python3 -m src.forecast.fetch_and_insert_all 6 7 8 9 10
python3 -m src.forecast.fetch_and_insert_all 11 12 13 14 15
```

> **⚠️ Nota sobre a API:**
> A chave de API gratuita tem um limite de requisições. Recomenda-se processar no **máximo 3 spots por vez**. Se precisar de mais, altere a chave de API no arquivo `config.py` antes de continuar.

-----

## Consultas SQL Úteis

Aqui estão algumas queries úteis para analisar os dados armazenados no banco de dados.

### 1\. Visão Geral dos Spots

Esta query resume o status de todos os spots, mostrando os dias para os quais há previsão e a data da última atualização.

```sql
SELECT
    s.spot_id AS "ID",
    s.spot_name AS "Nome do Spot",
    STRING_AGG(
        DISTINCT TO_CHAR(f.timestamp_utc, 'DD/MM'),
        ' - '
        ORDER BY TO_CHAR(f.timestamp_utc, 'DD/MM')
    ) AS "Dias com Previsão",
    MAX(TO_CHAR(f.last_modified_at, 'DD/MM HH24:MI')) AS "Dados Inseridos/Atualizados em (UTC): "
FROM
    public.spots s
JOIN
    public.forecasts f ON s.spot_id = f.spot_id
GROUP BY
    s.spot_id,
    s.spot_name
ORDER BY
    s.spot_id;
```

### 2\. Detalhes Diários por Spot

Esta query mostra o intervalo de horas de previsão (primeira e última) para cada dia de um **spot específico**.

**Lembre-se de alterar o `spot_id` na cláusula `WHERE`** para o spot que deseja consultar.

```sql
SELECT
    s.spot_name AS "Nome do Spot",
    TO_CHAR(f.timestamp_utc, 'DD/MM') AS "Data da Previsão",
    TO_CHAR(MIN(f.timestamp_utc), 'HH24:MI') AS "Primeira Previsão do Dia (UTC)",
    TO_CHAR(MAX(f.timestamp_utc), 'HH24:MI') AS "Última Previsão do Dia (UTC)",
    TO_CHAR(MAX(f.last_modified_at), 'DD/MM') AS "Dados Atualizados em (UTC)"
FROM
    public.spots s
JOIN
    public.forecasts f ON s.spot_id = f.spot_id
WHERE
    s.spot_id = 1 -- <<<<<< Altere o ID do spot aqui
GROUP BY
    s.spot_name,
    "Data da Previsão"
ORDER BY
    "Data da Previsão";
```

```
