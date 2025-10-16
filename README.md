# TheCheck Worker

Este repositório contém o serviço de worker para o TheCheck. Sua responsabilidade é executar tarefas agendadas e pesadas em segundo plano, incluindo:

1.  **Coleta de Dados:** Buscar os dados de previsão de tempo e maré da API da Stormglass para todos os spots cadastrados.
2.  **Cálculo de Recomendações:** Pré-calcular os scores e as recomendações personalizadas para cada usuário, armazenando-as em cache para acesso rápido pela API principal.
3.  **Manutenção:** Limpar dados de previsão antigos do banco de dados.

-----

## Configuração

Antes de executar o worker, é necessário configurar as variáveis de ambiente.

1.  **Crie um arquivo `.env`** na raiz do projeto.

2.  **Adicione as seguintes variáveis** ao arquivo, preenchendo com suas credenciais:

    ```env
    # Credenciais do Banco de Dados
    DB_USER="seu_usuario_do_banco"
    DB_PASSWORD="sua_senha_do_banco"
    DB_HOST="seu_host_do_banco"
    DB_PORT="5432"
    DB_NAME="postgres"

    # Chaves da API Stormglass (separadas por vírgula, sem espaços)
    STORMGLASS_API_KEYS="SUA_CHAVE_1,SUA_CHAVE_2,SUA_CHAVE_3"
    ```

    > **⚠️ Nota sobre as chaves de API:**
    > O worker foi projetado para rotacionar as chaves de API automaticamente, distribuindo as requisições entre elas para evitar exceder os limites.

## Como Executar o Worker

Com o arquivo `.env` configurado, siga os passos abaixo:

1.  **Instale as dependências:**

    ```bash
    pip install -r requirements.txt
    ```

2.  **Execute o worker:**
    O comando abaixo iniciará o ciclo completo de atualização e cálculo. O script processará **todos os spots** encontrados no banco de dados automaticamente.

    ```bash
    python3 -m src.main_worker
    ```

Este é o mesmo comando que deve ser usado para configurar um **Cron Job** em serviços de nuvem como o Render.

-----

## Consultas SQL Úteis

Aqui estão algumas queries úteis para analisar os dados armazenados no banco de dados.

### 1\. Visão Geral dos Spots

Esta query resume o status de todos os spots, mostrando os dias para os quais há previsão e a data da última atualização.

```sql
SELECT
    s.spot_id AS "ID",
    s.name AS "Nome do Spot",
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
    s.name
ORDER BY
    s.spot_id;
```

### 2\. Detalhes Diários por Spot

Esta query mostra o intervalo de horas de previsão (primeira e última) para cada dia de um **spot específico**.

**Lembre-se de alterar o `spot_id` na cláusula `WHERE`** para o spot que deseja consultar.

```sql
SELECT
    s.name AS "Nome do Spot",
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
    s.name,
    "Data da Previsão"
ORDER BY
    "Data da Previsão";
```