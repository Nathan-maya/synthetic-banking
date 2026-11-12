# Synthetic Banking Lakeflow Pipeline

Portfólio técnico de um pipeline batch no Databricks: oito fontes Parquet, camadas Bronze/Silver, quarentena observável e duas tabelas Gold.

## Arquitetura

- Bronze ingere customers, transactions, leasing_agreements, life_insurance, property_insurance, pension, margin_assets e margin_liabilities com Auto Loader.
- Silver publica apenas registros válidos.
- quarantine_<fonte> conserva registros rejeitados, regra, arquivo e timestamp.
- Gold publica gold_monthly_transaction_kpis e gold_customer_product_360.
- DAB descreve pipeline, Volume UC e alvos dev/full/prod.

## Amostra e execução

Instale dependências com `uv sync --dev`. Gere uma amostra pequena sem versionar Parquet:

```powershell
uv run python scripts/prepare_sample.py C:\Users\Nathan\Downloads\synthetic_banking\student_export
```

O alvo `dev` usa o catálogo `org`, schema `default` e cria o Volume `/Volumes/org/default/synthetic_banking_landing`. O script gera até 5.000 linhas por fonte de negócio e inclui em customers os clientes referenciados pelas sete amostras, para preservar a cobertura de chaves estrangeiras. Envie cada arquivo de `data/sample` para sua subpasta homônima em `/Volumes/org/default/synthetic_banking_landing/sample/`. Timestamps nanosecond são reduzidos para microseconds; Bronze guarda campos de data como texto e Silver converte para DATE. O Volume `org.default.syntheticbanking` contém o export integral existente; ele não é usado nem alterado pelo alvo `dev`.

```powershell
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev
databricks bundle run synthetic_banking_etl -t dev
```

Para o lote integral, envie os oito arquivos completos sob `/full/` e publique no alvo `full` somente quando houver capacidade de compute e o isolamento do destino estiver configurado. Não envie dados completos ao Git.

Configure DATABRICKS_HOST e DATABRICKS_CLIENT_ID como variáveis do ambiente GitHub dev e habilite workload identity federation para o service principal. Pull requests validam lint e bundle; merge em main publica em dev. O deploy prod continua manual até validar isolamento e permissões.

## Pull requests

O workflow `.github/workflows/ci.yml` é executado em pull requests para `main`. Configure no ambiente GitHub `dev` `DATABRICKS_HOST` e `DATABRICKS_CLIENT_ID` como Variables ou Secrets, além da relação de confiança OIDC para o repositório. O job de validação executa `uv sync --locked`, Ruff, pytest e `databricks bundle validate`. Depois do merge, o job `deploy-dev` publica o bundle e aplica a wheel do projeto ao ambiente da pipeline serverless.

Use uma branch curta para cada mudança e preencha o checklist em `.github/pull_request_template.md`. O upload dos Parquet completos e a execução do alvo `full` ficam fora do fluxo de pull request.
