# DESIGN: Transaction Pipeline

> Desenho técnico para demonstrar um pipeline bancário sintético com Lakeflow, DAB, GitHub Actions e qualidade rastreável.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | TRANSACTION_PIPELINE |
| **Date** | 2026-11-11 |
| **Author** | design-agent |
| **DEFINE** | [DEFINE_TRANSACTION_PIPELINE.md](./DEFINE_TRANSACTION_PIPELINE.md) |
| **Status** | ✅ Complete (Built) |

## Architecture Overview

O desenho usa um único pipeline Lakeflow, executado em modo triggered e serverless. Um Unity Catalog Volume recebe os arquivos de entrada; os alvos DAB separam a amostra de desenvolvimento do lote integral. O catálogo e os schemas seguem o bundle existente. As tabelas ficam no schema do alvo com prefixos por camada para manter o MVP simples.

    Exportação Parquet local (fora do Git)
      ├── script cria amostra pequena ── upload ──┐
      └── exportação integral ──────── sob demanda┤
                                                   ▼
                                  Unity Catalog Volume /sample ou /full
                                                   │
                                     Auto Loader, 8 fontes
                                                   ▼
                                  Bronze: 8 streaming tables
                                                   │
                             validação privada + métricas por regra
                                  ┌────────────────┴────────────────┐
                                  ▼                                 ▼
                       Silver: 8 tabelas válidas          Quarentena: 8 tabelas
                                  │
                                  ▼
                       Gold: 2 materialized views
                        ├── KPIs mensais de transações
                        └── Cliente e produtos 360

    GitHub ── pull request ── lint + pytest de amostra + bundle validate
       └──── merge em main ── deploy DAB em dev
                     workflow_dispatch ── execução integral no alvo full

O pipeline preserva as linhas originais em Bronze. Uma validação privada calcula regras e métricas; a saída válida alimenta Silver e a saída inválida segue para quarentena. Cada tabela de quarentena mantém os campos originais, o arquivo de origem, os nomes das regras violadas e o timestamp da rejeição. As métricas das expectativas ficam no event log do pipeline.

**Confiança do desenho: 0.95.** Há padrões internos e agentes especialistas correspondentes para Lakeflow, medallion, qualidade, Spark, testes e CI/CD. A arquitetura segue Auto Loader, tabelas declarativas modernas, materialized views para transformações batch e DAB para deploy. As decisões de expectativas, quarentena e autenticação foram conferidas na documentação oficial atual do Databricks: [Auto Loader schema evolution](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema), [expectations e quarentena](https://docs.databricks.com/aws/en/ldp/expectations), [materialized views](https://docs.databricks.com/aws/en/ldp/materialized-views) e [DAB Volume resources](https://docs.databricks.com/aws/en/dev-tools/bundles/resources).

## Components

| Component | Purpose | Technology |
|-----------|---------|------------|
| Landing | Receber a amostra e os arquivos integrais, sem versionar dados completos | Unity Catalog managed Volume |
| Bronze | Ingerir os oito Parquet uma vez, preservando schema e linhagem | Lakeflow streaming tables + Auto Loader |
| Validação privada | Aplicar regras, emitir métricas e separar os registros inválidos | Lakeflow private materialized views + expectations |
| Silver | Converter tipos, padronizar campos e publicar somente linhas válidas | Lakeflow materialized views |
| Quarentena | Preservar as linhas rejeitadas por fonte, regra e caminho de origem | Oito Lakeflow materialized views |
| Gold | Publicar os dois produtos analíticos pedidos | Lakeflow materialized views batch |
| Deploy e CI | Validar bundle, lint e testes pequenos; publicar alterações em dev | DAB, GitHub Actions, uv, pytest, Ruff |
| Preparação de amostra | Extrair quantidade limitada e reproduzível de cada Parquet para teste/dev | Script local Python + PyArrow |

## Key Decisions

### Decision 1: Lakeflow triggered em um DAB existente

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-11-11 |

**Context:** O projeto já possui DAB, pipeline Python serverless e targets dev/prod, mas as transformações são exemplos de NYC Taxi. A exportação é histórica, chega em arquivos Parquet e não tem exigência de baixa latência.

**Choice:** Substituir o pipeline de exemplo por um pipeline Lakeflow Python triggered. Usar Auto Loader para as oito tabelas Bronze; materialized views batch para qualidade, Silver e Gold. Manter o bundle atual, acrescentar um Volume e um alvo full isolado em schema próprio.

**Rationale:** Auto Loader controla checkpoint e descoberta incremental de arquivos no pipeline. Um update triggered processa os novos arquivos sem deixar compute contínuo ligado. Materialized views combinam com a limpeza e as agregações históricas e deixam o Lakeflow manter o grafo de dependências. O modo serverless já é o padrão do recurso existente. Como expectations requerem edição Advanced, o recurso deve declarar edition ADVANCED; [a referência de configuração do pipeline](https://docs.databricks.com/aws/en/ldp/configure-pipeline) confirma essa exigência para serverless e quality expectations.

**Alternatives Rejected:**
1. Jobs Spark escritos manualmente - duplicariam o controle de checkpoint e o grafo de dependências que Lakeflow já oferece.
2. Pipeline continuous ou CDC - não há eventos novos, fonte CDC nem SLA de minutos; execução contínua aumentaria custo sem atender um requisito.
3. Criar outro framework de orquestração - não agrega valor ao escopo enxuto nem ao objetivo de mostrar Lakeflow.

**Consequences:**
- O estado de Auto Loader fica associado ao pipeline e à raiz do alvo; não se deve mover a entrada de um alvo para o outro.
- Bronze preserva os arquivos lidos; a mesma entrada não é adicionada novamente quando o checkpoint continua intacto.
- O alvo full escreve em schema e estado próprios para que a execução pesada não sobrescreva as tabelas de amostra.
- O volume integral requer upload manual para o Volume e uma execução manual do alvo full.

---

### Decision 2: Quarentena explícita antes de Silver

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-11-11 |

**Context:** O requisito pede que cada rejeição seja rastreável, que Silver só contenha linhas válidas e que métricas sejam consultáveis. Expectation DROP remove linhas do destino e registra contagens; por si só não preserva os registros rejeitados na quarentena.

**Choice:** Para cada fonte, criar uma materialized view privada que mantenha todas as linhas recebidas, válidas ou inválidas, e aplique expectations em modo WARN para medir cada regra. Dela saem duas branches: Silver filtra o conjunto de regras aprovadas e seleciona as colunas conformadas; quarentena filtra as rejeições e adiciona _failed_rules, _source_file e _quarantined_at. Regras locais usam os mesmos predicados na validação e na branch de quarentena. Unicidade e referências a clientes são calculadas como flags por agregação/join antes das expectations.

**Rationale:** A branch privada torna os valores de qualidade disponíveis às duas saídas. WARN registra métricas por regra e update sem interromper o pipeline; o filtro em Silver garante que uma linha rejeitada não avance. A branch de quarentena preserva a linha e permite agrupar por fonte e regra. Esse arranjo segue o padrão Databricks de dividir registros válidos e inválidos em fluxos separados, descrito em [expectation patterns](https://docs.databricks.com/aws/en/ldp/expectation-patterns).

**Alternatives Rejected:**
1. Usar apenas expect_all_or_drop em Silver - produziria contagem de descarte, mas não deixaria os dados inválidos consultáveis na quarentena.
2. Usar apenas WARN em Silver - manteria registros inválidos junto dos válidos.
3. Unir as oito quarentenas em uma tabela sem normalizar schema - os campos das fontes são diferentes e isso exigiria conversões extras. O MVP mantém uma quarentena por fonte.

**Consequences:**
- Cada fonte possui tabela Silver e tabela de quarentena com schema conhecido.
- O event log contém contagens por expectation e update; as tabelas de quarentena permitem detalhar linhas e contar falhas por nome de regra.
- Verificações de chaves únicas e chaves estrangeiras exigem agregação/join. Esse custo é medido no primeiro lote integral.
- Uma alteração em uma regra deve mudar a validação privada, o filtro Silver e a branch de quarentena na mesma mudança.

## File Manifest

| # | File | Action | Purpose | Agent | Dependencies |
|---|------|--------|---------|-------|--------------|
| 1 | databricks.yml | Modify | Incluir configuração landing e alvo full com schema isolado; manter prod manual | @lakeflow-architect | None |
| 2 | resources/synthetic_banking_etl.pipeline.yml | Modify | Configurar execução triggered, serverless, edition Advanced, event log e raiz das transformações | @lakeflow-architect | 1, 3, 10-22 |
| 3 | resources/synthetic_banking_landing.volume.yml | Create | Declarar Volume gerenciado do Unity Catalog para os Parquet | @lakeflow-architect | 1 |
| 4 | .github/workflows/ci.yml | Create | Rodar lint, testes pequenos, validação DAB, deploy em merge e execução full manual | @ci-cd-specialist | 1-6, 23-25 |
| 5 | pyproject.toml | Modify | Remover entrypoint Taxi/dependência DLT antiga e declarar PyArrow para preparar amostra | @python-developer | 12, 23-25 |
| 6 | uv.lock | Modify | Refletir as dependências do pyproject.toml | @python-developer | 5 |
| 7 | .gitignore | Create | Ignorar Parquet de amostra/integral gerados localmente e arquivos locais de deploy | (general) | 23 |
| 8 | README.md | Modify | Documentar arquitetura, preparação/upload de amostra e execução manual de volume integral | (general) | 1-7, 9, 23 |
| 9 | src/synthetic_banking_etl/README.md | Modify | Documentar diretórios, nomes de tabelas e regras de camada | @lakeflow-pipeline-builder | 10-22 |
| 10 | src/synthetic_banking_etl/__init__.py | Create | Tornar o código ETL um pacote importável pelo ambiente do pipeline e pelos testes | @python-developer | 5 |
| 11 | src/synthetic_banking_etl/transformations/__init__.py | Create | Tornar módulos de transformações importáveis | @python-developer | 10 |
| 12 | src/synthetic_banking_etl/quality_rules.py | Create | Declarar contratos por fonte e helpers compartilhados de flags, falhas e quarentena | @data-quality-analyst | 10 |
| 13 | src/synthetic_banking_etl/transformations/customers.py | Create | Bronze, validação privada, Silver e quarentena de clientes | @lakeflow-pipeline-builder | 2, 11, 12 |
| 14 | src/synthetic_banking_etl/transformations/transactions.py | Create | Bronze, validação privada, Silver e quarentena de transações | @lakeflow-pipeline-builder | 2, 11-13 |
| 15 | src/synthetic_banking_etl/transformations/leasing_agreements.py | Create | Bronze, validação privada, Silver e quarentena de leasing | @lakeflow-pipeline-builder | 2, 11-13 |
| 16 | src/synthetic_banking_etl/transformations/life_insurance.py | Create | Bronze, validação privada, Silver e quarentena de seguro de vida | @lakeflow-pipeline-builder | 2, 11-13 |
| 17 | src/synthetic_banking_etl/transformations/property_insurance.py | Create | Bronze, validação privada, Silver e quarentena de seguro patrimonial | @lakeflow-pipeline-builder | 2, 11-13 |
| 18 | src/synthetic_banking_etl/transformations/pension.py | Create | Bronze, validação privada, Silver e quarentena de pensão | @lakeflow-pipeline-builder | 2, 11-13 |
| 19 | src/synthetic_banking_etl/transformations/margin_assets.py | Create | Bronze, validação privada, Silver e quarentena de saldos de ativo | @lakeflow-pipeline-builder | 2, 11-13 |
| 20 | src/synthetic_banking_etl/transformations/margin_liabilities.py | Create | Bronze, validação privada, Silver e quarentena de saldos de passivo | @lakeflow-pipeline-builder | 2, 11-13 |
| 21 | src/synthetic_banking_etl/transformations/gold/monthly_transaction_kpis.py | Create | Gerar Gold mensal por canal, tipo, segmento e direção | @lakeflow-pipeline-builder | 14, 13 |
| 22 | src/synthetic_banking_etl/transformations/gold/customer_product_360.py | Create | Gerar Gold com uma linha por cliente válido e resumos de produto/transação | @lakeflow-pipeline-builder | 13-20 |
| 23 | scripts/prepare_sample.py | Create | Gerar subset determinístico dos oito Parquet para o Volume de dev sem copiar o conjunto completo | @python-developer | 5, 7 |
| 24 | tests/test_quality_rules.py | Create | Cobrir regras, linha rejeitada, origem e separação entre válido e quarentena | @data-quality-analyst | 12-20, 23 |
| 25 | tests/test_gold_outputs.py | Create | Cobrir reconciliação mensal e unicidade/grão da Gold cliente-produto | @lakeflow-pipeline-builder | 21-22 |
| 26 | resources/sample_job.job.yml | Delete | Remover Job de exemplo sem função no pipeline de portfólio | (general) | 2 |
| 27 | src/sample_notebook.ipynb | Delete | Remover notebook de exemplo desconectado do pipeline | (general) | 8 |
| 28 | src/synthetic_banking/__init__.py | Delete | Remover pacote de exemplo Taxi substituído pelo pacote ETL | (general) | 5 |
| 29 | src/synthetic_banking/main.py | Delete | Remover entrypoint Taxi sem uso no Lakeflow | (general) | 5 |
| 30 | src/synthetic_banking/taxis.py | Delete | Remover leitura e análise de NYC Taxi | (general) | 8 |
| 31 | tests/sample_taxis_test.py | Delete | Remover teste de exemplo Taxi | (general) | 24-25 |
| 32 | src/synthetic_banking_etl/transformations/sample_trips_synthetic_banking.py | Delete | Remover transformação Taxi de trips | (general) | 13-22 |
| 33 | src/synthetic_banking_etl/transformations/sample_zones_synthetic_banking.py | Delete | Remover transformação Taxi de zones | (general) | 13-22 |
| 34 | src/synthetic_banking_etl/explorations/sample_exploration.ipynb | Delete | Remover exploração de exemplo Taxi | (general) | 9 |

**Total Files:** 34

## Agent Assignment Rationale

> Agents discovered from AgentSpec agents directory. As atribuições são roteamento planejado para a fase Build.

| Agent | Files Assigned | Why This Agent |
|-------|----------------|----------------|
| @lakeflow-architect | 1-3 | Especialista em arquitetura Lakeflow, DAB, UC e alvos de pipeline |
| @lakeflow-pipeline-builder | 9, 13-22, 25 | Especialista em tabelas Bronze/Silver/Gold, expectativas e implementação Lakeflow |
| @data-quality-analyst | 12, 24 | Especialista em regras de qualidade, contratos e evidência de falhas |
| @ci-cd-specialist | 4 | Especialista em GitHub Actions, DAB e autenticação para deploy |
| @python-developer | 5-6, 10-11, 23 | Especialista em pacote Python, dependências e ferramentas locais |
| (general) | 7-8, 26-34 | Convenções de repositório, documentação e remoção de artefatos de exemplo |

**Agent Discovery:**
- Scanned: AgentSpec agents/**/*.md
- Matched by: tipo de arquivo, finalidade, caminhos do projeto e domínios KB
- Confidence: 0.95 — padrões de KB e agentes especialistas encontrados

## Code Patterns

### Pattern 1: Ingestão Auto Loader preservando origem

Cada arquivo-fonte declara a streaming table Bronze correspondente. O caminho vem da configuração do pipeline e cada tabela lê uma pasta própria no Volume. O checkpoint e o schema state são gerenciados pelo Lakeflow; não definir cloudFiles.schemaLocation manualmente.

    from pyspark import pipelines as dp
    from pyspark.sql import functions as F

    def _source_path(dataset: str) -> str:
        root = spark.conf.get("landing_path").rstrip("/")
        return f"{root}/{dataset}/"

    @dp.table(name="bronze_customers", comment="Parquet original com linhagem de ingestão")
    def bronze_customers():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("rescuedDataColumn", "_rescued_data")
            .load(_source_path("customers"))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )

Schema Parquet é inferido no primeiro processamento. O modo rescue mantém dados de campos novos, diferenças de tipo e diferenças de caixa em _rescued_data, sem evoluir silenciosamente o contrato; registros com conteúdo resgatado são encaminhados à quarentena até revisão do schema. Não configurar partições Hive no Volume. Referência: [Auto Loader schema evolution](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema).

### Pattern 2: Métricas, branch válida e quarentena

quality_rules.py define predicados nomeados por fonte. A materialized view privada mantém todas as linhas e aplica expectations em modo WARN para medir cada regra. Dela saem duas branches: Silver filtra o conjunto de regras aprovadas e seleciona as colunas conformadas; quarentena filtra as rejeições e adiciona _failed_rules, _source_file e _quarantined_at. Regras locais usam os mesmos predicados na validação e na branch de quarentena. Unicidade e referências a clientes são calculadas como flags antes das expectations.

    from pyspark import pipelines as dp
    from pyspark.sql import functions as F
    from synthetic_banking_etl.quality_rules import add_quality_flags

    CUSTOMER_RULES = {
        "customer_id_required": "client_id IS NOT NULL",
        "gender_domain": "gender_cd IN ('M', 'F', 'U')",
        "registration_date_order": (
            "birth_or_reg_dt IS NOT NULL AND bank_reg_dt IS NOT NULL "
            "AND birth_or_reg_dt <= bank_reg_dt"
        ),
        "customer_id_unique": "_dq_duplicate_key IS NULL",
        "schema_not_rescued": "_rescued_data IS NULL",
    }

    @dp.materialized_view(name="quality_customers", private=True)
    @dp.expect_all(CUSTOMER_RULES)
    def quality_customers():
        return add_quality_flags(
            spark.read.table("bronze_customers"),
            CUSTOMER_RULES,
            duplicate_keys=["client_id"],
        )

    @dp.materialized_view(name="silver_customers")
    def silver_customers():
        return (
            spark.read.table("quality_customers")
            .where("_dq_valid = true")
            .select("client_id", "segment", "type_cd", "gender_cd", "region_cd",
                    "birth_or_reg_dt", "bank_reg_dt", "_source_file", "_ingested_at")
        )

    @dp.materialized_view(name="quarantine_customers")
    def quarantine_customers():
        return (
            spark.read.table("quality_customers")
            .where("_dq_valid = false")
            .withColumn("_quarantined_at", F.current_timestamp())
        )

add_quality_flags também marca chaves duplicadas e referências de cliente inexistentes antes da branch. Produtos e transações consultam silver_customers para validar client_id_fk; clientes é o único nó de referência, então o grafo não tem ciclo. Regras de domínio devem refletir DATA_DICTIONARY.html e o schema físico conferido dos Parquet. Anomalia gender_cd = X é rejeitada deliberadamente. Valores monetários são convertidos para DECIMAL(18,2); a validação de conversão FX é limitada às linhas em EUR porque não há tabela de câmbio entre as oito fontes.

### Pattern 3: Configuration Structure

    resources:
      volumes:
        synthetic_banking_landing:
          catalog_name: ${var.catalog}
          schema_name: ${var.schema}
          name: synthetic_banking_landing
          volume_type: MANAGED
      pipelines:
        synthetic_banking_etl:
          catalog: ${var.catalog}
          schema: ${var.schema}
          serverless: true
          continuous: false
          edition: ADVANCED
          configuration:
            landing_path: ${var.landing_path}
          event_log:
            catalog: ${var.catalog}
            schema: ${var.schema}
            name: synthetic_banking_etl_event_log

O DAB atual inclui resources/*.yml, então o novo Volume é incluído pelo bundle. O alvo dev aponta para /sample, o alvo full usa schema e estado próprios e aponta para /full. prod continua com deploy manual após revisão do workspace, identidade e permissões.

## Data Flow

    1. scripts/prepare_sample.py lê os oito Parquet locais e produz subconjuntos limitados em data/sample/; a pasta fica ignorada pelo Git.
       │
       ▼
    2. O usuário ou operador envia os oito arquivos para as pastas por fonte no UC Volume do alvo escolhido.
       │
       ▼
    3. Um update triggered do Lakeflow usa Auto Loader e grava bronze_<fonte>, incluindo _source_file e _ingested_at.
       │
       ▼
    4. quality_<fonte> valida campos, tipos, chaves e relações; expectations WARN publicam métricas no event log.
       │
       ├── _dq_valid = true  → silver_<fonte>, tipos conformados e colunas explícitas
       └── _dq_valid = false → quarantine_<fonte>, linha original e _failed_rules
       │
       ▼
    5. Gold mensal agrega transações válidas com customer segment; Gold 360 agrega transações e os snapshots mais recentes por cliente.
       │
       ▼
    6. GitHub Actions testa mudanças em PR, publica em dev no merge e permite disparar o alvo full manualmente.

## Integration Points

| External System | Integration Type | Authentication |
|-----------------|-----------------|----------------|
| Unity Catalog Volume | Arquivos Parquet sob /Volumes/...; uma pasta por fonte | Permissões UC do operador e da identidade de execução |
| Unity Catalog | Tabelas Delta gerenciadas, pipeline event log e schemas do bundle | Identidade do pipeline, com permissões no catálogo/schema/Volume |
| GitHub | Repositório Git, pull requests e workflow_dispatch | Credenciais da conta GitHub |
| GitHub Actions → Databricks | Databricks CLI e bundle validate/deploy/run | OAuth workload identity federation (OIDC) para service principal |
| Exportação local | Leitura Parquet para criar subset dev e upload manual do volume integral | Acesso local à pasta; nenhum segredo ou arquivo completo no Git |

O checkout atual não tem diretório .git nem workflow GitHub. Criar/conectar o repositório remoto e configurar a política OIDC do service principal é pré-requisito operacional para que Actions publique. O workflow usa variáveis DATABRICKS_HOST e DATABRICKS_CLIENT_ID; não grava token pessoal. A documentação recomenda OIDC e DAB para CI/CD: [Databricks CI/CD](https://docs.databricks.com/aws/en/dev-tools/ci-cd/), [GitHub Actions](https://docs.databricks.com/aws/en/dev-tools/ci-cd/github) e [federation para GitHub Actions](https://docs.databricks.com/aws/en/dev-tools/auth/provider-github).

## Testing Strategy

| Test Type | Scope | Files | Tools | Coverage Goal |
|-----------|-------|-------|-------|---------------|
| Unit/integration leve | Regras de qualidade, flags de integridade, quarentena e tabelas Silver com DataFrames pequenos | tests/test_quality_rules.py | pytest + fixture Spark de Databricks Connect; dados criados em memória | Cobrir regras por fonte e garantir que inválidos não passem |
| Unit/integration leve | Grão Gold, ausência de fanout e reconciliação por mês/direção | tests/test_gold_outputs.py | pytest + Databricks Connect serverless | Somatórios por mês/C-D iguais a Silver à precisão de centavos; chave cliente sem duplicação |
| CI em pull request | Ruff, testes pequenos e validação do bundle dev | .github/workflows/ci.yml | GitHub Actions, uv, Ruff, pytest, Databricks CLI | Falha em qualquer etapa bloqueia o workflow |
| Smoke em dev | Oito Parquet da amostra no Volume, contagem Bronze igual ao footer, atualização sem erro | Pipeline Lakeflow no target dev | DAB + Databricks UI/CLI | Uma execução reproduzível sobre o path /sample |
| Idempotência | Executar duas vezes a mesma amostra sem mudar arquivos nem checkpoint | Pipeline Lakeflow no target dev | DAB + contagens Delta | Segunda execução não duplica linhas |
| Execução integral | Upload manual dos oito arquivos completos e run do alvo full | Pipeline Lakeflow no target full | GitHub workflow_dispatch ou Databricks CLI | Status final, contagens, duração e métricas consultáveis |

Mapa para os critérios de aceite:

| Acceptance Test | Evidence |
|-----------------|----------|
| AT-001 | Smoke em dev confere as oito tabelas Bronze com os footers dos oito Parquet enviados |
| AT-002 | Testes fornecem linhas inválidas conhecidas; verificam fonte, regra e ausência em Silver; event log e quarentena mostram contagens |
| AT-003 | Duas atualizações da mesma entrada mantêm contagens estáveis no checkpoint do alvo |
| AT-004 | Teste reconcilia valores mensais por direção e confirma uma chave por cliente na Gold 360 |
| AT-005 | Execução GitHub em PR deve concluir Ruff, pytest de amostra e databricks bundle validate -t dev; merge publica em dev |
| AT-006 | workflow_dispatch com alvo full processa os oito arquivos integrais e deixa status, duração e contagens disponíveis |

Os testes do CI usam poucos registros em memória e não carregam o export integral. O conftest.py atual abre Databricks Connect durante pytest; o job de CI precisa de workload identity federation, acesso ao workspace dev e compute serverless disponível. Para PRs externos sem OIDC, o repositório deverá restringir a execução autenticada ou separar testes locais sem Spark da integração autenticada.

## Error Handling

| Error Type | Handling Strategy | Retry? |
|------------|-------------------|--------|
| Arquivo Parquet ilegível, ausente ou pasta de fonte incompleta | Update falha com o erro visível; corrigir upload ou schema e executar novamente | Sim, após corrigir a entrada |
| Coluna nova, tipo divergente ou caixa diferente | Auto Loader rescue guarda o conteúdo em _rescued_data; Bronze conserva a linha e a regra envia à quarentena até atualizar contrato | Sim, após decisão de schema |
| Falha de regra de qualidade | Expectation em WARN registra contagem; Silver filtra; quarentena mantém registros, regra e arquivo | Não requer retry para dados inválidos |
| Falha temporária de compute/serviço | Deixar o update triggered terminar ou reiniciar pelo Lakeflow depois de verificar status | Sim, por nova execução |
| Erro de autenticação OIDC ou permissão de Volume/schema | Falhar o job antes do deploy/run; corrigir federation policy ou grants UC | Sim, após corrigir a configuração |
| Divergência entre dicionário e arquivo | Usar footer/schema físico do Parquet como baseline atual e registrar a divergência; não forçar contagem do HTML | Não; resolver a origem antes de declarar reconciliação do dataset total |

## Configuration

| Config Key | Type | Default | Description |
|------------|------|---------|-------------|
| catalog | string | banking | Catálogo já definido no DAB atual |
| schema | string | dev: usuário atual; full: schema próprio; prod: prod | Isola tabelas, Volume e event log por alvo |
| landing_path | string | dev: Volume/sample; full/prod: Volume/full | Raiz da entrada; as oito subpastas têm o nome da fonte |
| serverless | bool | true | Mantém a escolha já usada pelo pipeline atual |
| continuous | bool | false | Atualizações triggered sob demanda |
| edition | string | ADVANCED | Necessário para expectations e serverless |
| event_log.name | string | synthetic_banking_etl_event_log | Destino consultável para métricas, estado e duração de updates |
| source_sample_rows | int | 5,000 | Limite do script local para preparar cada arquivo pequeno de dev |
| GitHub host/client id | string | Variáveis do ambiente GitHub | Identifica workspace e service principal para OIDC; não é segredo |

O nome das pastas de entrada deve ser customers, transactions, leasing_agreements, life_insurance, property_insurance, pension, margin_assets e margin_liabilities. A amostra local é gerada em data/sample/ e fica fora do Git. A exportação integral só é enviada para o Volume quando o operador selecionar o alvo full. O alvo full usa outra raiz de deploy e schema para evitar colisão de checkpoint ou tabelas com dev.

## Security Considerations

- O conjunto é sintético conforme a premissa aprovada, mas IBAN, código postal, data de nascimento/registro e demografia recebem tratamento restrito como campos sensíveis-like.
- Não incluir arquivos integrais em GitHub. Ignorar data/sample e data/full; publicar somente código, regras e documentação. Fixtures de teste são pequenas e artificiais.
- Restringir grants UC do Volume e tabelas à identidade de execução e aos revisores necessários.
- Usar workload identity federation com service principal para GitHub Actions; armazenar host e client ID como variáveis, sem token pessoal ou client secret no repositório.
- Manter o deploy de prod manual até confirmar identidade, isolamento e grants do workspace compartilhado.
- A Gold 360 inclui atributos de cliente necessários para demonstração, sem IBAN ou identificadores de contraparte.

## Observability

| Aspect | Implementation |
|--------|----------------|
| Logging | Event log do Lakeflow no Unity Catalog registra eventos, flow status, update id e duração |
| Metrics | Expectation WARN por regra na validação privada; contagens de quarantine por fonte e _failed_rules; row counts por camada e update |
| Lineage | Grafo Lakeflow registra Bronze → validação privada → Silver/quarentena → Gold; _source_file acompanha cada registro |
| Reconciliation | Query por mês e C/D compara as somas DECIMAL de Silver com gold_monthly_transaction_kpis |
| Run reporting | workflow_dispatch e atualização Lakeflow mostram resultado; README registra comandos para consultar event log, Bronze e quarantine |

Contagens do dicionário variam das contagens do footer dos Parquet disponíveis; os footers atuais são o baseline do upload. A pasta observada soma 130,614,687 linhas e cerca de 3.52 GB, com 120,926,662 linhas em transactions. A primeira execução integral também registra duração e volume produzido, ainda sem orçamento/SLA definido.

## Pipeline Architecture (if applicable)

### DAG Diagram

    customers.parquet ───────────────→ bronze_customers → quality_customers ──┬→ silver_customers
                                                                              └→ quarantine_customers
    transactions.parquet ────────────→ bronze_transactions → quality_transactions ─┬→ silver_transactions
                                                                                    └→ quarantine_transactions
    leas./insurance/pension/margin ──→ 6 Bronze tables → 6 quality views ────────────┬→ 6 Silver tables
                                                                                   └→ 6 quarantine tables

    silver_transactions + silver_customers → gold_monthly_transaction_kpis
    8 Silver tables, agregados antes dos joins → gold_customer_product_360

Cada fonte segue seu próprio fluxo Bronze → validação → Silver/quarentena. Os seis fluxos de produto e transações dependem de silver_customers para validar client_id_fk. A validação de clientes não depende desses fluxos. Gold consome apenas Silver; não há dependências circulares. Há um único bundle/pipeline implantável em cada alvo.

gold_monthly_transaction_kpis tem grão mês × canal × tipo de transação × segmento × direção. Publica contagem, total de débito, total de crédito e líquido (crédito menos débito), todos em EUR decimal. gold_customer_product_360 tem grão client_id: cada fonte mensal é reduzida ao snapshot mais recente disponível por cliente e agregada ao grão cliente antes dos joins. A visão inclui atributos demográficos necessários, saldos/posições recentes por produto e contagens/totais/datas de transação; não inclui IBAN ou contraparte.

### Partition Strategy

| Table | Partition Key | Granularity | Rationale |
|-------|-------------|-------------|-----------|
| Bronze/Silver/quarantine | Nenhuma partição manual | Delta managed | Tamanho atual ~3.52 GB; evitar partições pequenas por client_id ou alta cardinalidade |
| Gold mensal | Nenhuma partição manual | Delta managed | Agregação mensal reduz o volume a uma tabela pequena |
| Gold cliente-produto | Nenhuma partição manual | Delta managed | Uma linha por cliente; volume baixo comparado à transação |

Não criar partições Hive com year/month de origem. Manter layout inicial simples e medir o lote integral. Considerar liquid clustering em book_date ou month apenas se consultas e estatísticas de uso demonstrarem benefício; não antecipar custo de manutenção.

### Incremental Strategy

| Model | Strategy | Key Column | Lookback |
|-------|----------|------------|----------|
| Oito Bronze | Auto Loader streaming table com checkpoint do pipeline; triggered | Caminho de arquivo controlado pelo Auto Loader | Sem janela temporal |
| Oito validações privadas e Silver | Materialized views batch atualizadas a partir de Bronze | Chaves de origem quando definidas; demais linhas mantêm a granularidade física | Sem lookback |
| Oito quarentenas | Materialized views batch sobre validação privada | Identificador de origem e _source_file | Sem lookback |
| Gold mensal | Materialized view batch por truncamento de book_date em mês | mês, direção e dimensões de filtro | Sem lookback |
| Gold cliente-produto | Materialized view batch; agregar cada fonte ao grão cliente antes de juntar | client_id | snapshot máximo por cliente/fonte |

Clientes usa client_id único; transações usa txn_id único. Fontes de snapshot sem id de contrato documentado não recebem uma chave artificial. Para cada fonte de produto, selecionar o snapshot mais recente por cliente, agregar linhas desse snapshot ao grão cliente e só então juntar. Isso evita fanout na Gold 360. Soma monetária parte de amount_eur arredondado e convertido para DECIMAL(18,2); a reconciliação agrega por mês e direção C/D.

### Schema Evolution Plan

| Change Type | Handling | Rollback |
|-------------|----------|----------|
| Novo campo | Bronze preserva _rescued_data com modo rescue; regra bloqueia Silver até revisar e mapear o campo | Reverter mudança de código/schema; Bronze segue preservando origem |
| Tipo alterado | Conteúdo não compatível fica resgatado ou falha a leitura; atualizar mapeamento Silver após revisão | Reverter mapping e reprocessar explicitamente o alvo quando necessário |
| Campo removido | Bronze mantém o contrato aprendido; Silver detecta nulos/ausência e registra rejeições | Restaurar o campo na origem ou atualizar o contrato em mudança revisada |
| Campo renomeado/case diferente | Capturar em _rescued_data; tratar como alteração de contrato | Corrigir fonte ou mapear nome novo e reexecutar após revisão |

O schema físico Parquet inicial é conferido por fonte e os schemas Silver são explicitados. Schema drift não entra silenciosamente em Gold. Atualizações de schema e regra passam por pull request, testes pequenos e bundle validate antes do deploy.

### Data Quality Gates

| Gate | Tool | Threshold | Action on Failure |
|------|------|-----------|-------------------|
| Bronze conserva linha e origem | Auto Loader + _source_file | Contagem por tabela igual ao footer da entrada | Falha de leitura bloqueia a atualização; resolver arquivo |
| Contrato de customer | Expectations: client_id requerido/único, segmento/domínios válidos, datas coerentes, gender_cd X tratado como anomalia | Zero inválidos em Silver | WARN métrico, quarentena e filtro de Silver |
| Contrato de transactions | Expectations: txn_id requerido/único, cliente válido, campos financeiros, C/D, domínio de channel/type/currency, datas coerentes | Zero inválidos em Silver | WARN métrico, quarentena e filtro de Silver |
| Contratos de seis produtos | Expectations: cliente válido, status/domínio, datas e valores não negativos | Zero inválidos em Silver | WARN métrico, quarentena e filtro de Silver |
| Schema rescue | _rescued_data IS NULL para Silver | Zero linhas resgatadas em Silver | Preservar Bronze, enviar linha à quarentena e revisar schema |
| Gold mensal | DECIMAL por mês e direção compara com Silver | Diferença = 0.00 após arredondamento | Bloquear aceite da execução; investigar regra/fanout |
| Gold cliente-produto | Uma linha por client_id válido | 0 duplicatas; cobertura de clientes Silver | Bloquear aceite da Gold; investigar agregações/joins |
| Integridade de chave | Flags para PK duplicada e FK órfã | Zero em Silver | Guardar cada linha na quarentena com nome de regra |

As regras de FX só verificam amount_orig = amount_eur para trx_ccy = EUR, tolerando diferença inferior a um centavo. Para outras moedas, não há tabela de referência de câmbio disponível; não inventar taxa nem classificar divergência sem contrato.

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-11-11 | design-agent | Arquitetura Lakeflow/DAB, quarentena observável, CI/CD e manifestos para oito fontes e duas Gold |

## Next Step

**Ready for:** /ship .claude/sdd/features/DEFINE_TRANSACTION_PIPELINE.md
