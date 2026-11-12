# BRAINSTORM: Synthetic Banking Medallion Pipeline

> Sessão exploratória para esclarecer intenção e abordagem antes da captura de requisitos.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | SYNTHETIC_BANKING |
| **Date** | 2026-11-08 |
| **Author** | brainstorm-agent |
| **Status** | ✅ Complete (Defined) |

---

## Initial Idea

**Raw Input:** “Projeto synthetic banking montando um pipeline do zero, utilizando Databricks Lakeflow Pipelines, Git, CI/CD e data quality. Da camada raw até a gold.”

**Context Gathered:**
- O workspace já contém um projeto Python com DAB: `databricks.yml`, alvos `dev` e `prod`, e um recurso de pipeline.
- A transformação atual ainda é o exemplo de NYC Taxi. Ela usa a API moderna `from pyspark import pipelines as dp`; não há transformações bancárias implementadas.
- A pasta do projeto não contém `.git` nem `.github/workflows`; portanto não há histórico de commits nem CI configurado no workspace.
- Os Parquet e o dicionário foram encontrados fora do repositório em `C:\Users\Nathan\Downloads\synthetic_banking\student_export`.
- O dicionário descreve 24 meses (jan/2024–dez/2025) e anomalias intencionais em cerca de 0,5% dos dados. A contagem indicada no dicionário difere da contagem dos footers Parquet desta exportação; usar os arquivos como referência de volume e registrar a divergência.

**Technical Context Observed (for Define):**

| Aspect | Observation | Implication |
|--------|-------------|-------------|
| Likely Location | `src/synthetic_banking_etl/transformations/`, `resources/synthetic_banking_etl.pipeline.yml`, `databricks.yml` | Substituir as transformações de demonstração e estender o pipeline DAB existente |
| Relevant KB Domains | `lakeflow`, `medallion`, `data-quality`, `testing`, `spark` | Consultar padrões para ingestão, medallion, expectativas/quarentena e testes |
| IaC Patterns | DAB existente com targets dev/prod; atualmente ambos apontam para o mesmo workspace e usam schemas distintos | Reutilizar DAB; revisar isolamento e identidade antes de promover para prod |

---

## Discovery Questions & Answers

| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | Qual é o objetivo principal? | Portfólio técnico | Priorizar evidência de decisões de engenharia e execução reproduzível |
| 2 | Quem consome a Gold no MVP? | Somente demonstrar o pipeline no portfólio | Tabelas consultáveis bastam; dashboard não é requisito inicial |
| 3 | Há amostras para orientar o domínio? | Sim, há arquivos de amostra | Usar schemas, relacionamentos e defeitos do conjunto real |
| 4 | Onde estão e em que formato? | `C:\Users\Nathan\Downloads\synthetic_banking\student_export`, Parquet; há também `DATA_DICTIONARY.html` | Usar os Parquet como entrada e o HTML para semântica e regras |
| 5 | Como tratar o volume no MVP? | Amostra em dev/CI; volume completo sob demanda | Reduzir custo e tempo de iteração, mantendo demonstração de escala |
| 6 | Qual plataforma de Git/CI? | GitHub + GitHub Actions | Criar repositório/workflow; usar DAB para validação e promoção |
| 7 | Qual assunto de negócio priorizar na Gold? | KPIs de transações e cliente/produtos, com escopo enxuto | Entregar duas tabelas Gold focadas |
| 8 | Como tratar linhas inválidas? | Quarentena com métricas; Silver só recebe linhas válidas | Preservar origem, investigar rejeições e proteger consumidores |
| 9 | Qual abordagem de ingestão? | A — Volume Unity Catalog + Auto Loader + Lakeflow | Adotar ingestão incremental de arquivos e execução triggered |
| 10 | Qual recorte de camadas? | Oito fontes até Silver e duas Gold | Aplicar limpeza/qualidade às oito fontes e limitar as saídas de negócio |
| 11 | Os critérios de sucesso propostos estão corretos? | Sim, pode registrar assim | Critérios abaixo aprovados para o MVP |

**Minimum Questions:** 3 (to ensure clarity before proceeding)

---

## Sample Data Inventory

| Type | Location | Count | Notes |
|------|----------|-------|-------|
| Input files | `C:\Users\Nathan\Downloads\synthetic_banking\student_export` | 8 Parquet; 130,614,687 rows total | 3.516 GB total; transactions.parquet tem 120,926,662 linhas e 3,347,559,295 bytes |
| Output examples | N/A | 0 | Não foram fornecidos resultados esperados por tabela Gold |
| Ground truth | `DATA_DICTIONARY.html` na pasta de amostras | 1 | Descreve colunas, domínios, relacionamentos, período e anomalias; seus totais não coincidem integralmente com esta exportação |
| Related code | `databricks.yml`, configuração do pipeline e duas transformações de exemplo | 4 | Estrutura reutilizável; código atual trata dados de NYC Taxi, não banking |

**Contagens dos footers dos Parquet desta exportação:**
- customers: 200,000
- transactions: 120,926,662
- leasing_agreements: 116,660
- life_insurance: 592,381
- margin_assets: 4,561,521
- margin_liabilities: 238,478
- pension: 2,966,040
- property_insurance: 1,012,945

O dicionário descreve 221,000 clientes e 151.1 milhões de transações, acima das contagens lidas dos arquivos presentes. As demais contagens também variam em alguns casos. O pipeline deve registrar a contagem real dos arquivos; a divergência deve ser entendida antes de usar os números do dicionário como reconciliação.

**How samples will be used:**
- Confirmar schemas e mapear tipos antes de definir os contratos de Silver.
- Usar client_id/client_id_fk e txn_id para regras de chave e integridade referencial.
- Derivar regras de domínio e anomalias intencionais do dicionário.
- Preparar fixture/amostra pequena para CI e manter a exportação completa fora dos commits.
- Comparar métricas das camadas com os footers Parquet, documentando a diferença em relação ao dicionário.

---

## Approaches Explored

### Approach A: Volume Unity Catalog + Auto Loader + Lakeflow ⭐ Recommended

**Description:** Fazer landing dos oito Parquet em um Volume Unity Catalog; carregar Bronze com Auto Loader e Streaming Tables; limpar e validar em Silver com saídas explícitas de quarentena; produzir duas Materialized Views Gold. DAB configura ambientes e GitHub Actions executa CI/CD.

**Pros:**
- Demonstra ingestão incremental de arquivos, dependências declarativas e o fluxo Bronze/Silver/Gold.
- Auto Loader e Lakeflow oferecem estado de ingestão e execução incremental; Volume mantém o landing governado pelo Unity Catalog.
- O mesmo bundle suporta validação e deploy por ambiente; a execução integral pode ser manual.

**Cons:**
- Requer criar/configurar o Volume e permissões de acesso no workspace.
- Quarentena com retenção de linhas exige lógica de split além das métricas de expectations.

**Why Recommended:** Confiança 0.95: o projeto já tem DAB, recurso de pipeline e código na API atual de Lakeflow. A base interna recomenda Auto Loader para arquivos, expectations para qualidade e Materialized Views para agregações. A documentação Databricks recomenda DAB como abordagem de CI/CD e descreve Auto Loader/Streaming Tables para ingestão de arquivos e Materialized Views para agregações ([CI/CD](https://docs.databricks.com/aws/en/dev-tools/ci-cd), [Lakeflow concepts](https://docs.databricks.com/aws/en/ldp/concepts), [expectations e quarentena](https://docs.databricks.com/aws/en/ldp/expectations)).

### Approach B: Leitura batch direta dos Parquet

**Description:** Ler os Parquet diretamente do caminho de landing e materializar transformações batch em tabelas geridas pelo pipeline, sem estado de Auto Loader.

**Pros:**
- Menos componentes de ingestão para uma exportação estática.
- Útil para exploração e uma carga pontual.

**Cons:**
- Mostra menos claramente ingestão incremental, descoberta de novos arquivos e estado/checkpoints.
- Pode tornar cada atualização cara se precisar reprocessar a exportação completa de transações.

### Approach C: Simulação de streaming/CDC

**Description:** Dividir a exportação histórica em eventos/lotes e simular chegada contínua e mudanças de estado.

**Pros:**
- Demonstra processamento stateful e CDC.

**Cons:**
- A origem atual é uma exportação histórica Parquet, não um feed de eventos ou CDC.
- Aumenta compute, código e operação sem atender ao objetivo central do portfólio.

---

## Data Engineering Context (if applicable)

### Source Systems

| Source | Type | Volume Estimate | Current Freshness |
|--------|------|-----------------|-------------------|
| Export sintética de banking | Parquet local, copiado para Volume Unity Catalog | 8 arquivos, ~130.6M linhas, ~3.52 GB; 120.9M linhas em transactions | Histórico jan/2024–dez/2025; lote manual/sob demanda |

### Data Flow Sketch

```text
Parquet local → Volume Unity Catalog → Auto Loader → Bronze (8 fontes)
→ Silver (tipos, regras, válidos + quarentena) → Gold (2 Materialized Views)
→ consultas demonstrativas por revisores do portfólio
```

### Key Data Questions Explored

| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | Qual é o volume esperado? | ~130.6M linhas no export; transações dominam com 120.9M | Amostra em CI/dev; full load manual |
| 2 | Qual frescor/SLA é necessário? | Sem necessidade de baixa latência; dados históricos | Execução triggered, sem pipeline contínuo |
| 3 | Quem consome o resultado? | Revisor técnico do portfólio; sem dashboard no MVP | Priorizar tabelas Gold legíveis e queryáveis |

---

## Selected Approach

| Attribute | Value |
|-----------|-------|
| **Chosen** | Approach A |
| **User Confirmation** | 2026-11-08 |
| **Reasoning** | O usuário confirmou Volume + Auto Loader + Lakeflow para demonstrar ingestão, qualidade e deploy sem simular streaming que não existe na fonte |

---

## Key Decisions Made

| # | Decision | Rationale | Alternative Rejected |
|---|----------|-----------|----------------------|
| 1 | Manter DAB como unidade de deploy e usar GitHub + GitHub Actions | O bundle e targets dev/prod já existem; DAB é indicado pela Databricks para CI/CD | Deploy manual de arquivos ou reimplementação de infraestrutura fora do bundle |
| 2 | Usar Volume Unity Catalog + Auto Loader e modo triggered | A origem são arquivos Parquet históricos; ingestão incremental de arquivos atende ao caso e evita execução contínua | Ler paths diretamente em cada transformação; simular streaming/CDC |
| 3 | Processar todas as oito fontes até Silver e produzir duas Gold | O usuário quer caminho raw-to-gold completo, com duas saídas analíticas enxutas | Limitar o domínio à tabela de transações |
| 4 | Separar inválidos em quarentena e manter métricas | Facilita investigação e impede que inválidos cheguem à Silver | Descartar silenciosamente ou apenas manter inválidos junto com dados limpos |
| 5 | CI/dev usa amostra; full dataset sob demanda | Mantém o feedback de PR barato e ainda demonstra escala real | Executar os 120.9M de transações em cada PR |
| 6 | Continuar com Python e a API atual `pyspark.pipelines` | O projeto já é Python e contém exemplo na API moderna | Reescrever o bundle em SQL durante o MVP |

---

## Features Removed (YAGNI)

| Feature Suggested | Reason Removed | Can Add Later? |
|-------------------|----------------|----------------|
| Dashboard de KPIs | O MVP demonstra o pipeline; Gold queryável atende ao objetivo | Yes |
| Streaming contínuo e CDC | A amostra atual é uma exportação histórica em arquivos, sem feed mutável | Yes |
| Modelo de fraude/ML | Não é necessário para demonstrar pipeline, camadas e data quality | Yes |
| Executar o dataset completo em todos os PRs | Custo e duração não se justificam para feedback frequente; full run fica manual | Yes |

---

## Incremental Validations

| Section | Presented | User Feedback | Adjusted? |
|---------|-----------|---------------|-----------|
| Architecture concept / approaches | ✅ | Confirmou Approach A: Volume + Auto Loader + Lakeflow | No |
| Component breakdown / layer scope | ✅ | Confirmou oito fontes até Silver e duas Gold | No |
| Error handling / data quality | ✅ | Confirmou quarentena com métricas e Silver apenas com válidos | No |
| Success criteria / YAGNI | ✅ | Confirmou critérios de sucesso e itens adiados | No |

**Minimum Validations:** 2 (to ensure alignment)

---

## Suggested Requirements for /define

Based on this brainstorm session, the following should be captured in the DEFINE phase:

### Problem Statement (Draft)

Transformar o projeto DAB de exemplo em um pipeline Lakeflow de banking sintético, rastreável por Git e CI/CD, que processe as oito fontes Parquet de Bronze a Silver com qualidade observável e publique duas tabelas Gold demonstrativas.

### Target Users (Draft)

| User | Pain Point |
|------|------------|
| Recrutadores, entrevistadores e revisores técnicos | Precisam avaliar decisões de arquitetura, qualidade e entrega automatizada em um exemplo reproduzível |

### Success Criteria (Draft)

- [ ] Os oito arquivos Parquet têm schemas e contagens de entrada documentados e são carregados em Bronze.
- [ ] Silver contém somente registros válidos; rejeições ficam em quarentena com regra, origem e métricas.
- [ ] As regras cobrem chaves, relações entre cliente/produto, campos obrigatórios, domínios, datas e coerência monetária suportada pelos campos disponíveis.
- [ ] Duas tabelas Gold — KPIs mensais de transações e visão cliente/produtos — podem ser consultadas e reconciliadas com Silver.
- [ ] GitHub Actions roda lint, testes sobre amostra e validação DAB nos PRs; merge implanta em dev.
- [ ] Uma execução manual do volume integral é concluída e suas métricas de qualidade e contagens ficam registradas.

### Constraints Identified

- Os arquivos de amostra estão em Downloads, fora do workspace Git, e não devem ser incluídos integralmente no repositório.
- A exportação é grande e contém 120.9M transações; CI/dev deve usar uma amostra pequena.
- A origem é histórica e batch; não existe requisito de latência para justificar pipeline contínuo.
- O dicionário e os footers Parquet discordam em algumas contagens; a causa não foi determinada.
- Não foi definido SLA de duração/custo para o full run; medir durante o Define/Build.
- O repositório local ainda não tem histórico Git nem workflow; é necessário inicializar e conectar o remoto em GitHub antes do CI.

### Out of Scope (Confirmed)

- Dashboard para consumidores de negócio.
- Streaming contínuo ou CDC.
- Modelo de fraude ou outra solução ML.
- Execução integral dos dados a cada pull request.

---

## Session Summary

| Metric | Value |
|--------|-------|
| Questions Asked | 11, incluindo localização/formato e checkpoints de confirmação |
| Approaches Explored | 3 |
| Features Removed (YAGNI) | 4 |
| Validations Completed | 4 |
| Duration | Aprox. 15 minutos |

---

## Next Step

**Ready for:** `/define .claude/sdd/features/BRAINSTORM_SYNTHETIC_BANKING.md`

