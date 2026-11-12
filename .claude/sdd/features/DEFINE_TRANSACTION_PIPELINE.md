# DEFINE: Transaction Pipeline

> Transformar a exportação bancária sintética em um pipeline Lakeflow com qualidade observável, CI/CD e duas saídas Gold.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | TRANSACTION_PIPELINE |
| **Date** | 2026-11-11 |
| **Author** | define-agent |
| **Status** | ✅ Complete (Built) |
| **Clarity Score** | 13/15 |

---

## Problem Statement

O projeto synthetic_banking ainda contém transformações de exemplo de NYC Taxi e não demonstra como construir, validar e entregar dados bancários do raw à Gold. Revisores técnicos do portfólio precisam de um exemplo reproduzível que mostre ingestão Lakeflow, qualidade rastreável e promoção por GitHub Actions e DAB.

---

## Target Users

| User | Role | Pain Point |
|------|------|------------|
| Recrutadores, entrevistadores e revisores técnicos | Avaliam um portfólio de engenharia de dados Databricks | Não conseguem verificar decisões de ingestão, qualidade e entrega em um pipeline completo e reproduzível |

---

## Goals

What success looks like (prioritized):

| Priority | Goal |
|----------|------|
| **MUST** | Ingerir os oito arquivos Parquet fornecidos para Bronze com Auto Loader e transformar todas as fontes em Silver com schemas e regras documentados. |
| **MUST** | Reter linhas de origem em Bronze; enviar registros inválidos para quarentena com regra/origem/métricas; garantir que apenas registros válidos avancem em Silver. |
| **MUST** | Publicar duas tabelas Gold queryable: KPIs mensais de transações e visão cliente/produtos com escopo enxuto. |
| **MUST** | Versionar código e configuração DAB em GitHub; executar lint, testes sobre amostra e validação do bundle em pull requests; implantar em dev após merge. |
| **SHOULD** | Executar manualmente o volume integral e registrar duração, contagens e métricas de qualidade; promover para prod por ação manual após validar isolamento e permissões. |
| **COULD** | Adicionar dashboard de demonstração em uma fase posterior, caso o portfólio passe a exigir uma interface visual. |

**Priority Guide:**
- **MUST** = MVP fails without this (non-negotiable)
- **SHOULD** = Important, but a workaround exists
- **COULD** = Nice-to-have, cut first if needed

---

## Success Criteria

Measurable outcomes:

- [ ] Os 8 de 8 arquivos-fonte têm schema e contagem documentados; cada tabela Bronze contém a mesma contagem de linhas registrada no footer do respectivo Parquet.
- [ ] 100% dos registros rejeitados pelas regras definidas são identificáveis em quarentena por tabela, regra e arquivo de origem; 0 registros que falhem essas regras chegam às tabelas Silver válidas.
- [ ] Todas as 8 tabelas Silver publicam métricas por regra e atualização do pipeline.
- [ ] As 2 tabelas Gold são consultáveis: KPIs por mês/canal/tipo/segmento e uma linha por cliente válido na visão cliente/produtos.
- [ ] Para cada mês e direção C/D, totais monetários da Gold reconciliam com Silver à precisão de centavos; a Gold cliente/produtos não tem chaves de cliente duplicadas.
- [ ] Um pull request executa com sucesso lint, testes na amostra pequena e databricks bundle validate -t dev; o merge implanta a versão correspondente em dev.
- [ ] Uma execução manual com os oito arquivos completos termina com status de sucesso e registra contagens/métricas para cada camada.

---

## Acceptance Tests

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| AT-001 | Carga das fontes | Os oito Parquet estão no Volume Unity Catalog e o alvo dev está configurado | O pipeline triggered executa | As oito tabelas Bronze são criadas e cada contagem corresponde ao footer de entrada |
| AT-002 | Quarentena de registros inválidos | Há linhas que violam regras do contrato, incluindo anomalias intencionais identificadas no dicionário | O pipeline processa Bronze para Silver | Cada rejeição tem regra e origem registradas em quarentena; nenhuma linha rejeitada aparece na Silver válida; métricas ficam disponíveis |
| AT-003 | Reexecução do mesmo lote | O checkpoint do Auto Loader existe e os mesmos arquivos não foram alterados | O mesmo lote é executado novamente | Nenhum registro é duplicado por reprocessamento dos mesmos arquivos e as contagens de origem permanecem estáveis |
| AT-004 | Reconciliação Gold | Silver válida foi materializada para todas as fontes | As duas Gold são atualizadas | Soma de valores por mês/direção reconcilia com Silver à precisão de centavos; customer_id é único na visão cliente/produtos |
| AT-005 | Verificações de pull request | Um pull request altera código ou configuração | GitHub Actions executa o workflow | Lint, testes na fixture pequena e validação DAB em dev passam; uma falha bloqueia o workflow |
| AT-006 | Demonstração de escala | Os oito Parquet completos estão no Volume e o operador aciona a execução manual | O pipeline executa o lote completo | A atualização termina com sucesso e deixa contagens, duração e métricas por camada consultáveis |

---

## Out of Scope

Explicitly NOT included in this feature:

- Dashboard para consumidores de negócio.
- Pipeline contínuo de baixa latência ou simulação de streaming/CDC.
- Modelo de fraude, classificação ou outra solução ML.
- Processar o dataset integral a cada pull request.
- Subir os arquivos Parquet completos para Git; somente fixtures pequenas e não sensíveis podem integrar os testes.

---

## Constraints

| Type | Constraint | Impact |
|------|------------|--------|
| Technical | O projeto tem um DAB e pipeline Python de exemplo, mas as transformações atuais leem NYC Taxi. | Estender o bundle existente e substituir o exemplo pelas oito fontes financeiras. |
| Technical | O workspace usa targets dev e prod no mesmo host Databricks, com schemas distintos. | Promoção a prod fica manual até validar isolamento, permissões e identidade de execução. |
| Data | Exportação local contém 8 Parquet, 130,614,687 linhas e ~3.52 GB; transactions tem 120,926,662 linhas e ~3.35 GB. | Usar amostra em CI/dev; reservar o volume integral para execução manual. |
| Data | DATA_DICTIONARY.html descreve 221,000 clientes e 151.1M transações, mas os footers presentes mostram 200,000 e 120,926,662; outras contagens também divergem. | Usar contagens dos arquivos como baseline da exportação atual e resolver a diferença antes de tratar números do dicionário como reconciliação. |
| Data | Exportação é histórica (jan/2024–dez/2025), em Parquet; não há fonte de eventos/CDC nem SLA de baixa latência. | Lakeflow triggered é suficiente; não configurar execução contínua. |
| Infrastructure | Não há diretório .git nem workflow GitHub Actions no checkout atual. | Criar/conectar o repositório GitHub e configurar workflow e autenticação antes do primeiro deploy automatizado. |
| Infrastructure | Volume Unity Catalog, permissões e identidade federada do workflow ainda não foram verificados. | Design deve incluir recursos e permissões; não armazenar credenciais no código. |
| Resource | Não há orçamento ou limite de duração definido para o full run. | Medir custo/duração antes de estabelecer SLA de execução integral. |

---

## Technical Context

> Essential context for Design phase - prevents misplaced files and missed infrastructure needs.

| Aspect | Value | Notes |
|--------|-------|-------|
| **Deployment Location** | `src/synthetic_banking_etl/transformations/`, `resources/synthetic_banking_etl.pipeline.yml`, `databricks.yml`, `.github/workflows/` | Manter um arquivo por dataset; reutilizar bundle e recurso de pipeline existentes; novo workflow cobre CI/CD |
| **KB Domains** | `lakeflow`, `medallion`, `data-quality`, `testing`, `spark`, `python` | Consultar padrões de Auto Loader, expectations/quarentena, Bronze/Silver/Gold, testes PySpark e estrutura Python |
| **IaC Impact** | Modificar existente e adicionar recursos | Atualizar o DAB para Volume/configuração do pipeline se necessário; adicionar GitHub Actions; revisar alvos dev/prod e autorização Unity Catalog |

**Why This Matters:**

- **Location** → Design phase uses correct project structure, prevents misplaced files
- **KB Domains** → Design phase pulls correct patterns from the KB
- **IaC Impact** → Triggers infrastructure planning, avoids "works locally" failures

---

## Data Contract (if applicable)

> Include this section when the feature involves data pipelines, ETL, or analytics.

### Source Inventory

| Source | Type | Volume | Freshness | Owner |
|--------|------|--------|-----------|-------|
| customers.parquet | Parquet | 200,000 rows | Historical 2024-01–2025-12; processar após upload, sob demanda | TBD |
| transactions.parquet | Parquet | 120,926,662 rows; 3,347,559,295 bytes | Histórico; processar após upload, sob demanda | TBD |
| leasing_agreements.parquet | Parquet | 116,660 rows | Histórico; processar após upload, sob demanda | TBD |
| life_insurance.parquet | Parquet | 592,381 rows | Histórico; processar após upload, sob demanda | TBD |
| margin_assets.parquet | Parquet | 4,561,521 rows | Histórico; processar após upload, sob demanda | TBD |
| margin_liabilities.parquet | Parquet | 238,478 rows | Histórico; processar após upload, sob demanda | TBD |
| pension.parquet | Parquet | 2,966,040 rows | Histórico; processar após upload, sob demanda | TBD |
| property_insurance.parquet | Parquet | 1,012,945 rows | Histórico; processar após upload, sob demanda | TBD |

### Schema Contract

| Column | Type | Constraints | PII? |
|--------|------|-------------|------|
| customers.client_id | STRING | NOT NULL, UNIQUE; chave primária | No (identificador sintético) |
| customers.segment | STRING | NOT NULL quando aplicável; domínio Private/SME/Corporate | No (sintético) |
| customers.birth_or_reg_dt | TIMESTAMP no Parquet; DATE em Silver | Não posterior a bank_reg_dt; validar exceções | No (dado sintético, sensível-like) |
| transactions.txn_id | STRING | NOT NULL, UNIQUE | No (identificador sintético) |
| transactions.client_id_fk | STRING | NOT NULL; referência a customers.client_id | No (identificador sintético) |
| transactions.book_date | TIMESTAMP no Parquet; DATE em Silver | Entre 2024-01-01 e 2025-12-31 conforme a exportação descrita | No (data sintética) |
| transactions.amount_eur | DOUBLE no Parquet; DECIMAL(18,2) em Silver | Obrigatório para linha válida; reconciliar totais à precisão de centavos | No (dado sintético) |
| transactions.dc_indicator | STRING | Domínio C/D; usar direção ao agregar valores | No (dado sintético) |
| transactions.channel / txn_type | STRING | Validar contra domínios descritos no DATA_DICTIONARY.html | No (dado sintético) |
| Produtos: client_id_fk | STRING | NOT NULL para linha válida; referência a customers.client_id | No (identificador sintético) |
| Produtos: snapshot_date / month_end / contrib_date | TIMESTAMP no Parquet; DATE em Silver | Datas do período histórico conforme cada fonte | No (data sintética) |
| IBANs, postal_code e campos demográficos | STRING/TIMESTAMP conforme Parquet | Manter apenas para uso necessário; tratar como campos sensíveis-like mesmo sendo sintéticos | No (dados sintéticos) |

O schema físico completo de cada arquivo Parquet é a referência de ingestão; esta tabela define as colunas-chave e regras críticas. Valores monetários de origem em DOUBLE devem ser convertidos explicitamente para decimal em Silver. A validação de FX para moedas diferentes de EUR depende de uma referência de câmbio, que não aparece entre as oito fontes; não inferir uma taxa ausente.

### Freshness SLAs

| Layer | Target | Measurement |
|-------|--------|-------------|
| Raw / Bronze | Disponível após upload dos arquivos e execução manual do pipeline; não há SLA de minutos | Timestamp de ingestão e conclusão da atualização |
| Silver / Gold | Atualizar na mesma atualização triggered bem-sucedida que processa Bronze | Timestamp/status da atualização e dependências do pipeline |

### Completeness Metrics

- 8 de 8 fontes carregadas; contagens Bronze iguais aos footers dos arquivos de entrada.
- 100% das linhas que falham regras definidas ficam identificáveis em quarentena; 0 falhas nessas regras nas tabelas Silver válidas.
- Customer_id é único na Silver de clientes e na Gold cliente/produtos; todas as referências cliente/produto são válidas ou ficam em quarentena.
- Totais monetários agregados da Gold de transações reconciliam com Silver por mês e direção à precisão de centavos.
- Todas as tabelas Silver expõem contagem de entradas, válidas, rejeitadas e métricas por regra/execução.

### Lineage Requirements

- Permitir rastrear nome/caminho do arquivo de origem e regra aplicada para cada registro de Bronze/Silver/quarentena.
- Manter relação de dependências entre as oito fontes, tabelas Silver e as duas Gold no grafo do pipeline.
- Registrar contagens e métricas por tabela e atualização, incluindo o resultado da execução manual integral.

---

## Assumptions

Assumptions that if wrong could invalidate the design:

| ID | Assumption | If Wrong, Impact | Validated? |
|----|------------|------------------|------------|
| A-001 | Os valores nos oito Parquet são inteiramente sintéticos e podem ser usados em um portfólio público. | Se houver dados reais, uso público, versionamento e política de acesso precisam ser revistos. | [x] |
| A-002 | A pasta student_export é a exportação correta para o MVP, apesar de divergir dos totais no dicionário. | Se estiver incompleta, baseline de volume, schemas e critérios de reconciliação mudam. | [ ] |
| A-003 | O workspace permite Unity Catalog Volumes, Lakeflow serverless e acesso do pipeline aos arquivos. | Se não, será necessário outro landing ou compute e o plano de ingestão muda. | [ ] |
| A-004 | O usuário terá um remoto GitHub e permissões para criar GitHub Actions e autenticação federada. | Sem isso, CI/CD não consegue validar/deploy no workspace. | [ ] |
| A-005 | Uma amostra pequena representativa pode ser extraída dos Parquet para fixtures de CI sem publicar os arquivos completos. | Se não, testes deverão gerar dados de fixture localmente ou usar outro dataset reduzido. | [ ] |
| A-006 | Não há SLA de duração/custo para o lote integral; a primeira execução servirá para medir. | Um SLA estrito pode exigir otimização, alteração de compute ou nova política de execução. | [x] |

**Note:** Validar as premissas críticas antes de DESIGN. Premissas não validadas tornam-se riscos.

---

## Clarity Score Breakdown

| Element | Score (0-3) | Notes |
|---------|-------------|-------|
| Problem | 3 | A lacuna do projeto e o impacto para demonstrar o portfólio estão claros. |
| Users | 2 | Revisor técnico está identificado; perfil de consumo e avaliação ainda é amplo. |
| Goals | 3 | Lakeflow, DAB, GitHub Actions, camadas, quarentena e duas Gold foram escolhidos. |
| Success | 2 | Há métricas testáveis, mas o limite de custo/duração do full run e a proporção real de rejeições ainda não estão estabelecidos. |
| Scope | 3 | Oito fontes até Silver, duas Gold e exclusões foram confirmadas. |
| **Total** | **13/15** | **87%; atende ao gate de 12/15 para Design.** |

**Scoring Guide:**
- 0 = Missing entirely
- 1 = Vague or incomplete
- 2 = Clear but missing details
- 3 = Crystal clear, actionable

**Minimum to proceed: 12/15**

---

## Open Questions

- A diferença entre DATA_DICTIONARY.html e os footers Parquet é intencional (exportação parcial) ou faltam arquivos/partições?
- Qual catalog/schema e Volume serão usados em dev e prod, e prod deve permanecer no mesmo workspace para o portfólio?
- O workspace já tem UC Volume, permissões de serverless Lakeflow e service principal/federação disponíveis para GitHub Actions?
- Para valores não EUR, existe tabela de câmbio de referência fora desta exportação? Sem ela, a checagem de amount_eur fica limitada às regras verificáveis com os campos presentes.
- Qual limite aceitável de duração/custo para a primeira execução integral de ~3.52 GB?

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-11-11 | define-agent | Requisitos derivados do BRAINSTORM_SYNTHETIC_BANKING.md; clareza 13/15, pronto para Design |

---

## Next Step

**Ready for:** /ship .claude/sdd/features/DEFINE_TRANSACTION_PIPELINE.md

