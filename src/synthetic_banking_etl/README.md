# Pipeline package

Cada módulo de fonte declara Bronze, validação privada, Silver e quarentena. O diretório gold contém as duas materialized views finais. quality_rules.py centraliza predicados e separação de registros.

As oito fontes do landing são customers, transactions, leasing_agreements, life_insurance, property_insurance, pension, margin_assets e margin_liabilities. O caminho de entrada vem da configuração landing_path do pipeline.
