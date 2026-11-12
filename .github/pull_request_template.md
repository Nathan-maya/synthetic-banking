## Summary

Describe the pipeline, quality-rule, DAB, or CI/CD change.

## Validation

- [ ] `uv run ruff check .`
- [ ] `uv run pytest -q`
- [ ] `databricks bundle validate --strict --target dev`
- [ ] Confirmed that no full Parquet data was added to Git

## Lakeflow impact

- [ ] No pipeline behavior change
- [ ] Sample-only update verified in `dev`
- [ ] Full-volume execution is intentionally excluded

## Data quality impact

- [ ] No new rule
- [ ] Rule added or changed; quarantine behavior documented
- [ ] Silver remains restricted to valid rows
