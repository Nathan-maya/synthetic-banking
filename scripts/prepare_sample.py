from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

SOURCES = (
    "customers",
    "transactions",
    "leasing_agreements",
    "life_insurance",
    "property_insurance",
    "pension",
    "margin_assets",
    "margin_liabilities",
)


def _compatible_timestamps(table: pa.Table) -> pa.Table:
    schema = pa.schema(
        [
            field.with_type(pa.timestamp("us", tz=field.type.tz))
            if pa.types.is_timestamp(field.type) and field.type.unit == "ns"
            else field
            for field in table.schema
        ]
    )
    return table.cast(schema, safe=False)


def _first_rows(path: Path, rows: int) -> pa.Table:
    parquet = pq.ParquetFile(path)
    batch = next(parquet.iter_batches(batch_size=rows), None)
    if batch is None:
        raise ValueError(f"No rows found in {path.name}")
    return _compatible_timestamps(pa.Table.from_batches([batch.slice(0, rows)]))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source_dir", type=Path)
    parser.add_argument("--output", type=Path, default=Path("data/sample"))
    parser.add_argument("--rows", type=int, default=5000)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    samples = {
        source: _first_rows(args.source_dir / f"{source}.parquet", args.rows)
        for source in SOURCES
        if source != "customers"
    }
    customer_ids = {
        value
        for sample in samples.values()
        if "client_id_fk" in sample.column_names
        for value in sample["client_id_fk"].drop_null().to_pylist()
    }
    customer_file = pq.ParquetFile(args.source_dir / "customers.parquet")
    customer_schema = customer_file.schema_arrow
    if customer_ids:
        id_type = customer_schema.field("client_id").type
        id_values = pa.array(customer_ids, type=id_type)
        matching_batches = []
        for batch in customer_file.iter_batches(batch_size=args.rows):
            table = pa.Table.from_batches([batch])
            matching = table.filter(pc.is_in(table["client_id"], value_set=id_values))
            if matching.num_rows:
                matching_batches.append(matching)
        customer_sample = (
            pa.concat_tables(matching_batches)
            if matching_batches
            else pa.Table.from_batches([], schema=customer_schema)
        )
    else:
        customer_sample = _first_rows(args.source_dir / "customers.parquet", args.rows)
    samples["customers"] = _compatible_timestamps(customer_sample)
    for source in SOURCES:
        pq.write_table(samples[source], args.output / f"{source}.parquet")


if __name__ == "__main__":
    main()
