#!/usr/bin/env python3
from __future__ import annotations

import csv

from eip7904_analysis.config import default_paths
from eip7904_analysis.duckdb_utils import connect
from eip7904_analysis.labels import infer_project_label


def main() -> None:
    paths = default_paths()
    conn = connect(paths.duckdb_path)
    class_rows = conn.execute(
        "SELECT * FROM read_csv_auto('cache/contract_classification.csv', header=true)"
    ).df()
    conn.close()

    classification_by_address = {
        row["address"].lower(): row for row in class_rows.to_dict(orient="records")
    }

    in_path = paths.artifacts_dir / "tables" / "status_failure_call_pairs.csv"
    out_path = paths.artifacts_dir / "tables" / "status_failure_call_pairs_labeled.csv"

    with in_path.open() as input_handle, out_path.open("w", newline="") as output_handle:
        reader = csv.DictReader(input_handle)
        writer = csv.DictWriter(
            output_handle,
            fieldnames=[
                "caller",
                "caller_project",
                "callee",
                "callee_project",
                "status_failures",
                "avg_gas_provided",
                "avg_gas_used",
            ],
        )
        writer.writeheader()
        for row in reader:
            caller = row["caller"].lower() if row["caller"] else None
            callee = row["callee"].lower() if row["callee"] else None
            caller_class = classification_by_address.get(caller or "")
            callee_class = classification_by_address.get(callee or "")
            writer.writerow(
                {
                    "caller": caller,
                    "caller_project": infer_project_label(
                        caller,
                        compiled_name=None if caller_class is None else caller_class.get("name"),
                        classification=None
                        if caller_class is None
                        else caller_class.get("classification"),
                        source_hint=None if caller_class is None else caller_class.get("source_hint"),
                    ),
                    "callee": callee,
                    "callee_project": infer_project_label(
                        callee,
                        compiled_name=None if callee_class is None else callee_class.get("name"),
                        classification=None
                        if callee_class is None
                        else callee_class.get("classification"),
                        source_hint=None if callee_class is None else callee_class.get("source_hint"),
                    ),
                    "status_failures": row["status_failures"],
                    "avg_gas_provided": row["avg_gas_provided"],
                    "avg_gas_used": row["avg_gas_used"],
                }
            )


if __name__ == "__main__":
    main()
