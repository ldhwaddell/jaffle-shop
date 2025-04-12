import os

import duckdb
import pandas as pd
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from .project import jaffle_shop_project

duckdb_db_path = jaffle_shop_project.project_dir.joinpath("tutorial.duckdb")


def raw_customers(context: AssetExecutionContext) -> None:
    data = pd.read_csv("https://docs.dagster.io/assets/customers.csv")
    conn = duckdb.connect(os.fspath(duckdb_db_path))
    conn.execute("create schema if not exists jaffle_shop")
    conn.execute(
        "create or replace table jaffle_shop.raw_customers as select * from data"
    )

    context.add_output_metadata({"num_rows": data.shape[0]})


@dbt_assets(manifest=jaffle_shop_project.manifest_path)
def jaffle_shop_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

