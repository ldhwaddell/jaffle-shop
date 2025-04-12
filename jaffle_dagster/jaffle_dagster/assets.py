import os

import duckdb
import pandas as pd
import plotly.express as px
from dagster import MetadataValue, AssetExecutionContext, asset
from dagster_dbt import DbtCliResource, dbt_assets, get_asset_key_for_model

from .project import jaffle_shop_project

duckdb_db_path = jaffle_shop_project.project_dir.joinpath("tutorial.duckdb")


@asset(compute_kind="python")
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


@asset(
    compute_kind="python",
    deps=[get_asset_key_for_model([jaffle_shop_dbt_assets], "customers")],
)
def order_count_chart(context: AssetExecutionContext):
    conn = duckdb.connect(os.fspath(duckdb_db_path))
    customers = conn.sql("select * from customers").df()

    fig = px.histogram(customers, x="number_of_orders")
    fig.update_layout(bargap=0.2)
    save_chart_path = duckdb_db_path.parent.joinpath("order_count_chart.html")
    fig.write_html(save_chart_path, auto_open=True)

    context.add_output_metadata(
        {"plot_url": MetadataValue.url(f"file://{os.fspath(save_chart_path)}")}
    )
