from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import jaffle_shop_dbt_assets, order_count_chart, raw_customers
from .project import jaffle_shop_project
from .schedules import schedules

defs = Definitions(
    assets=[order_count_chart, raw_customers, jaffle_shop_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=jaffle_shop_project),
    },
)
