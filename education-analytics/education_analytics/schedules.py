# schedule.py

from dagster import (
    ScheduleDefinition,
    DefaultScheduleStatus,
    define_asset_job,
    AssetSelection
)

# Import your dbt assets
from dagster_project.assets import dbt_assets

# Daily job → specific dbt model
daily_dbt_job = define_asset_job(
    name="daily_dbt_job",
    selection=AssetSelection.keys("country_year_mapping")
)


# Hourly job → run all dbt models
hourly_dbt_job = define_asset_job(
    name="hourly_dbt_job",
    selection=AssetSelection.assets(dbt_assets)
)


# --------------------------------------------------
# SCHEDULE DEFINITIONS
# --------------------------------------------------

# Daily at 02:00 UTC
daily_dbt_schedule = ScheduleDefinition(
    job=daily_dbt_job,
    cron_schedule="0 2 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
)


# Hourly schedule
hourly_dbt_schedule = ScheduleDefinition(
    job=hourly_dbt_job,
    cron_schedule="0 * * * *",
    default_status=DefaultScheduleStatus.STOPPED,
)