from dagster import ScheduleDefinition, DefaultScheduleStatus, AssetSelection  # Add AssetSelection here
from dagster_project.definitions import dbt_build_assets

# Daily schedule at 2 AM UTC
daily_dbt_schedule = ScheduleDefinition(
    name="daily_dbt_run",
    target=AssetSelection.keys("country_year_mapping", "population_of_primary_education", "restructure_table"),
    cron_schedule="0 2 * * *",  # 2 AM every day
    default_status=DefaultScheduleStatus.RUNNING,  # Auto-start when deployed
)

# Hourly schedule (if needed)
hourly_dbt_schedule = ScheduleDefinition(
    name="hourly_dbt_run",
    target=dbt_build_assets,
    cron_schedule="0 * * * *",  # Every hour
    default_status=DefaultScheduleStatus.STOPPED,  # Start manually
)