"""Where `etl chart-pull` and `etl chart-clean` read chart layers from, and how they query it."""

import rich_click as click
from rich.console import Console
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql.elements import TextClause

from etl import config
from etl.config import OWIDEnv

console = Console()


def source_env(env_name: str | None) -> tuple[OWIDEnv, bool]:
    """The environment to read admin layers from, and whether it's the staging-site-master copy."""
    if env_name:
        return OWIDEnv.from_staging_or_env_file(env_name), False
    if config.OWID_ENV.env_remote == "production":
        return config.OWID_ENV, False
    if config.ENV_FILE_PROD:
        return OWIDEnv.from_env_file(str(config.ENV_FILE_PROD)), False
    return OWIDEnv.from_staging("master"), True


def warn_master_copy_age(owid_env: OWIDEnv) -> None:
    """staging-site-master's database is a periodic copy of production: say how old it is."""
    latest = fetch_rows(owid_env, text("SELECT MAX(updatedAt) AS latest FROM chart_configs"), {})[0].latest
    console.print(
        f"[yellow]staging-site-master is a copy of production whose newest chart edit is from {latest} (UTC). "
        "Admin edits made since are missed: pass `--env <production env file>` or set ENV_FILE_PROD to read "
        "production.[/yellow]"
    )


def fetch_rows(owid_env: OWIDEnv, statement: TextClause, params: dict):
    try:
        with owid_env.engine.connect() as con:
            return con.execute(statement, params).all()
    except OperationalError as e:
        raise click.ClickException(
            f"Could not reach the grapher database at {owid_env.conf.DB_HOST} (db '{owid_env.conf.DB_NAME}'): {e.orig}"
        ) from None
