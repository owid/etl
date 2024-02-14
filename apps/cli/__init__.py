"""ETL services CLI."""
import rich_click as click


@click.group(name="etl", help="ETL operations")
def cli():
    pass


if __name__ == "__main__":
    cli()
