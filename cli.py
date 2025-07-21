import click
from dotenv import load_dotenv
import os

load_dotenv()

@click.group()
def cli():
    """Tech News Aggregator CLI"""
    pass

@cli.command()
def ping():
    """Test CLI responsiveness"""
    click.echo("Pong! CLI is running.")

@cli.command()
def help():
    """Show available commands"""
    click.echo(cli.get_help(click.Context(cli)))