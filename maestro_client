#!/usr/bin/env python3

import sys

import click

from maestro_python_client.Client import Client


@click.group(context_settings=dict(help_option_names=['-h', '--help']))
def cli():
    pass


@cli.command(short_help='Submit a task to maestro')
@click.option('--retries', default=0, show_default=True, help='Number of allowed retries in case of fail or timeouts.')
@click.option('--timeout', default=900, show_default=True, help='Allowed time span for the task to execute')
@click.option('--executes_in', default=0, show_default=True, help='Number of seconds to wait before executing the task')
@click.option('--start_timeout', default=0, show_default=True, help='Allowed time span in seconds for the task to start')
@click.option('--callback_url', default="", show_default=True, help='URL called after task execution is complete [default: ""]')
@click.option('--endpoint', required=True, show_default=True, help='Maestro address')
@click.argument('owner')
@click.argument('queue')
@click.argument('payload')
def submit(retries: int, timeout: int, executes_in: int, start_timeout: int, callback_url: str, endpoint: str,
           owner: str, queue: str, payload: str):
    """ Submit a single task to the specified Maestro endpoint. """
    click.echo(f"Submit with {retries},{timeout},{executes_in},{callback_url},{endpoint},{owner},{queue},{payload}")
    if endpoint == "":
        click.echo("Mastro endpoint (--endpoint) must be set.")
        return 1
    try:
        task_id = Client(endpoint).launch_task(owner, queue, payload, retries, timeout, executes_in, start_timeout,
                                               callback_url)
        click.echo(task_id)
    except Exception as e:
        click.echo(f"Failed to submit the task - Error: {e}")
        return 2


def main() -> int:
    cli()
    return 0


if __name__ == '__main__':
    sys.exit(main())
