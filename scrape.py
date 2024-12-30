import time
from typing import List, Optional

import click

import api


@click.group()
def cli():
    pass


@cli.command()
@click.argument("boss", type=str, required=True)
@click.argument("gate", type=int, required=False)
@click.argument("difficulty", type=str, required=False)
@click.option(
    "--from-scratch",
    default=False,
    is_flag=True,
    help="Start from scratch, overwrite cached logs",
)
@click.option(
    "--page-size",
    default=25,
    help="Number of logs to fetch per batch",
)
@click.option(
    "--max-logs",
    default=None,
    help="Maximum number of logs to fetch before stopping.",
)
@click.option(
    "--verbose",
    "-v",
    default=False,
    is_flag=True,
    help="Print extra information",
)
def boss(
    boss: str,
    gate: int = None,
    difficulty: str = None,
    from_scratch: bool = False,
    page_size: int = 20,
    max_logs: Optional[int] = None,
    verbose: bool = False,
):
    """
    Fetch logs for a specific boss, gate, and difficulty.

    BOSS is required, GATE and DIFFICULTY should not be set unless necessary.
    """
    # TODO: Scrap until date

    # Start timer
    start = time.time()

    # Build a list of filter args
    bossArgs = []
    if boss == "all":
        for bossName, info in api.BOSSES.items():
            # If info is empty, it's probably a guardian
            if info == {}:
                bossArgs += [{"boss": bossName}]
            else:
                # Get gate
                gate = int(bossName[-1:])

                # Strip G
                bossName = bossName[:-3]

                # Get the keys for difficulty
                for diff in info['difficulties']:
                    bossArgs += [
                        {"boss": bossName, "gate": gate, "difficulty": diff}
                    ]
        bossArgs = bossArgs[::-1]
    else:
        bossArgs += [{"boss": boss, "gate": gate, "difficulty": difficulty}]

    for kwargs in bossArgs:
        api.scrape_log(
            **kwargs,
            from_scratch=from_scratch,
            page_size=page_size,
            max_logs=max_logs,
            force=boss == "all",
            verbose=verbose,
        )

    # End timer
    end = time.time()
    click.echo(f"Time elapsed: {end - start:.2f} seconds")


@cli.command()
@click.argument("boss", type=str, required=True)
@click.argument("gate", type=int, required=False)
@click.argument("difficulty", type=str, required=False)
@click.option(
    "--id",
    default=[],
    help="Update specific log IDs",
    multiple=True,
)
@click.option(
    "--build",
    default=[],
    help="Update specific builds",
    multiple=True,
)
@click.option(
    "--page-size",
    default=25,
    help="Number of logs to fetch per batch",
)
def update(
    boss: str,
    gate: int = None,
    difficulty: str = None,
    *,
    id: List[int] = [],
    specs: List[str] = [],
    page_size: int = 25,
):
    """
    Update the log IDs or builds for the BOSS, GATE, and DIFFICULTY.

    BOSS is required, GATE and DIFFICULTY should not be set unless necessary.
    """
    if len(id) == 0 and len(specs) == 0:
        raise ValueError("Either ID or build must be set.")

    # Start timer
    start = time.time()

    # Build a list of filter args
    bossArgs = []
    if boss == "all":
        for bossName, info in api.BOSSES.items():
            # If info is empty, it's probably a guardian
            if info == {}:
                bossArgs += [{"boss": bossName}]
            else:
                # Get the keys for difficulty
                for _, difficulties in info.items():
                    # Get gate
                    gate = int(bossName[-1:])

                    # Remove gate from bossName
                    bossName = bossName[:-3]

                    for diff in difficulties:
                        bossArgs += [
                            {"boss": bossName, "gate": gate, "difficulty": diff}
                        ]
    else:
        bossArgs += [{"boss": boss, "gate": gate, "difficulty": difficulty}]

    # Loop through bossArgs
    for kwargs in bossArgs:
        api.update_logs(
            **kwargs,
            ids=id,
            specs=specs,
            page_size=page_size,
        )

    # End timer
    end = time.time()
    click.echo(f"Time elapsed: {end - start:.2f} seconds")


if __name__ == "__main__":
    cli()
