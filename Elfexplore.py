import json
import os
import time
from typing import List, Literal, Optional, Tuple, Union
import warnings
from datetime import datetime

import click
import pandas as pd
import requests
from dateutil import parser
from ratelimit import limits, sleep_and_retry
from yaspin import yaspin

SUPPORTS = ["Full Bloom", "Blessed Aura", "Desperate Salvation"]
BOSSES = {
    "Argeos": {},
    "Echidna G1": {
        "names": ["Red Doom Narkiel", "Agris"],
        "difficulties": ["Hard"],
    },
    "Echidna G2": {
        "names": [
            "Echidna",
            "Covetous Master Echidna",
            "Desire in Full Bloom, Echidna",
            "Alcaone, the Twisted Venom",
            "Agris, the Devouring Bog",
        ],
        "difficulties": ["Hard"],
    },
    "Behemoth G1": {
        "names": [
            "Behemoth, the Storm Commander",
            "Despicable Skolakia",
            "Untrue Crimson Yoho",
            "Ruthless Lakadroff",
            "Vicious Argeos",
        ],
        "difficulties": ["Normal"],
    },
    "Behemoth G2": {
        "names": ["Behemoth, Cruel Storm Slayer"],
        "difficulties": ["Normal"],
    },
    "Aegir G1": {
        "names": ["Akkan, Lord of Death", "Abyss Monarch Aegir"],
        "difficulties": ["Normal", "Hard"],
    },
    "Aegir G2": {
        "names": ["Aegir, the Oppressor", "Pulsating Giant's Heart"],
        "difficulties": ["Normal", "Hard"],
    },
}
logs_endpoint = os.getenv("LOGS_ENDPOINT")
ids_endpoint = os.getenv("IDS_ENDPOINT")

if logs_endpoint is None:
    warnings.warn("LOGS_ENDPOINT environment variable not set")
if ids_endpoint is None:
    warnings.warn("IDS_ENDPOINT environment variable not set")


class Filter:
    """Class for a query filter"""

    def __init__(
        self,
        *,
        boss: str,
        gate: Optional[int] = None,
        difficulty: Literal["Normal", "Hard", ""] = "",
        classes: Optional[List[str]] = None,
        sort: Literal["id", "dps", "duration", "fight_start"] = "id",
        order: Literal["asc", "desc"] = "asc",
        regions: Optional[List[str]] = None,
    ) -> None:
        self.boss = boss
        self.difficulty = difficulty
        self.gate = gate
        self.classes = classes if classes is not None else []
        self.sort = sort
        self.order = 1 if order == "asc" else 2
        self.regions = regions if regions is not None else []

        if gate is not None:
            self.bosses = BOSSES[f"{self.boss} G{gate}"]["names"]
        else:
            self.bosses = [self.boss]

    def to_dict(self) -> dict:
        """Convert the filter to a dictionary"""
        return {
            "bosses": self.bosses,
            "difficulty": self.difficulty if self.difficulty is not None else "",
            "sort": self.sort,
            "order": self.order,
            "regions": self.regions,
            "classes": self.classes,
        }

    def to_name(self) -> str:
        if self.gate is None:
            return f"{self.boss}"
        else:
            return f"{self.boss}_G{self.gate}_{self.difficulty}"

    def __repr__(self) -> str:
        return (
            f"Filter(boss={self.boss}, gate={self.gate}, difficulty={self.difficulty})"
        )


class ShortLog:
    def __init__(self, encounter: dict) -> None:
        self.id = encounter["id"]
        self.uploadedAt = parser.isoparse(encounter["uploadedAt"])
        self.boss = encounter["boss"]
        self.difficulty = encounter["difficulty"]
        self.timestamp = encounter["timestamp"]
        self.duration = encounter["duration"]
        self.version = encounter["version"]
        self.localPlayer = encounter["localPlayer"]
        self.region = encounter["region"]
        self.totalDamageDealt = encounter["totalDamageDealt"]
        self.totalDps = encounter["totalDps"]
        self.minGearScore = encounter["minGearScore"]
        self.maxGearScore = encounter["maxGearScore"]
        self.playerOverviews = [
            PlayerOverview(player, self) for player in encounter["playerOverviews"]
        ]
        self.weird = classify_weird(self)

    def to_dataframe(self, short=False) -> pd.DataFrame:
        # Make each player a row
        rows = []
        for player in self.playerOverviews:
            if short:
                rows.append(
                    {
                        "id": self.id,
                        "name": player.name,
                        "spec": player.spec,
                        "gearscore": player.gearScore,
                        "dps": player.dps,
                        "percent": player.percent,
                        "timestamp": self.timestamp,
                        "duration": self.duration,
                        "isDead": player.isDead,
                        "weird": self.weird,
                        "arkPassiveActive": player.arkPassiveActive,
                        "localPlayer": self.localPlayer == player.name,
                        "hasSpec": player.hasSpec,
                    }
                )
            else:
                rows.append(
                    {
                        "id": self.id,
                        "uploadedAt": self.uploadedAt,
                        "boss": self.boss,
                        "difficulty": self.difficulty,
                        "timestamp": self.timestamp,
                        "duration": self.duration,
                        "version": self.version,
                        "localPlayer": self.localPlayer,
                        "region": self.region,
                        "totalDamageDealt": self.totalDamageDealt,
                        "totalDps": self.totalDps,
                        "minGearScore": self.minGearScore,
                        "maxGearScore": self.maxGearScore,
                        "name": player.name,
                        "class": player.class_,
                        "spec": player.spec,
                        "dps": player.dps,
                        "percent": player.percent,
                        "gearScore": player.gearScore,
                        "isDead": player.isDead,
                        "deaths": player.deaths,
                        "arkPassiveActive": player.arkPassiveActive,
                        "weird": self.weird,
                        "hasSpec": player.hasSpec,
                    }
                )

        return pd.DataFrame(rows)


class PlayerOverview:
    def __init__(self, player: dict, log: ShortLog) -> None:
        self.name = player["name"]
        self.id = log.id
        self.class_ = player["class"]
        self.spec = player["spec"] if player["spec"] is not None else player["class"]
        self.dps = player["dps"]
        self.percent = player["dps"] / log.totalDps
        self.gearScore = player["gearScore"]
        self.isDead = player["isDead"]
        self.deaths = player["deaths"]
        self.arkPassiveActive = player["arkPassiveActive"] or False
        self.hasSpec = player["spec"] is not None


@sleep_and_retry
# @limits(calls=1, period=35)
def _call_logs_API(
    filter: Filter,
    search: str = "",
    page: int = 1,
    page_size: Literal[10, 25] = 25,
) -> requests.Response:
    """Call the logs API with a filter, returns a set of logs"""
    # Turn query_strings dict into a string
    body = {
        "filter": filter.to_dict(),
        "page": page,
        "pageSize": page_size,
        "search": search,
    }

    try:
        return requests.post(logs_endpoint, json=body)
    except:
        time.sleep(35)
        return requests.post(logs_endpoint, json=body)


def fetch_IDs(
    filter: Filter,
    parsed_logs: Optional[List[int]] = None,
    page_size: Literal[10, 25] = 25,
    verbose: bool = False,
) -> List[int]:
    if parsed_logs is None:
        parsed_logs = []

    # Calculate page given page size and parsed_logs
    page = (len(parsed_logs) // page_size) + 1

    if verbose:
        click.echo("Looking for log IDs")
        fetch_start = time.time()

    r = _call_logs_API(filter=filter, page=page, page_size=page_size)

    if r.status_code == 429:
        click.echo(f"Rate limited, waiting to retry.")
        r = exponential_backoff(
            lambda: _call_logs_API(filter=filter, page=page, page_size=page_size)
        )

        # rate_limit_start = time.time()
        # while r.status_code == 429:
        #     time.sleep(35)
        #     r = _call_logs_API(filter=filter, page=page, page_size=page_size)

        # click.echo(
        #     f"Rate limit wait time: {time.time() - rate_limit_start:.2f} seconds."
        # )

    try:
        data = json.loads(r.text)
    except json.JSONDecodeError:
        time.sleep(35)
        r = _call_logs_API(filter=filter, page=page, page_size=page_size)
        data = json.loads(r.text)

    # Get IDs
    ids = [log["id"] for log in data["encounters"] if log["id"] not in parsed_logs]

    if verbose:
        fetch_end = time.time()
        click.echo(
            f"Found {len(ids)} new logs within {fetch_end - fetch_start:.2f} seconds."
        )

    return ids


@sleep_and_retry
# @limits(calls=1, period=35)
def _call_ids_API(ids: List[int]) -> requests.Response:
    """Call the log API with a log ID, returns the log data"""
    try:
        return requests.post(ids_endpoint, json=ids)
    except:
        time.sleep(35)
        return requests.post(ids_endpoint, json=ids)


def exponential_backoff(fun):
    tries = 0
    total_time = 0
    with yaspin(text="", color="cyan", timer=True) as sp:
        while True:
            r = fun()
            if r.status_code == 429:
                wait_time = 2**tries
                total_time += wait_time
                sp.text = f"Attempt {tries + 1}, Waiting {wait_time} seconds to retry. Total wait time: {total_time}s."
                tries += 1
                time.sleep(wait_time)
            else:
                sp.text = f"{tries} attempts, over {total_time}s."
                sp.ok(f"Success!")
                return r


def fetch_logs(
    ids: List[int], form: Literal["long", "short", "both"] = "long", verbose=False
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, pd.DataFrame]]:
    r = _call_ids_API(ids)

    if r.status_code == 429:
        click.echo(f"Rate limited, backing off.")
        r = exponential_backoff(lambda: _call_ids_API(ids))

        # rate_limit_start = time.time()
        # while r.status_code == 429:
        #     time.sleep(35)
        #     r = _call_ids_API(ids)

        # click.echo(
        #     f"Rate limit wait time: {time.time() - rate_limit_start:.2f} seconds."
        # )
    elif r.status_code == 404:
        # Try once again
        time.sleep(35)
        r = _call_ids_API(ids)

        if r.status_code == 404:
            click.echo(f"Log {ids} not found.")
            return pd.DataFrame()

    try:
        data = json.loads(r.text)
    except json.JSONDecodeError:
        time.sleep(35)
        r = _call_ids_API(id)
        data = json.loads(r.text)

    logs = [ShortLog(log) for log in data]

    if verbose:
        click.echo(
            f"Last log date time: {str(datetime.fromtimestamp(logs[-1].timestamp/1000))}"
        )

    # Return a big dataframe with all log dataframes concatenated
    if form == "long":
        return pd.concat([log.to_dataframe() for log in logs])
    elif form == "short":
        return pd.concat([log.to_dataframe(short=True) for log in logs])
    elif form == "both":
        return pd.concat([log.to_dataframe() for log in logs]), pd.concat(
            [log.to_dataframe(short=True) for log in logs]
        )
    else:
        raise ValueError("form should be 'long', 'short', or 'both'")


def classify_weird(encounter: ShortLog) -> bool:
    # Doesn't have have the right number of players
    nPlayers = len(encounter.playerOverviews)
    if nPlayers not in [4, 8, 16]:
        return True

    player_specs = [player.spec for player in encounter.playerOverviews]
    # Has Princess GL
    if "Princess" in player_specs:
        return True

    # Does not have the expected number of supports
    nSupports = len([spec for spec in player_specs if spec in SUPPORTS])
    if nPlayers // 4 != nSupports:
        return True

    # Player without a spec
    if "Unknown" in player_specs:
        return True

    return False


def scrape_log(
    boss: str,
    gate: int = None,
    difficulty: str = None,
    from_scratch: bool = False,
    page_size: Literal[10, 25] = 25,
    max_logs: Optional[int] = None,
    force: bool = False,
    verbose: bool = False,
) -> None:
    click.echo(
        f"Fetching logs for {boss} {gate if gate is not None else ''} {difficulty if difficulty is not None else ''}"
    )

    # Start timer
    start = time.time()

    # Make filter
    filter = Filter(boss=boss, gate=gate, difficulty=difficulty)

    if from_scratch:
        click.echo("")
        click.echo("=== Starting from scratch ===")
        click.echo("WARNING: THIS OVERWRITES OLD LOGS")
        if not force:
            click.confirm("Are you sure you want to continue?", abort=True)
        else:
            click.echo("Continuing without confirmation in three seconds.")
            time.sleep(3)

        df = pd.DataFrame()
        oldIDs = []

        # Delete the old long data file
        try:
            os.remove(f"./data_long/{filter.to_name()}_long.csv")
        except FileNotFoundError:
            pass

    else:
        # Try to load old data file
        try:
            df = pd.read_parquet(f"./data/{filter.to_name()}.parquet")
            oldIDs = df["id"].unique()

        except FileNotFoundError:
            df = pd.DataFrame()
            oldIDs = []

    # Fetch logs until we hit max
    newLogsParsed = 0
    while max_logs is None or newLogsParsed < max_logs:
        logIDs = fetch_IDs(
            filter,
            parsed_logs=oldIDs,
            page_size=page_size,
            verbose=verbose,
        )

        nLogs = len(logIDs)
        if nLogs == 0:
            click.echo(
                f"Empty batch of logs with a page size of {page_size} (page {nLogs // page_size})."
            )
            break

        if verbose:
            click.echo(f"Getting log info for {nLogs} logs.")
            log_start = time.time()

        short_log = fetch_logs(logIDs, form="short", verbose=verbose)

        if verbose:
            log_end = time.time()
            click.echo(
                f"Total time elapsed: {log_end - log_start:.2f} seconds for {nLogs} logs."
            )

        df = pd.concat([df, short_log])
        newLogsParsed += nLogs

        oldIDs = df["id"].unique()

        if verbose:
            click.echo(
                f"Batch complete, saving logs. Total of {newLogsParsed} logs scraped."
            )
            click.echo("")

        # Save to parquet (saves once per batch)
        df.to_parquet(f"./data/{filter.to_name()}.parquet", index=False)
        # long_path = f"./data_long/{filter.to_name()}_long.csv"
        # long_log.to_csv(
        #     long_path,
        #     mode="a",
        #     header=not os.path.exists(long_path),
        #     index=False,
        # )

    # End timer
    end = time.time()
    click.echo(f"Time elapsed: {end - start:.2f} seconds")
    click.echo(f"Logs scraped: {newLogsParsed}")
    click.echo("==========")


def update_logs(
    boss: str,
    gate: int = None,
    difficulty: str = None,
    *,
    ids: List[int] = [],
    specs: List[str] = [],
    page_size: int = 25,
) -> None:
    """
    Update the logs for a specific boss, gate, difficulty based on an ID or specs.
    """
    raise NotImplementedError("This function is not updated yet.")
    if page_size > 25:
        raise ValueError("Batch size should be less than 25")

    # Only one of IDs or builds should be set
    if len(ids) > 0 and len(specs) > 0:
        raise ValueError("Either ID or build must be set.")

    # Load the data for the encounter
    filter = Filter(boss=boss, gate=gate, difficulty=difficulty)
    data = pd.read_csv(f"./data_long/{filter.to_name()}_long.csv")

    # Get IDs to update
    toUpdate = list(ids)
    if len(specs) > 0:
        toUpdate += list(data.loc[data["spec"].isin(specs)]["id"].unique())

    # Remove the to be updated IDs from data
    data = data[~data["id"].isin(toUpdate)]

    # Fetch the logs
    click.echo(f"Updating logs for {filter}")
    click.echo(f"Updating {len(toUpdate)} logs")
    if len(specs) > 0:
        click.echo(f"Updating logs for specs: {', '.join(specs)}")

    # Calculate batches
    nBatches = len(toUpdate) // page_size
    if len(toUpdate) % page_size != 0:
        nBatches += 1

    with click.progressbar(range(nBatches)) as bar:
        for i in bar:
            ids = toUpdate[i * page_size : (i + 1) * page_size]
            log = fetch_logs(ids)

            data = pd.concat([data, log])

    # Turn the data long form to the short form
    data_short = data.loc[
        :,
        [
            "id",
            "name",
            "spec",
            "gearscore",
            "dps",
            "percent",
            "date",
            "duration",
            "isDead",
            "weird",
            "arkPassiveActive",
            "localPlayer",
        ],
    ]

    # Save the data
    data.to_csv(f".{filter.to_name()}_long.csv", index=False)
    data_short.to_csv(f".{filter.to_name()}.csv", index=False)