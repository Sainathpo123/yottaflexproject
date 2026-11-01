"""Dataset registry. Adding a feed means registering one contract here."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import sqlite3

from ..contracts.exceptions import UnknownDatasetError
from ..contracts.models import Candidate, RejectDecision
from . import customers, orders


Screen = Callable[
    [sqlite3.Connection, list[Candidate]],
    tuple[list[Candidate], list[RejectDecision]],
]
ParseRow = Callable[[dict[str, str], int], Candidate | RejectDecision]


def _keep_all(
    _connection: sqlite3.Connection,
    candidates: list[Candidate],
) -> tuple[list[Candidate], list[RejectDecision]]:
    return candidates, []


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    columns: tuple[str, ...]
    business_columns: tuple[str, ...]
    key_column: str
    version_column: str
    current_table: str
    history_table: str
    history_prefix: str
    parse: ParseRow
    screen: Screen


CUSTOMERS = DatasetSpec(
    name="customers",
    columns=customers.COLUMNS,
    business_columns=customers.BUSINESS_COLUMNS,
    key_column=customers.KEY_COLUMN,
    version_column=customers.VERSION_COLUMN,
    current_table=customers.CURRENT_TABLE,
    history_table=customers.HISTORY_TABLE,
    history_prefix=customers.HISTORY_PREFIX,
    parse=customers.parse_row,
    screen=_keep_all,
)

ORDERS = DatasetSpec(
    name="orders",
    columns=orders.COLUMNS,
    business_columns=orders.BUSINESS_COLUMNS,
    key_column=orders.KEY_COLUMN,
    version_column=orders.VERSION_COLUMN,
    current_table=orders.CURRENT_TABLE,
    history_table=orders.HISTORY_TABLE,
    history_prefix=orders.HISTORY_PREFIX,
    parse=orders.parse_row,
    screen=orders.screen_references,
)

DATASETS: dict[str, DatasetSpec] = {
    CUSTOMERS.name: CUSTOMERS,
    ORDERS.name: ORDERS,
}


def get_dataset(name: str) -> DatasetSpec:
    try:
        return DATASETS[name]
    except KeyError:
        raise UnknownDatasetError(name) from None


def dataset_names() -> tuple[str, ...]:
    return tuple(DATASETS)
