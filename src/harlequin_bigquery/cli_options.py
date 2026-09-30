from __future__ import annotations

import re

from harlequin.options import TextOption


def is_valid_project(project: str | None) -> tuple[bool, str | None]:
    if project is None:
        return True, None
    is_valid = (
        re.match(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$", project, flags=re.IGNORECASE)
        is not None
    )
    return (
        is_valid,
        "Must provide a valid project ID" if not is_valid else None,
    )


def is_valid_region(region: str | None) -> tuple[bool, str | None]:
    if region is None:
        return True, None
    is_valid = (
        re.match(r"^[a-z][a-z0-9-]+[a-z0-9]$", region, flags=re.IGNORECASE) is not None
    )
    return (
        is_valid,
        "Must provide a valid region" if not is_valid else None,
    )


def is_valid_dataset(dataset: str | None) -> tuple[bool, str | None]:
    if dataset is None:
        return True, None
    # Dataset IDs must be alphanumeric (plus underscores) and max 1024 characters
    is_valid = (
        re.match(r"^[a-zA-Z_][a-zA-Z0-9_]{0,1023}$", dataset) is not None
    )
    return (
        is_valid,
        "Must provide a valid dataset ID" if not is_valid else None,
    )


project = TextOption(
    name="project",
    description="The project ID to use for the BigQuery connection",
    short_decls=["-p"],
    validator=is_valid_project,
)

location = TextOption(
    name="location",
    description="The location to use for the BigQuery connection",
    short_decls=["-l"],
)

dataset = TextOption(
    name="default-dataset",
    description="The default dataset to use for unqualified table references",
    short_decls=["-D"],
    validator=is_valid_dataset,
)

BIGQUERY_ADAPTER_OPTIONS = [project, location, dataset]
