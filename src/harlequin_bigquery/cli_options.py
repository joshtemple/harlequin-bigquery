import re

from harlequin.options import TextOption


def is_valid_project(project: str) -> tuple[bool, str | None]:
    is_valid = (
        re.match(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$", project, flags=re.IGNORECASE)
        is not None
    )
    return (
        is_valid,
        "Must provide a valid project ID" if not is_valid else None,
    )


def is_valid_region(region: str) -> tuple[bool, str | None]:
    is_valid = (
        re.match(r"^[a-z][a-z0-9-]+[a-z0-9]$", region, flags=re.IGNORECASE) is not None
    )
    return (
        is_valid,
        "Must provide a valid region" if not is_valid else None,
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

BIGQUERY_ADAPTER_OPTIONS = [project, location]
