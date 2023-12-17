from harlequin.options import (
    FlagOption,  # noqa
    ListOption,  # noqa
    PathOption,  # noqa
    SelectOption,  # noqa
    TextOption,
)

project = TextOption(
    name="project",
    description="The project ID to use for the BigQuery connection",
    short_decls=["-p"],
)

location = TextOption(
    name="location",
    description="The location to use for the BigQuery connection",
    short_decls=["-l"],
)

BIGQUERY_ADAPTER_OPTIONS = [project, location]
