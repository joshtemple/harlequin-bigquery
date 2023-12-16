from harlequin.options import (
    FlagOption,  # noqa
    ListOption,  # noqa
    PathOption,  # noqa
    SelectOption,  # noqa
    TextOption,
)

foo = TextOption(
    name="foo",
    description="Help text goes here",
    short_decls=["-f"],
)

BIGQUERY_ADAPTER_OPTIONS = [foo]
