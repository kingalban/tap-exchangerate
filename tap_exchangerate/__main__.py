from __future__ import annotations

from tap_exchangerate import get_exr_timeseries
from datetime import datetime
import argparse
import pathlib
import json


def emit(_type, *, record=None, schema=None):
    if _type == "SCHEMA":
        print(json.dumps({"type": "SCHEMA", "schema": schema}, default=str))

    elif _type == "RECORD":
        print(json.dumps({"type": "RECORD", "record": record}, default=str))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser("tap-exchangerate")
    parser.add_argument("-c", "--config", required=True, help="path to config file")
    parser.add_argument("--catalog", help="path to catalog file")
    parser.add_argument("-d", "--discover", action="store_true",
                        help="enable discover mode to generate catalog")

    args = parser.parse_args()

    if args.discover:
        raise NotImplemented  # TODO: implement discovery mode

    if args.config and args.catalog:
        config = json.loads(pathlib.Path(args.config).read_text())
        catalog = json.loads(pathlib.Path(args.catalog).read_text())  # TODO: change to recording breadcrumbs
        schema = json.loads((pathlib.Path(__file__).parent / "schemas/time-series.schema.json").read_text())

        emit("SCHEMA", schema=schema)

        for row in get_exr_timeseries(config["start_date"], datetime.today().strftime('%Y-%m-%d')):
            emit("RECORD", record=row._asdict())

    else:
        raise ValueError("both catalog and config must be specified outside of discovery mode")

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
