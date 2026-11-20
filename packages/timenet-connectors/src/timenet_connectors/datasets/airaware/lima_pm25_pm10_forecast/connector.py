"""Build paired hourly pollution forecasts from the pinned SENAMHI export."""

# SPDX-License-Identifier: MIT

from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

import pyarrow as pa

from timenet.dataset import Record, Signal, Source, TimeFDataset
from timenet.errors import TimeFValidationError
from timenet.json import JsonMapping
from timenet.types import ForecastingTask, Split, TimeOrigin, TimeSeriesSpec, ureg
from timenet_connectors.datasets.airaware.lima_pm25_forecast.connector import (
    AXIS,
    FUTURE,
    HISTORY,
    HOUR,
    PM25,
    LimaPM25Connector,
    _key,
    _parse,
    _timestamp_key,
    type_reading,
)


CUT = datetime(2023, 4, 2, 7, tzinfo=UTC)
END = datetime(2024, 6, 1, tzinfo=UTC)
PM10 = TimeSeriesSpec(
    spec_type="pm10", name="PM10", unit_value=ureg.microgram / ureg.meter**3, dtype="float64", missing=None
)


def _station_columns(
    fine: dict[datetime, type_reading | None], coarse: dict[datetime, type_reading | None]
) -> tuple[list[datetime], list[tuple[TimeSeriesSpec, pa.Array, list[int]]]]:
    ticks, pm25, pm10 = [], [], []
    for t in sorted(fine.keys() & coarse.keys()):
        a, b = fine[t], coarse[t]
        if a is not None and b is not None and t < END:
            ticks.append(t)
            pm25.append(a)
            pm10.append(b)
    return ticks, [
        (spec, pa.array([v for v, _ in values]), [r for _, r in values])
        for spec, values in ((PM25, pm25), (PM10, pm10))
    ]


class LimaJointConnector(LimaPM25Connector):
    """Convert the historical two-pollutant training and validation tasks."""

    def convert(self, raw_refs: list[Path]) -> TimeFDataset:
        """Resolve both readings independently and preserve the source rows of each record."""
        (path,) = raw_refs
        body = path.read_bytes()
        if sha256(body).hexdigest() != self.source_sha256:
            raise TimeFValidationError("SENAMHI CSV SHA-256 mismatch")
        fine, coarse = (_parse(body, field, reject_nonfinite=True) for field in ("PM2_5", "PM10"))
        ds = TimeFDataset(metadata=self.metadata())
        for station in sorted(fine.keys() & coarse.keys()):
            ticks, channels = _station_columns(fine[station], coarse[station])
            series_ids = [self._series_key(station), _key([station, None, "PM10", "PM10", "ug/m3"])]
            for i in range(len(ticks) - HISTORY - FUTURE + 1):
                start, end = ticks[i], ticks[i + HISTORY + FUTURE - 1]
                if end - start != (HISTORY + FUTURE - 1) * HOUR or start < CUT <= end:
                    continue
                origin = ticks[i + HISTORY - 1]
                records = []
                for index, length in ((i, HISTORY), (i + HISTORY, FUTURE)):
                    record_metadata: JsonMapping = {
                        "station_key": station,
                        "series_ids": [*series_ids],
                        "source": "historico",
                        "unit_status": "inferred",
                        "quality": "unknown",
                        "available_at": None,
                        "first_label": ticks[index].isoformat(),
                    }
                    signals = []
                    for spec, values, rows in channels:
                        signals.append(
                            Signal.from_values(
                                values.slice(index, length), name=spec.spec_type, spec=spec, time_axis=AXIS
                            )
                        )
                        record_metadata[spec.spec_type + "_rows"] = [*rows[index : index + length]]
                    records.append(
                        ds.add_record(
                            record=Record(
                                sources=(Source(name=station, signals=tuple(signals)),),
                                start_time=TimeOrigin(ticks[index]),
                                metadata=record_metadata,
                            )
                        )
                    )
                ds.add_task(
                    task=ForecastingTask(
                        inputs=(records[0],),
                        targets=(records[1],),
                        split=Split.TRAIN if end < CUT else Split.VALIDATION,
                        metadata={
                            "window_id": _key([series_ids[0], *map(_timestamp_key, (origin, start, end))]),
                            "origin_at": origin.isoformat(),
                            "clock": "UTC",
                        },
                    )
                )
        return ds


CONNECTOR = LimaJointConnector
