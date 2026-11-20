from datetime import timedelta
from hashlib import sha256

import pytest

from timenet.dataset import Record
from timenet.reader import TimeFReader
from timenet.types import ForecastingTask
from timenet_connectors.datasets.airaware.lima_pm25_pm10_forecast.connector import CUT, LimaJointConnector


def test_shared_windows_round_trip(tmp_path, monkeypatch):
    header = "ESTACION,LONGITUD,LATITUD,ALTITUD,DEPARTAMENTO,PROVINCIA,DISTRITO,FECHA,HORA,PM2_5,PM10"
    rows = [header]
    for i in range(-30, 32):
        t = CUT + timedelta(hours=i)
        prefix = f"A,-77,-12,1,LIMA,LIMA,A,{t:%Y%m%d},{t:%H%M%S}"
        rows.extend([f"{prefix},{i + 30},", f"{prefix},,{100 + i}"])
        if i == 30:
            rows.append(f"{prefix},,0")
    content = ("\n".join(rows) + "\n").encode()
    path = tmp_path / "senamhi.csv"
    path.write_bytes(content)
    monkeypatch.setattr(LimaJointConnector, "source_sha256", sha256(content).hexdigest())
    ds = LimaJointConnector().convert([path])
    with TimeFReader.open(ds.write(path=tmp_path / "local")) as reader:
        reader.verify()
        training, validation = list(reader.iter_tasks())
        assert [training.split, validation.split] == ["train", "validation"]
        assert isinstance(training, ForecastingTask) and training.targets is not None
        (x,), (y,) = training.inputs, training.targets
        assert isinstance(y, Record)
        assert isinstance(x.start_time.timestamp, int) and isinstance(y.start_time.timestamp, int)
        assert {s.name for s in x.signals} == {"pm25", "pm10"}
        assert {s.name: s for s in x.signals}["pm25"].to_arrow().to_pylist() == list(range(24))
        assert {s.name: s for s in y.signals}["pm10"].to_arrow().to_pylist() == list(range(94, 100))
        assert x.metadata["pm25_rows"] == list(range(2, 50, 2))
        assert y.metadata["pm10_rows"] == list(range(51, 63, 2))
        assert y.start_time.timestamp - x.start_time.timestamp == 24 * 3_600_000_000
    path.write_bytes(content + b" ")
    with pytest.raises(ValueError, match="SHA-256"):
        LimaJointConnector().convert([path])
