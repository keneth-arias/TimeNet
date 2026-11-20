# Lima hourly PM2.5 and PM10 forecasting

This connector builds `airaware/lima-pm25-pm10-forecast@0.1.0` from the pinned SENAMHI
CSV *Monitoreo de los contaminantes del aire en Lima Metropolitana*. AirAware Perú
adapted the measurements from seven provisional station identities into hourly forecast
windows. SENAMHI licenses the source database under [ODC-By-1.0](https://opendatacommons.org/licenses/by/1-0/).
The connector uses the TimeNet repository's MIT license.

Each task has one input record with 24 hourly measurements of each pollutant and one
target record with the following six hours. Select each signal by its name, `pm25` or
`pm10`. Both use inferred µg/m³. Records preserve each channel's CSV row numbers,
counting the header as row 1. Equal reports retain the first finite reading for each
pollutant. Missing hours, disagreeing finite readings and nonfinite values break a
window. Publication delays, measurement quality and interval alignment are unknown.

Training ends before April 2, 2023 at 07:00 UTC. Validation histories start at or
after that instant. This leaves 251,915 training and 44,458 validation windows and
purges 145 crossing the cutoff. All targets precede June 1, 2024. Both model input
variants use the same window identifiers, even when PM10 history is hidden from one
variant. AirAware evaluates subsequent provider records locally.

## Build offline

Place the original CSV, with the SHA-256 listed on the card, at
`${TIMENET_CACHE}/downloads/airaware/lima-pm25-pm10-forecast/senamhi-lima.csv`.
Build using the installed connector and an empty local registry.

```sh
timenet-build build airaware/lima-pm25-pm10-forecast@0.1.0 \
  --local-registry "$REGISTRY" --remote-registry "$REGISTRY" \
  --no-isolation --keep-cache
```

Open the version with `TimeFReader`, run `verify()`, and select either `train` or
`validation` when reading tasks. The [AirAware experiment](https://github.com/keneth-arias/airaware/issues/88)
documents the cohort, split, and paired evaluation.
