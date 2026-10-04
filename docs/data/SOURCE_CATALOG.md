# Atlas Energy source catalogue

This catalogue defines the approved raw-data boundary for the academic Atlas Energy project.
The machine-readable source contracts in `configs/sources/` are authoritative; this document
explains how they are used.

The Kafka telemetry demonstration is generated inside Atlas and classified `SYNTHETIC`. It is not
an approved public or utility source; see `docs/data/TELEMETRY_STREAMING.md`.

## Acquisition policy

1. Python connectors acquire and validate data. Airflow only schedules those connectors.
2. Bronze objects preserve the exact bytes received from the publisher.
3. Every object has a JSON sidecar containing its URL, retrieval time, source period, content
   type, byte count, SHA-256 checksum, licence, attribution, and ingestion version.
4. A path is immutable. A publisher revision is stored in a new ingestion-date partition.
5. PDF text, OCR, tables, country filters, unit conversions, and joins are Silver operations.
6. Raw data is ignored by Git; code, source contracts, and transformation rules are versioned.

## Approved sources

- Weather: NASA POWER and Open-Meteo historical ERA5 data for the versioned Morocco locations.
- Macroeconomic context: World Bank Indicators API for Morocco (`MAR`).
- National reports: French ONEE, ANRE, and MEF official publications.
- Electricity history: Ember as the primary generation dataset; OWID for broader context and
  cross-checking. Because OWID incorporates Ember, matching values are not independent evidence.
- Renewable capacity and costs: IRENA official datasets and publications.
- Technology assumptions: a pinned PyPSA technology-data release for generic values, plus the
  Morocco-specific Zenodo dataset by Caillard (version 1.0, DOI 10.5281/zenodo.10427703).

## Target periods

- Hourly weather: 2020-2024, starting with the already acquired 2024 NASA POWER data.
- Annual statistical series and reports: 2010-2024 or the latest published observation.
- Technology assumptions: model years 2020, 2030, 2040, and 2050.

## Commands available now

```powershell
uv sync --extra dev --extra data
uv run atlas-ingest-open-meteo --config configs/locations/morocco.yml --year 2024
uv run atlas-ingest-world-bank --config configs/indicators/world_bank.yml
uv run atlas-ingest-files --config configs/downloads/owid.yml
uv run atlas-ingest-files --config configs/downloads/ember.yml
uv run atlas-ingest-files --config configs/downloads/technology_data.yml
uv run atlas-audit-bronze
uv run atlas-build-structured-silver
uv run atlas-build-report-silver
uv run atlas-build-zenodo-silver
```

The audit exits unsuccessfully only for integrity errors. A registered source with no acquired
objects is a warning while the catalogue is being populated.

## Structured Silver contract

`configs/silver/structured_sources.yml` selects the Morocco period, OWID metrics, and technology
model years. `atlas-build-structured-silver` verifies source checksums and builds separate Delta
tables for World Bank, OWID, Ember, IRENA, and PyPSA technology-data. Separate tables preserve
publisher definitions and prevent correlated sources from being mistaken for independent evidence.

The official-report contract is defined in `configs/silver/reports.yml`. It builds a complete page
index, raw cells from reviewed table pages, and curated metrics with page-level provenance. See
`docs/data/PDF_EXTRACTION.md` for its OCR and review rules.

`configs/silver/zenodo_technoeconomic.yml` contains the reviewed Morocco-to-PyPSA technology
mappings and comparison years. `atlas-build-zenodo-silver` creates source-aligned tables, a
unit-safe assumption comparison, and a versioned field-level data dictionary. See
`docs/data/ZENODO_MOROCCO_TECHNOECONOMIC.md`.

## Manual-assisted sources

Some IRENA downloads or government reports may require a browser interaction or acceptance of
publisher terms. Put the untouched original in `data/inbox/<source_id>/`. A registration command
will later validate the extension, MIME signature, checksum, and declared source URL before moving
it into Bronze. Do not rename, edit, resave, or convert a source file before registration.

Example after placing an untouched ANRE report in its inbox:

```powershell
uv run atlas-register-bronze `
  --source anre `
  --file data/inbox/anre/original-report.pdf `
  --source-url "https://anre.ma/path/from-the-publication-page.pdf" `
  --source-period 2024 `
  --attribution "ANRE, Rapport annuel 2024"
```
