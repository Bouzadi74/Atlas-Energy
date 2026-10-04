# Morocco techno-economic workbook: Silver contract

## Source and scope

Atlas uses version 1.0 of *Techno-economic dataset for long-term energy systems modelling in
Morocco*, published by Amandine Caillard on Zenodo under CC BY 4.0. The pinned record is
<https://zenodo.org/records/10427703> and its DOI is `10.5281/zenodo.10427703`.

The original `.xlsx` file remains unchanged in Bronze. The Silver build verifies its SHA-256
checksum, preserves workbook citations and cell-level lineage, and does not silently repair source
values. Its transformation contract is `zenodo-technoeconomic-silver-v1`.

## Reproduce the build

```powershell
uv sync --extra dev --extra data
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-21.0.7.6-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
uv run atlas-build-zenodo-silver
```

The reviewed comparison mappings and model years are versioned in
`configs/silver/zenodo_technoeconomic.yml`.

## Silver tables

- `morocco_electricity_capacity`: annual on-grid and off-grid installed capacity by source
  technology.
- `morocco_power_plant_assumptions`: 2020 capital cost, fixed cost, operating life, and average
  capacity factor.
- `morocco_renewable_capex`: annual renewable and storage capital-cost trajectories.
- `morocco_fossil_reserves`: published coal, oil, and gas reserve assumptions.
- `morocco_renewable_potential`: renewable supply-potential assumptions by year.
- `morocco_hydrogen_export_demand`: hydrogen export-demand scenarios by year.
- `morocco_technology_assumption_comparison`: explicit Zenodo-to-PyPSA mappings and comparison
  results for model years 2020, 2030, 2040, and 2050.
- `morocco_technoeconomic_data_dictionary`: one row per documented Silver field, including type,
  unit, nullability, definition, source sheet, and build lineage.

The build also writes a human-readable, versioned dictionary and a JSON manifest to
`data/silver/manifests/`. The current build version is `b77aa09691ef7fef`; it contains 110
field definitions across the seven analytical tables.

## Lineage and missing-value rules

Source-derived tables retain `source_id`, filename, Bronze checksum, ingestion date, worksheet,
row, citation, Silver version, and transformation timestamp. Tables parsed at cell level also keep
the Excel cell address and original formula.

`value_origin` has these meanings:

- `raw`: a literal numeric workbook cell;
- `formula_cached`: the published workbook contains a formula with a usable cached result;
- `formula_error`: the formula or cached result is an Excel error such as `#REF!`;
- `missing`: an empty source cell;
- `missing_marker`: a source marker such as `-`, which is not converted to zero.

## Assumption-comparison policy

Technology names are never joined by fuzzy matching. Every allowed mapping is reviewed and
versioned in configuration. A numeric difference is calculated only when both parameter meaning
and unit basis match exactly.

The comparison statuses are:

- `matched_comparable`: parameter and unit bases match; absolute and percentage differences are
  available;
- `matched_basis_mismatch`: the technology/parameter matches but currency year, currency, or cost
  basis differs;
- `pypsa_parameter_missing`: the mapped PyPSA technology has no corresponding parameter;
- `source_value_missing`: the Zenodo source value is blank or otherwise unavailable;
- `mapping_unavailable`: no approved cross-source technology mapping exists.

The first build produced 128 comparison rows: 16 comparable, 71 basis mismatches, 22 missing PyPSA
parameters, 3 missing Zenodo values, and 16 unmapped rows. The 16 directly comparable rows are
operating-life assumptions. Examples include coal at 35 versus 40 years, utility solar at 24
versus 35 years, hydropower at 50 versus 80 years, and battery storage at 15 versus 20 years.

These results do not establish which source is correct. They identify where scenario assumptions
need an explicit project choice before optimization.

## Known source-quality issue

The `Vietnam Energy Outlook` proxy row in the hydrogen sheet contains broken `#REF!` formulas from
2025 onward in the published workbook. The current build records 26 affected cells as null with
`value_origin = formula_error`. Atlas does not interpolate or replace them. The separate Morocco
roadmap scenario remains available as published.
