# Official report extraction

Atlas converts the registered ONEE, ANRE, and MEF reports to Silver without modifying the Bronze
PDFs. The workflow uses only free, local components: `pypdf` for page text, `pdfplumber` for
configured tables, and Spark with Delta Lake for storage.

## Run the build

```powershell
uv sync --extra dev --extra data
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-21.0.7.6-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
uv run atlas-build-report-silver
```

`configs/silver/reports.yml` is the reviewable contract. It contains energy keywords, table pages,
and curated metrics. Each metric must identify the original filename, one-based PDF page, printed
page, period, unit, short evidence text, and qualifier.

## Outputs

- `report_documents`: one row per registered report with page and extraction-status counts.
- `report_pages`: one row per PDF page with normalized embedded text and matched energy keywords.
- `report_table_cells`: raw cell coordinates and values from explicitly configured pages.
- `report_metrics`: visually verified facts suitable for later source reconciliation.
- `manifests/reports_<version>.json`: input checksums, configuration, row counts, and page counts.

All rows retain the source identifier, Bronze filename, SHA-256 checksum, ingestion date, and
deterministic Silver version.

## OCR and quality policy

Pages with enough embedded text are classified `embedded_text`. Short fragments are
`sparse_text`; pages with no extractable text are `ocr_required`. OCR-required pages remain in the
page index but are not automatically promoted to metrics. A future local Tesseract workflow must
retain the original page number, record the OCR engine and language pack, expose confidence, and
undergo visual review before its output can enter `report_metrics`.

OWID, Ember, IRENA, ANRE, ONEE, and MEF values remain source-aligned. Differences are evidence for
the reconciliation layer, not values to overwrite silently.
