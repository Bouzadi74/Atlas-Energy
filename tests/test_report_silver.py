from pathlib import Path

import pytest

from atlas.transforms.report_silver import (
    TABLE_SCHEMAS,
    ReportSilverConfig,
    classify_page,
    load_config,
    normalize_for_search,
    normalize_page_text,
    reuse_verified_build,
)


def test_report_config_has_unique_traceable_metrics() -> None:
    config = load_config(Path("configs/silver/reports.yml"))

    assert len(config.metrics) >= 20
    assert {metric.source_id for metric in config.metrics} == {"anre", "onee", "mef"}
    assert all(metric.page_number_pdf >= 1 for metric in config.metrics)
    assert all(metric.evidence for metric in config.metrics)


def test_page_text_normalization_and_classification() -> None:
    text = normalize_page_text(" Énergie\t renouvelable \n\n  12 017 MW\x00 ")

    assert text == "Énergie renouvelable\n12 017 MW"
    assert normalize_for_search(text).startswith("energie renouvelable")
    assert classify_page(text, 10) == "embedded_text"
    assert classify_page("court", 10) == "sparse_text"
    assert classify_page("", 10) == "ocr_required"


def test_report_config_rejects_duplicate_metric_ids(tmp_path: Path) -> None:
    config_path = tmp_path / "reports.yml"
    metric = """
  - {metric_id: duplicate, source_id: onee, source_file: report.pdf,
     page_number_pdf: 1, printed_page: '1', metric_name: Test,
     category: test, value: 1, unit: MW, period: '2024', geography: Morocco,
     qualifier: exact, evidence: Test evidence}
"""
    config_path.write_text(
        """version: 1
minimum_embedded_text_characters: 40
energy_keywords: [energie]
table_pages: []
metrics:
"""
        + metric
        + metric,
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate metric_id"):
        load_config(config_path)


def test_report_config_rejects_accent_equivalent_keywords() -> None:
    with pytest.raises(ValueError, match="energy_keywords"):
        ReportSilverConfig(
            version=1,
            minimum_embedded_text_characters=40,
            energy_keywords=["énergie", "energie"],
        )


def test_reuse_requires_complete_manifest(tmp_path: Path) -> None:
    class SparkThatMustNotRead:
        @property
        def read(self) -> None:
            raise AssertionError("incomplete build should not be read")

    output_root = tmp_path / "silver"
    manifest_dir = output_root / "manifests"
    manifest_dir.mkdir(parents=True)
    (manifest_dir / "reports_test.json").write_text(
        '{"silver_version":"test","table_rows":{"report_documents":6}}',
        encoding="utf-8",
    )

    result = reuse_verified_build(
        spark=SparkThatMustNotRead(), output_root=output_root, silver_version="test"
    )

    assert result is None
    assert set(TABLE_SCHEMAS) == {
        "report_documents",
        "report_pages",
        "report_table_cells",
        "report_metrics",
    }
