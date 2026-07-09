"""
tests/shared/test_gcs.py

Unit tests for the shared parquet/watermark helpers in shared/gcs.py.
All GCS and parquet I/O is mocked — no live GCS required.

Covers:
  _load_id_from_filename — dlt parquet filename parsing
  read_new_parquet_rows  — glob + watermark filtering + row_filter
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from aqueduct_dagster.shared.gcs import _load_id_from_filename, read_new_parquet_rows

# ── _load_id_from_filename ────────────────────────────────────────────────────


class TestLoadIdFromFilename:
    def test_extracts_load_id(self):
        path = "bucket/raw_pvacd/hydrovu_readings/year=2024/month=06/day=18/1781192390.555875.0.parquet"
        assert _load_id_from_filename(path) == 1781192390.555875

    def test_returns_none_for_unrecognized_name(self):
        assert _load_id_from_filename("bucket/dataset/not-a-load-id.parquet") is None


# ── read_new_parquet_rows ──────────────────────────────────────────────────────


def _mock_fs(files: list[str], tables: dict[str, dict]) -> MagicMock:
    """
    Build a mocked gcsfs.GCSFileSystem: fs.glob() returns `files`, and
    fs.open(path) yields a context manager whose identity is patched into
    pyarrow.parquet.read_table via the caller's `patch` block, keyed by path.
    """
    fs = MagicMock()
    fs.glob.return_value = files

    def _open(path, *_a, **_k):
        cm = MagicMock()
        cm.__enter__ = MagicMock(return_value=path)
        cm.__exit__ = MagicMock(return_value=False)
        return cm

    fs.open.side_effect = _open
    return fs


def _fake_read_table(tables: dict[str, dict]):
    def _read_table(fh):
        table = MagicMock()
        table.to_pydict.return_value = tables[fh]
        return table

    return _read_table


class TestReadNewParquetRows:
    def test_returns_empty_when_no_files(self):
        fs = _mock_fs([], {})
        rows, max_load_id = read_new_parquet_rows("bucket", "ds/*.parquet", None, fs)
        assert rows == []
        assert max_load_id is None

    def test_skips_files_at_or_below_watermark(self):
        files = [
            "bucket/ds/year=2024/month=01/day=01/100.0.0.parquet",
            "bucket/ds/year=2024/month=01/day=02/200.0.0.parquet",
        ]
        tables = {files[0]: {"v": [1]}, files[1]: {"v": [2]}}
        fs = _mock_fs(files, tables)
        with patch(
            "aqueduct_dagster.shared.gcs.pq.read_table", side_effect=_fake_read_table(tables)
        ):
            rows, max_load_id = read_new_parquet_rows("bucket", "ds/*.parquet", 100.0, fs)
        assert rows == [{"v": 2}]
        assert max_load_id == 200.0

    def test_applies_row_filter(self):
        files = ["bucket/ds/year=2024/month=01/day=01/100.0.0.parquet"]
        tables = {files[0]: {"parameter_id": ["4", "1"], "value": [10.0, 20.0]}}
        fs = _mock_fs(files, tables)
        with patch(
            "aqueduct_dagster.shared.gcs.pq.read_table", side_effect=_fake_read_table(tables)
        ):
            rows, _ = read_new_parquet_rows(
                "bucket",
                "ds/*.parquet",
                None,
                fs,
                row_filter=lambda row: row["parameter_id"] == "4",
            )
        assert rows == [{"parameter_id": "4", "value": 10.0}]

    def test_max_load_id_across_multiple_files(self):
        files = [
            "bucket/ds/year=2024/month=01/day=01/100.0.0.parquet",
            "bucket/ds/year=2024/month=01/day=02/300.0.0.parquet",
            "bucket/ds/year=2024/month=01/day=03/200.0.0.parquet",
        ]
        tables = {f: {"v": [i]} for i, f in enumerate(files)}
        fs = _mock_fs(files, tables)
        with patch(
            "aqueduct_dagster.shared.gcs.pq.read_table", side_effect=_fake_read_table(tables)
        ):
            rows, max_load_id = read_new_parquet_rows("bucket", "ds/*.parquet", None, fs)
        assert max_load_id == 300.0
        assert len(rows) == 3

    def test_ignores_files_without_parseable_load_id(self):
        files = ["bucket/ds/README.parquet"]
        fs = _mock_fs(files, {})
        rows, max_load_id = read_new_parquet_rows("bucket", "ds/*.parquet", None, fs)
        assert rows == []
        assert max_load_id is None
