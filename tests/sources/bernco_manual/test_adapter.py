from datetime import UTC

from aqueduct_dagster.sources.bernco_manual.adapter import BerncoManualAdapter

_record = {
    "location_id": "4f92c895-6b41-42d6-be6b-508ee44812fa",
    "location_name": "9-Mile Hill LF",
    "latitude": 35.071526,
    "longitude": -106.778477,
    "alternate_id": {"id": "BC-0364", "agency": "BERNCO"},
    "readings": [{"timestamp": 1573689600, "value": 711.11}],
}


def test_to_thing_produces_correct_key():
    thing = BerncoManualAdapter([_record]).to_thing(_record)
    assert thing.external_key == "bernco-4f92c895-6b41-42d6-be6b-508ee44812fa"


def test_to_observations_returns_canonical_obs():
    observations = BerncoManualAdapter([_record]).to_observations(_record)
    assert len(observations) == 1
    assert (
        observations[0].datastream_external_key == "bernco-4f92c895-6b41-42d6-be6b-508ee44812fa-dtw"
    )
    assert observations[0].phenomenon_time.timestamp() == 1573689600
    assert observations[0].phenomenon_time.tzinfo == UTC
    assert observations[0].result == 711.11


def test_build_datastreams_returns_one_stream():
    adapter = BerncoManualAdapter([_record])
    datastream = adapter._build_datastreams(adapter.to_thing(_record))
    assert len(datastream) == 1
    assert datastream[0].external_key == "bernco-4f92c895-6b41-42d6-be6b-508ee44812fa-dtw"


def test_fully_successful_batch_has_no_failures():
    records = [_record, {**_record, "location_id": "4f92c895-6b41-42d6-be6b-508ee44812fb"}]
    adapter = BerncoManualAdapter(records)
    list(adapter.run())
    assert adapter.failure_count == 0


def test_bad_record_is_skipped_and_counted_as_failure():
    bad_record = {**_record}
    del bad_record["location_id"]
    adapter = BerncoManualAdapter([bad_record])
    bundles = list(adapter.run())
    assert bundles == []
    assert adapter.failure_count == 1


def test_mixed_batch_good_records_still_produce_bundles():
    bad_record = {**_record}
    del bad_record["location_id"]
    records = [
        _record,
        bad_record,
        {**_record, "location_id": "4f92c895-6b41-42d6-be6b-508ee44812fb"},
    ]
    adapter = BerncoManualAdapter(records)
    bundles = list(adapter.run())
    assert len(bundles) == 2
    assert adapter.failure_count == 1
