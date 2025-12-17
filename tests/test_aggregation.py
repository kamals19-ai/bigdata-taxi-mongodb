from db_scripts.agg_trips_pipeline import AGG_TRIPS_PIPELINE


def test_aggregation_pipeline_has_required_stages():
    """
    Ensures the documented MongoDB pipeline for agg_trips exists and has key stages.
    """
    assert isinstance(AGG_TRIPS_PIPELINE, list)
    assert len(AGG_TRIPS_PIPELINE) >= 2

    stage_keys = {list(stage.keys())[0] for stage in AGG_TRIPS_PIPELINE if isinstance(stage, dict) and stage}
    assert "$group" in stage_keys
    assert "$project" in stage_keys
