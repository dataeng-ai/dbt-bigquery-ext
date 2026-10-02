from dbt.adapters.bigquery.impl import BigQueryAdapter
from dbt.adapters.bigquery.relation import BigQueryRelation


def test_make_relation_marker_id_from_list():
    assert BigQueryAdapter.make_relation_marker_id("ref", ["dim_products"]) == "ref:dim_products"
    assert (
        BigQueryAdapter.make_relation_marker_id("source", ["raw", "cv_jobs_raw"])
        == "source:raw:cv_jobs_raw"
    )


def test_make_relation_marker_id_sanitizes():
    assert (
        BigQueryAdapter.make_relation_marker_id("ref", ["pkg.name", "model-1"])
        == "ref:pkg_name:model_1"
    )


def test_relation_render_with_relation_marker():
    rel = BigQueryRelation.create(database="proj", schema="ds", identifier="tbl")
    marked = rel.incorporate(relation_marker="ref:tbl")
    assert marked.database == "proj"
    assert marked.schema == "ds"
    assert marked.identifier == "tbl"
    assert marked.render() == "/* <ref:tbl> */`proj`.`ds`.`tbl`/* <ref:tbl> */"
    assert str(marked) == marked.render()


def test_relation_include_preserves_relation_marker():
    rel = BigQueryRelation.create(database="proj", schema="ds", identifier="tbl")
    marked = rel.incorporate(relation_marker="source:raw:t")
    without_db = marked.include(database=False)
    assert without_db.relation_marker == "source:raw:t"
    assert without_db.render() == "/* <source:raw:t> */`ds`.`tbl`/* <source:raw:t> */"


def test_unmarked_relation_render_unchanged():
    rel = BigQueryRelation.create(database="proj", schema="ds", identifier="tbl")
    assert rel.render() == "`proj`.`ds`.`tbl`"


def test_should_mark_relations():
    class Cfg:
        def __init__(self, **kwargs):
            self._d = kwargs

        def get(self, key, default=None):
            return self._d.get(key, default)

    fn = BigQueryAdapter.should_mark_relations
    assert fn(None, None) is False
    assert fn(None, Cfg(materialized="incremental_ext")) is True
    assert fn(None, Cfg(materialized="script")) is True
    assert fn(None, Cfg(materialized="table")) is False
    assert fn(None, Cfg(materialized="table", mark_relations=True)) is True
    assert fn(None, Cfg(materialized="incremental_ext", mark_relations=False)) is False


def test_marker_helpers_are_available_on_adapter():
    assert "should_mark_relations" in BigQueryAdapter._available_
    assert "make_relation_marker_id" in BigQueryAdapter._available_
    assert "mark_relation" in BigQueryAdapter._available_
