# CROP RUN
# ========== HYBRID (OPTION A) TEST — 1 FULL DOCUMENT ==========
import dataclasses, asyncio
from preprocessing_etl.custom.ai_parse_pipeline import run_ai_parse_batch, set_crop_debug_dir  # <-- add set_crop_debug_dir
from preprocessing_etl.custom.storage import DeltaLakeStorage
from preprocessing_etl.search.ai_search_store import AzureAISearchStore
from preprocessing_etl.shared.config import CONFIG

# --- isolate to v5_ ---
_orig = CONFIG.storage
CONFIG.storage = dataclasses.replace(CONFIG.storage, table_prefix="v5_")
storage = DeltaLakeStorage(spark=spark)
search_store = AzureAISearchStore()

# --- reuse the authenticated client from stage 00 ---
client = extractor._client

# --- fresh-loop async runner ---
def run_async(coro):
    return asyncio.new_event_loop().run_until_complete(coro)

# --- dedicated image folder (cleared per document) ---
IMG_OUT = "/Volumes/<catalog>/<schema>/<volume>/ai_parse_figures"

# --- ADD THIS: folder to save the chart crops for inspection ---
CROP_DEBUG = "/Volumes/<catalog>/<schema>/<volume>/crop_debug"
dbutils.fs.mkdirs(CROP_DEBUG)
set_crop_debug_dir(CROP_DEBUG)

# --- run ---
result = run_ai_parse_batch(
    spark, submissions, storage,
    search_store=search_store,
    use_sonnet=True,
    llm_client=client,
    run_async_fn=run_async,
    image_output_path=IMG_OUT,
    dbutils=dbutils,
    limit=1)

set_crop_debug_dir(None)   # <-- ADD THIS: turn off crop-saving after the run

print(f"docs={result.documents} claims={result.claims_total} "
      f"gold={result.gold_total} review={result.review_total} "
      f"quarantined={result.quarantined} indexed={result.indexed_chunks}")
if result.failures:
    for f, e in result.failures: print(f"  FAILED {f}: {e}")

CONFIG.storage = _orig
spark.sql("SHOW TABLES IN docextract.reporting LIKE 'v5_*'").show(truncate=False)

# AI PARSE PIPELINE

"""Production orchestrator for the ai_parse extraction branch.

Wraps the full operational flow into ONE entry point so the notebook stays thin
and non-technical operators don't hand-edit pipeline internals:

    ai_parse_document -> rule-based structuring -> gate for review
      -> write gold / silver / review / quarantine -> audit log -> search index

Call ``run_ai_parse_batch(spark, submissions, storage, ...)`` and it does the
lot, returning a small summary. All the moving parts (gating, quarantine on
failure, audit events, indexing) live here, not in the notebook.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

from .hybrid_extraction import hybrid_extract_document
from .validator import gate_for_review


@dataclass
class BatchResult:
    run_id: str
    documents: int = 0
    claims_total: int = 0
    gold_total: int = 0
    review_total: int = 0
    quarantined: int = 0
    indexed_chunks: int = 0
    failures: list = field(default_factory=list)


def _run_ai_parse(spark, path: str, image_output_path: str = None):
    """Run ai_parse_document on one file. Uses the DEFAULT call (no
    descriptionElementTypes option): ai_parse handles figures automatically and
    populates their ``content`` with the read values (verified — the plain call
    returns the figure value blob). Adding descriptionElementTypes changed that
    behaviour (moved values to a generic description), suppressing figure
    extraction — so we use the default. The parser reads whatever ai_parse
    populates (content or description).

    When ``image_output_path`` is given (Option A), ai_parse also RENDERS each
    figure to an image there, so figures can be sent to Sonnet for accurate
    (colour-coded) reading."""
    (spark.read.format("binaryFile").load(path)
        .createOrReplaceTempView("_aip_doc"))
    if image_output_path:
        return spark.sql(
            f"SELECT ai_parse_document(content, "
            f"map('imageOutputPath','{image_output_path}')) AS p FROM _aip_doc"
        ).collect()[0]["p"]
    return spark.sql(
        "SELECT ai_parse_document(content) AS p FROM _aip_doc"
    ).collect()[0]["p"]


# optional debug: when set (via set_crop_debug_dir), each chart crop sent to
# Sonnet is also written here as a PNG for inspection. None = don't save.
_SAVE_CROPS_TO = None


def set_crop_debug_dir(path):
    """Notebook helper: set a Volumes folder to save each chart crop to (for
    inspection), or None to turn it off."""
    global _SAVE_CROPS_TO
    _SAVE_CROPS_TO = path


def _build_figure_crop_fn(spark, image_output_path, dbutils_ref,
                          all_elements=None, save_crops_to=None):
    """Build a crop_figure_image(el) callable for Option A.

    ai_parse renders WHOLE PAGES to images (each page image contains that page's
    logos, tables, text AND figures). Sending a whole page to Sonnet would make
    it re-read the tables/text we already extracted -> duplication. So we CROP
    just the figure's region out of its page image using the figure's bbox, and
    send only that crop to Sonnet. Sonnet then reads only the chart.

    Mapping: page images are hash-named, so we order them by modification time
    and map page N -> the Nth image (the folder holds only THIS document's pages,
    cleared per document). The figure's bbox (coords relative to the rendered
    page image) gives the crop rectangle.
    """
    from PIL import Image
    from ..search.figure_preprocessor import encode_image_base64

    try:
        imgs = sorted(dbutils_ref.fs.ls(image_output_path),
                      key=lambda f: f.modificationTime)
    except Exception:
        imgs = []

    def _page_image(page_id):
        # page_id is 1-based; images are in page order (folder cleared per doc)
        idx = (page_id - 1) if page_id else 0
        if idx < 0 or idx >= len(imgs):
            return None
        p = imgs[idx].path
        if p.startswith("dbfs:/Volumes"):
            p = p.replace("dbfs:", "")
        elif p.startswith("dbfs:"):
            p = p.replace("dbfs:", "/dbfs")
        try:
            return Image.open(p)
        except Exception:
            return None

    _counter = {"n": 0}

    def crop(el):
        import base64 as _b64, io as _io
        # region = figure + adjacent header/legend (layout-agnostic via
        # chart_region), else the figure's own bbox page, else whole page.
        region = None
        if all_elements is not None:
            try:
                from .chart_region import chart_crop_region
                region = chart_crop_region(el, all_elements)
            except Exception:
                region = None
        bbox = getattr(el, "bbox", None)
        page_id = (region or {}).get("page_id") or (
            bbox[0].get("page_id", 1) if bbox and isinstance(bbox, list)
            and isinstance(bbox[0], dict) else 1)
        page_img = _page_image(page_id)
        if page_img is None:
            return None
        try:
            rgb = page_img.convert("RGB")
            coord = (region or {}).get("coord")
            if coord:
                rgb = rgb.crop(tuple(int(x) for x in coord))    # chart + legend
            if save_crops_to:
                try:
                    _counter["n"] += 1
                    outp = f"{save_crops_to}/crop_{_counter['n']}.png".replace("dbfs:", "")
                    rgb.save(outp)
                except Exception:
                    pass
            buf = _io.BytesIO()
            rgb.save(buf, format="PNG")            # full-res genuine PNG
            buf.seek(0)
            return _b64.b64encode(buf.read()).decode()
        except Exception:
            return None

    return crop

    return crop


def _index_ai_parse_elements(parsed_response, sub, search_store, entity_ref,
                             claims=None):
    """Index ai_parse's content into the search store, formatted for
    READABILITY by type:
      * table  -> clean MARKDOWN table (not raw HTML)
      * text   -> the prose as-is
      * figure -> the EXTRACTED chart VALUES (from Sonnet/rule extraction) as
                  readable lines, not the generic description — so chart data is
                  actually searchable. Falls back to the description if no claims.
    Best-effort — never breaks the batch."""
    if search_store is None:
        return 0
    try:
        from .hybrid_extraction import parse_ai_parse_response
        from ..shared.schema import RegulatoryChunk, ChunkType, content_hash
    except Exception:
        return 0

    # group the extracted claims by the page they came from, so a figure's page
    # can be indexed with its chart values.
    figure_claims_by_page = {}
    for c in (claims or []):
        if getattr(c, "model_used", "") in ("sonnet_chart_digitiser",
                                            "ai_parse_desc_structured"):
            figure_claims_by_page.setdefault(getattr(c, "page", None), []).append(c)

    elements = parse_ai_parse_response(parsed_response)
    chunks = []
    for el in elements:
        if el.el_type in ("page_header", "page_footer", "page_number"):
            continue
        if el.el_type == "table":
            try:
                from .ai_parse_structurer import html_table_to_markdown
                text = html_table_to_markdown(el.content or "") or (el.content or "")
            except Exception:
                text = el.content or ""
        elif el.el_type == "figure":
            # prefer the EXTRACTED chart values (what Sonnet/rules read) as
            # readable lines — that's the searchable chart data. Fall back to the
            # description only if no values were extracted for this page.
            fcs = figure_claims_by_page.get(el.page, [])
            if fcs:
                lines = [f"{getattr(c, 'field_name', '') or c.canonical_metric} = "
                         f"{c.value}" + (f" {c.unit}" if getattr(c, 'unit', None) else "")
                         for c in fcs]
                head = (el.description or "Chart").strip()
                text = head + "\n" + "\n".join(lines)
            else:
                text = (el.description or "") or (el.content or "")
        else:  # text, caption, footnote — index as-is
            text = el.content or el.description or ""
        text = text.strip()
        if not text or len(text) < 3:
            continue
        try:
            chunks.append(RegulatoryChunk(
                chunk_id=content_hash(f"{sub['path']}-{el.page}-{text[:40]}"),
                chunk_type=ChunkType.RAW_TEXT,
                content=text,
                entity_ref=entity_ref or sub.get("entity_ref") or "",
                source_document_id=os.path.basename(sub["path"]),
                content_hash=content_hash(text),
                page=el.page,
                team=sub.get("team"),
                report_type=sub.get("report_type"),
                metrics={"page": el.page, "element_type": el.el_type,
                         "team": sub.get("team"),
                         "report_type": sub.get("report_type")}))
        except Exception:
            continue
    if chunks:
        try:
            # provision the vector index (ops_search_index) if it doesn't exist,
            # THEN write the chunks. The main pipeline does this in stage 4; the
            # ai_parse branch must do it too or only ops_search_chunks is created.
            if hasattr(search_store, "ensure_index"):
                search_store.ensure_index()
            search_store.index_many(chunks)
        except Exception:
            return 0
    return len(chunks)


def run_ai_parse_batch(spark, submissions, storage, *, audit=None,
                       search_store=None, limit=None, use_sonnet=False,
                       llm_client=None, run_async_fn=None) -> BatchResult:
    """Run the ai_parse branch over a batch of submissions, end-to-end.

    Args:
      spark        — the Spark session.
      submissions  — list of {path, team, report_type, entity_ref}.
      storage      — a DeltaLakeStorage (already prefix-configured by caller).
      audit        — optional AuditLog; created if None.
      search_store — optional vector store for indexing (RAG). None -> no index.
      limit        — process only the first N submissions (None = all).
      use_sonnet   — escalate hard figures to Sonnet (needs llm_client +
                     run_async_fn). Default False = pure rule-based (cheapest).

    Returns a BatchResult summary. Writes gold, silver, review, quarantine,
    audit, and (if search_store given) the index — the full operational set.
    """
def run_ai_parse_batch(spark, submissions, storage, *, audit=None,
                       search_store=None, limit=None, use_sonnet=False,
                       llm_client=None, run_async_fn=None,
                       image_output_path=None, dbutils=None) -> BatchResult:
    """Run the ai_parse branch over a batch of submissions, end-to-end.

    Args:
      spark        — the Spark session.
      submissions  — list of {path, team, report_type, entity_ref}.
      storage      — a DeltaLakeStorage (already prefix-configured by caller).
      audit        — optional AuditLog; created if None.
      search_store — optional vector store for indexing (RAG). None -> no index.
      limit        — process only the first N submissions (None = all).
      use_sonnet   — send FIGURES (only) to Sonnet for accurate colour-coded
                     chart reading (Option A). Tables/text stay rule-based.
                     Needs llm_client + run_async_fn + image_output_path +
                     dbutils.
      image_output_path — a DEDICATED Unity Catalog volume folder. When set with
                     use_sonnet, ai_parse renders each figure there and Sonnet
                     reads it. Cleared PER DOCUMENT so the figure->image mapping
                     stays unambiguous.
      dbutils      — the notebook's dbutils (for clearing/listing the volume).

    Returns a BatchResult summary. Writes gold, silver, review, quarantine,
    audit, and (if search_store given) the index — the full operational set.
    """
    from ..shared.audit_log import AuditLog
    audit = audit or AuditLog()
    result = BatchResult(run_id=str(uuid.uuid4()))
    option_a = bool(use_sonnet and image_output_path and dbutils
                    and llm_client and run_async_fn)

    subs = submissions[:limit] if limit else submissions
    for sub in subs:
        path = sub["path"]
        fname = os.path.basename(path)
        try:
            crop_fn = None
            if option_a:
                # clear the image folder PER DOCUMENT so it holds only this
                # document's rendered figures (hash-named, mapped by order).
                try:
                    dbutils.fs.rm(image_output_path, recurse=True)
                    dbutils.fs.mkdirs(image_output_path)
                except Exception:
                    pass
                parsed = _run_ai_parse(spark, path, image_output_path)
            else:
                parsed = _run_ai_parse(spark, path)
            audit.log(result.run_id, "ai_parse", path, detail=f"parsed {fname}")

            # tag each figure with its index (position among figures) so the
            # crop fn can map it to the correct rendered image.
            crop_fn = None
            if option_a:
                from .hybrid_extraction import parse_ai_parse_response
                _els = parse_ai_parse_response(parsed)
                fi = 0
                for e in _els:
                    if e.el_type == "figure":
                        e.figure_index = fi
                        fi += 1
                # build the crop fn WITH the elements (so chart_region can find
                # each chart's adjacent header/legend) and an optional debug save
                # path from the module global set by the notebook.
                crop_fn = _build_figure_crop_fn(
                    spark, image_output_path, dbutils,
                    all_elements=_els, save_crops_to=_SAVE_CROPS_TO)

            claims = hybrid_extract_document(
                ai_parse_response=parsed, filename=fname,
                team=sub["team"], report_type=sub["report_type"],
                entity_ref=sub.get("entity_ref"),
                use_sonnet=option_a, llm_client=llm_client,
                run_async_fn=run_async_fn, crop_figure_image=crop_fn)
            audit.log(result.run_id, "extract", path,
                      detail=f"{len(claims)} claims")

            gated = gate_for_review(claims, [])
            clean, review = gated["clean"], gated["needs_review"]

            # derive the report date (best-effort; never break the doc over it)
            try:
                from .storage import derive_report_date
                rdate = derive_report_date(claims, None)
            except Exception:
                rdate = None

            # parse entity_id / entity_name from the document's volume path so
            # they become recognisable columns (submission may already carry them).
            from .entity_path import parse_entity_path
            ent = parse_entity_path(path)
            entity_id = sub.get("entity_id") or ent["entity_id"]
            entity_name = sub.get("entity_name") or ent["entity_name"]

            storage.write_silver(claims, team=sub["team"],
                                 report_type=sub["report_type"],
                                 report_date=rdate, entity_id=entity_id,
                                 entity_name=entity_name)
            result.gold_total += storage.write_gold_metrics(
                clean, team=sub["team"], report_type=sub["report_type"],
                report_date=rdate, entity_id=entity_id, entity_name=entity_name)
            if review:
                storage.write_review_claims(
                    review, team=sub["team"], report_type=sub["report_type"],
                    report_date=rdate, entity_id=entity_id, entity_name=entity_name)
            audit.log(result.run_id, "store", path,
                      detail=f"gold={len(clean)} review={len(review)}")

            n_idx = _index_ai_parse_elements(
                parsed, sub, search_store, sub.get("entity_ref"), claims=claims)
            result.indexed_chunks += n_idx

            result.documents += 1
            result.claims_total += len(claims)
            result.review_total += len(review)

        except Exception as e:
            try:
                storage.write_quarantine([{
                    "kind": "document", "source_element_id": fname,
                    "error": str(e)[:200], "source_document": path,
                    "team": sub["team"], "report_type": sub["report_type"],
                    "entity_ref": sub.get("entity_ref")}])
            except Exception:
                pass
            audit.log(result.run_id, "error", path, error=str(e)[:200])
            result.quarantined += 1
            result.failures.append((fname, str(e)[:150]))

    # flush the audit events to ops_audit_events.
    try:
        storage.write_audit(audit.to_rows())
    except Exception:
        pass
    _log_run_to_mlflow(result, run_name="ai_parse_batch",
                       params={"use_sonnet": use_sonnet, "limit": limit})
    return result


def _log_run_to_mlflow(result, run_name="pipeline_run", params=None):
    """Log the run's scorecard metrics to MLflow — only when MLflow is present
    and enabled in config. Safe no-op otherwise (never breaks the run)."""
    try:
        from ..shared.config import CONFIG
        if not getattr(getattr(CONFIG, "observability", None), "mlflow_tracing", False):
            return
        import mlflow
        from .scorecard import build_scorecard
        sc = build_scorecard(result)
        with mlflow.start_run(run_name=run_name):
            if params:
                mlflow.log_params(params)
            for section in ("coverage", "quality", "robustness", "cost"):
                for k, v in sc[section].items():
                    if isinstance(v, (int, float)):
                        mlflow.log_metric(k, v)
    except Exception:
        pass


def run_ai_extract_batch(spark, submissions, storage, *, audit=None,
                         search_store=None, limit=None, mode="precision"):
    """Run the ai_parse -> ai_extract branch over a batch, end-to-end, with the
    full operational set (gate, gold/silver/review/quarantine, audit, index) —
    the LLM-structured alternative to run_ai_parse_batch. Requires ai_extract to
    be available/approved. Returns a BatchResult summary."""
    from ..shared.audit_log import AuditLog
    from .ai_extract_branch import build_ai_extract_sql, ai_extract_result_to_claims
    audit = audit or AuditLog()
    result = BatchResult(run_id=str(uuid.uuid4()))

    subs = submissions[:limit] if limit else submissions
    for sub in subs:
        path = sub["path"]
        fname = os.path.basename(path)
        try:
            (spark.read.format("binaryFile").load(path)
                .createOrReplaceTempView("_aix_doc"))
            sql = build_ai_extract_sql("_aix_doc", mode=mode)
            extracted = spark.sql(sql).collect()[0]["extracted"]
            audit.log(result.run_id, "ai_extract", path, detail=f"extracted {fname}")

            claims = ai_extract_result_to_claims(
                extracted, fname, team=sub["team"],
                report_type=sub["report_type"], entity_ref=sub.get("entity_ref"))
            audit.log(result.run_id, "map", path, detail=f"{len(claims)} claims")

            gated = gate_for_review(claims, [])
            clean, review = gated["clean"], gated["needs_review"]

            from .storage import derive_report_date
            rdate = derive_report_date(claims, None)

            storage.write_silver(claims, team=sub["team"],
                                 report_type=sub["report_type"],
                                 report_date=rdate)
            result.gold_total += storage.write_gold_metrics(
                clean, team=sub["team"], report_type=sub["report_type"],
                report_date=rdate)
            if review:
                storage.write_review_claims(
                    review, team=sub["team"], report_type=sub["report_type"],
                    report_date=rdate)
            audit.log(result.run_id, "store", path,
                      detail=f"gold={len(clean)} review={len(review)}")
            try:
                parsed = _run_ai_parse(spark, path)
                result.indexed_chunks += _index_ai_parse_elements(
                    parsed, sub, search_store, sub.get("entity_ref"), claims=claims)
            except Exception:
                pass

            result.documents += 1
            result.claims_total += len(claims)
            result.review_total += len(review)

        except Exception as e:
            try:
                storage.write_quarantine([{
                    "kind": "document", "source_element_id": fname,
                    "error": str(e)[:200], "source_document": path,
                    "team": sub["team"], "report_type": sub["report_type"],
                    "entity_ref": sub.get("entity_ref")}])
            except Exception:
                pass
            audit.log(result.run_id, "error", path, error=str(e)[:200])
            result.quarantined += 1
            result.failures.append((fname, str(e)[:150]))

    try:
        storage.write_audit(audit.to_rows())
    except Exception:
        pass
    return result

# RUN JOB INGESTION
"""Batch ingestion entry point (custom track).

Discovers submissions under the volume and hands them to the orchestrator.
Invoked by a Databricks Job / Workflow.

Layout: <root>/<team>/<report_type>/<file>. The orchestration scans EVERY
team folder, then EVERY report_type folder under it, and processes every
document found. entity_ref is NOT in the path — it is resolved from the
filename convention (entityID_entityName_documentTitle_reportDate.ext) and stays
a column in the data. A filename that doesn't parse still processes; its
entity_ref is left None and handled by the existing quarantine/UNKNOWN path
rather than crashing the run.

Idempotency comes from content-hash dedup in storage, so re-scanning already
processed files is a no-op.
"""
from __future__ import annotations

import asyncio
import argparse
import os
import re

from ..search.ai_search_store import AzureAISearchStore
from ..shared.config import CONFIG
from .production import process_batch

# entityID_entityName_documentTitle_reportDate.ext  (reportDate = YYYY-MM-DD)
_FILENAME_RE = re.compile(
    r"^(?P<entity_ref>[A-Za-z0-9]+)_[A-Za-z0-9\-]+_[A-Za-z0-9\-]+_"
    r"\d{4}-\d{2}-\d{2}\.[A-Za-z0-9]+$")


def entity_ref_from_filename(filename: str) -> str | None:
    """Extract entity_ref from the filename convention, or None if it doesn't
    match (the document still processes; attribution routes to review)."""
    m = _FILENAME_RE.match(filename.strip())
    return m.group("entity_ref") if m else None


def _normalise_entity(folder: str) -> str:
    """Normalise an entity folder name so casing/spacing differences don't
    fragment one entity into several (ACME_CORP vs 'Acme Corp'). Trim, collapse
    whitespace, and keep the given form otherwise — teams file under a canonical
    entity folder; this only guards against trivial variance."""
    return re.sub(r"\s+", " ", folder.strip())


def discover_submissions(root: str) -> list[dict]:
    """Scan <root>/<team>/<report_type>/<entity>/<entity_id>/<entity_special_name>/<file>.

    Attribution comes from the PATH, not the filename: a file is only collected
    when it sits inside a complete team/report_type/entity/entity_id/
    entity_special_name/ path, so every discovered file is fully attributed by
    construction. The walk descends the full entity hierarchy to find work, so a
    file dropped at a shallower level (no full entity path) is simply not
    collected.

    team, report_type, entity, entity_id and entity_name (the special name) are
    all taken from the PATH folder names. The filename is free-form.
    """
    subs: list[dict] = []
    if not os.path.isdir(root):
        return subs

    def _subdirs(d):
        try:
            return sorted(x for x in os.listdir(d)
                          if os.path.isdir(os.path.join(d, x)) and not x.startswith("."))
        except OSError:
            return []

    for team in _subdirs(root):
        team_dir = os.path.join(root, team)
        for report_type in _subdirs(team_dir):
            rt_dir = os.path.join(team_dir, report_type)
            for entity in _subdirs(rt_dir):
                entity_dir = os.path.join(rt_dir, entity)
                for entity_id in _subdirs(entity_dir):
                    eid_dir = os.path.join(entity_dir, entity_id)
                    for entity_name in _subdirs(eid_dir):
                        leaf = os.path.join(eid_dir, entity_name)
                        for fname in sorted(os.listdir(leaf)):
                            fpath = os.path.join(leaf, fname)
                            if fname.startswith(".") or not os.path.isfile(fpath):
                                continue
                            subs.append({
                                "path": fpath,
                                "entity_ref": _normalise_entity(entity),
                                "entity": entity,
                                "entity_id": entity_id,
                                "entity_name": entity_name,
                                "team": team,
                                "report_type": report_type,
                            })
    return subs


def main() -> None:
    parser = argparse.ArgumentParser(description="the pipeline regdata ingestion")
    parser.add_argument("--root", default=CONFIG.storage.volume_root)
    parser.add_argument("--no-index", action="store_true",
                        help="Skip Databricks AI Search conflict indexing.")
    parser.add_argument("--crop-zoom", action="store_true",
                        help="Enable crop-and-zoom multimodal reading of "
                             "chart-like figures (PDF sources).")
    args = parser.parse_args()

    subs = discover_submissions(args.root)
    print(f"Discovered {len(subs)} submissions under {args.root}")

    search_store = None if args.no_index else AzureAISearchStore()
    if search_store is not None:
        search_store.ensure_index()

    # Crop-and-zoom is opt-in. The pdfplumber-backed provider renders figure
    # pages for legible re-reading; left off, extraction is text-only.
    page_image_provider = None
    if args.crop_zoom:
        from ..search.figure_preprocessor import PdfPlumberPageImageProvider
        page_image_provider = PdfPlumberPageImageProvider()

    result = asyncio.run(process_batch(subs, search_store=search_store,
                           page_image_provider=page_image_provider))
    print(
        f"run_id={result.run_id} documents={result.documents} "
        f"gold_rows={result.gold_metrics_total} "
        f"cost=${result.total_cost_usd:.4f} review={result.needs_review} "
        f"conflicts={result.conflicts} quarantined={result.quarantined} "
        f"cropped_figures={result.cropped_figures}"
    )


if __name__ == "__main__":
    main()
    
# SCORECARD
"""Per-run pipeline SCORECARD — coverage, quality, robustness and cost.

Produces an objective, comparable snapshot after each pipeline run, so two
approaches (or two runs) can be evaluated on measured outcomes rather than on
design preference. Every number here is computed from the run's own output —
the Gold/silver/quarantine/audit tables and the BatchResult — so it is
reproducible and can be re-run for any pipeline over the same documents.

Honest scope of each dimension:
  * COVERAGE   — how much was extracted (counts). Objective.
  * QUALITY    — PROXIES for correctness: grounding rate, confidence, citation
                 tier mix. These are strong signals, not a substitute for a
                 manual ground-truth check, which should accompany them.
  * ROBUSTNESS — how gracefully it handled the inputs: quarantine rate, review
                 rate, coverage across document formats.
  * COST       — LLM spend + ai_parse DBU estimate, per document and per metric.
"""

from __future__ import annotations

from typing import Optional


def build_scorecard(result, storage=None, prefix: str = "",
                    ai_parse_cost_per_page: float = 0.005,
                    pages_processed: Optional[int] = None) -> dict:
    """Compute the four-dimension scorecard from a BatchResult (and, if given,
    the storage layer to read gold/review/quarantine counts).

    Returns a flat dict of metrics; pass it to ``scorecard_table`` to render.
    """
    docs = max(1, getattr(result, "documents", 0) or 0)
    claims = getattr(result, "claims_total", 0) or 0
    gold = getattr(result, "gold_total", 0) or 0
    review = getattr(result, "review_total", 0) or 0
    quar = getattr(result, "quarantined", 0) or 0
    indexed = getattr(result, "indexed_chunks", 0) or 0
    llm_cost = getattr(result, "total_cost_usd", 0.0) or 0.0

    total_docs = docs + quar          # attempted = processed + failed
    # --- COVERAGE ---
    coverage = {
        "documents_processed": docs,
        "total_metrics": claims,
        "gold_metrics": gold,
        "metrics_per_document": round(claims / docs, 1),
        "indexed_chunks": indexed,
    }
    # --- QUALITY (proxies) ---
    quality = {
        "gold_rate_pct": round(100 * gold / claims, 1) if claims else 0.0,
        "review_rate_pct": round(100 * review / claims, 1) if claims else 0.0,
        # filled from the tables when storage is provided (see below)
        "grounded_pct": None,
        "avg_confidence": None,
        "parsed_tier_pct": None,
    }
    # --- ROBUSTNESS ---
    robustness = {
        "quarantine_rate_pct": round(100 * quar / total_docs, 1) if total_docs else 0.0,
        "success_rate_pct": round(100 * docs / total_docs, 1) if total_docs else 0.0,
        "failures": len(getattr(result, "failures", []) or []),
    }
    # --- COST ---
    pages = pages_processed or docs        # fall back to docs if pages unknown
    ai_parse_cost = pages * ai_parse_cost_per_page
    total_cost = llm_cost + ai_parse_cost
    cost = {
        "llm_cost_usd": round(llm_cost, 4),
        "ai_parse_cost_usd": round(ai_parse_cost, 4),
        "total_cost_usd": round(total_cost, 4),
        "cost_per_document_usd": round(total_cost / docs, 4),
        "cost_per_metric_usd": round(total_cost / claims, 6) if claims else 0.0,
    }

    # enrich QUALITY from the gold tables if storage is available
    if storage is not None:
        try:
            _fill_quality_from_gold(quality, storage, prefix)
        except Exception:
            pass

    return {"coverage": coverage, "quality": quality,
            "robustness": robustness, "cost": cost}


def _fill_quality_from_gold(quality: dict, storage, prefix: str) -> None:
    """Read grounding / confidence / tier stats from the gold rows (best-effort)."""
    rows = []
    try:
        for layer in ("gold",):
            data = storage.read_jsonl(layer)
            if data:
                rows.extend(data)
    except Exception:
        rows = []
    if not rows:
        return
    confs = [r.get("confidence") for r in rows if isinstance(r.get("confidence"), (int, float))]
    grounded = sum(1 for r in rows if str(r.get("grounding", "")).lower() == "grounded")
    parsed = sum(1 for r in rows if str(r.get("citation_tier", "")).lower() == "parsed")
    n = len(rows)
    quality["grounded_pct"] = round(100 * grounded / n, 1) if n else None
    quality["avg_confidence"] = round(sum(confs) / len(confs), 3) if confs else None
    quality["parsed_tier_pct"] = round(100 * parsed / n, 1) if n else None


def scorecard_table(scorecard: dict, label: str = "this run") -> str:
    """Render the scorecard as a readable text table (for the notebook / an
    email). Two scorecards can be rendered side by side to compare approaches."""
    lines = [f"===== PIPELINE SCORECARD — {label} =====", ""]
    for section in ("coverage", "quality", "robustness", "cost"):
        lines.append(section.upper())
        for k, v in scorecard[section].items():
            lines.append(f"  {k:28} {v}")
        lines.append("")
    return "\n".join(lines)


def compare_scorecards(a: dict, b: dict, label_a="A", label_b="B") -> str:
    """Render two scorecards side by side for an evidence-based comparison."""
    lines = [f"{'metric':32} {label_a:>16} {label_b:>16}", "-" * 66]
    for section in ("coverage", "quality", "robustness", "cost"):
        lines.append(section.upper())
        for k in a[section]:
            va, vb = a[section].get(k), b[section].get(k)
            lines.append(f"  {k:30} {str(va):>16} {str(vb):>16}")
    return "\n".join(lines)