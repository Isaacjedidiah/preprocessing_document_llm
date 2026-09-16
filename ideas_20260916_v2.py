# ** AI PARSE PIPELINE
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

    ai_parse renders WHOLE PAGES to images. We send the figure's FULL PAGE image
    (full resolution) to Sonnet — this is what tested correctly for reading chart
    values (a downsized or mis-cropped image makes the model read the axis scale,
    not the data). Sonnet is instructed (via the chart prompt) and enforced (via
    source_type) to read CHARTS ONLY, ignoring tables/text — so there is no
    duplication of the rule-based table/text claims.

    save_crops_to: optional folder — saves each page image sent to Sonnet, for
    inspection.
    """
    from PIL import Image

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
        # send the figure's FULL PAGE at full resolution.
        import base64 as _b64, io as _io
        bbox = getattr(el, "bbox", None)
        page_id = (bbox[0].get("page_id", 1)
                   if bbox and isinstance(bbox, list) and isinstance(bbox[0], dict)
                   else 1)
        page_img = _page_image(page_id)
        if page_img is None:
            return None
        try:
            rgb = page_img.convert("RGB")
            if save_crops_to:
                try:
                    _counter["n"] += 1
                    outp = f"{save_crops_to}/page_{_counter['n']}.png".replace("dbfs:", "")
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

# ** HYBRID EXTRACTION
"""Hybrid figure/table extraction: ai_parse_document first, Sonnet for hard charts.

The design (validated on real documents):
  * ai_parse_document is excellent at tables-in-images and MOST charts, and
    returns, per element, a ``type`` (text/table/figure/...), ``content``
    (NULL for figures it couldn't read), ``confidence`` (0-1), and ``bbox``.
  * So we route by the element ai_parse gives back:
      - table / text / title / ... -> use ai_parse's content directly.
      - figure with good confidence AND non-null content -> use it.
      - figure with LOW confidence OR null content -> escalate to Sonnet, which
        acts as a focused "chart digitisation engine" (structured output, no
        narrative), because those are the hard charts ai_parse couldn't do.

This keeps cost/rate-limit pressure low: only the residual hard charts reach
Sonnet, not every figure. Text and native tables stay on the existing direct
(pdfplumber) path upstream — ai_parse is used only for image content, per the
Databricks-recommended hybrid pattern.

This module is pure-logic where possible (parsing the ai_parse response,
deciding escalation) so it is unit-testable without a live endpoint; the actual
ai_parse and Sonnet calls are injected.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


# Element types ai_parse returns that are NON-figure — used as-is from ai_parse.
_NON_FIGURE_TYPES = {"text", "table", "title", "caption", "section_header",
                     "page_header", "page_footer", "page_number", "footnote"}


# The focused prompt for reading a complex TABLE image that ai_parse couldn't
# reliably extract. Returns row-wise {metric, value} pairs, preserving the
# row/column meaning. Structured output only, no narrative.
TABLE_DIGITISATION_PROMPT = (
    "You are a table digitisation engine. Read this table image and return ONLY "
    "a JSON array of its data cells — no prose:\n"
    '[{"label": "<block | all row levels/entity | grouping band | sub-header>", '
    '"value": <number>, "unit": "<%/£m/etc if shown>"}]\n'
    "Rules:\n"
    "- ROWS may be NESTED across several left columns (e.g. portfolio->ty->"
    "position: 'Seg'->'ju'->'Able'); merged cells apply to every row they span. "
    "A first-column entity/organisation name (e.g. 'HTST YU') is also part of "
    "the row. Combine ALL row levels.\n"
    "- COLUMNS are often TWO levels: a grouping band (e.g. 'State', 'Aug TE') "
    "AND a per-column sub-header (e.g. 'Jun-28', 'Lit Sek'). Combine both.\n"
    "- SEPARATE table BLOCKS side by side (e.g. 'State' block, 'Pot' block) are "
    "distinct — prefix each value with its block; never mix blocks.\n"
    "- SECTION banner rows (no data) are context, not metrics.\n"
    "- Free-text columns (e.g. 'Comments') are context, not values.\n"
    "- Build each label combining EVERY coordinate, e.g. "
    "'Pot | Seg | ju | Able | Jun-28' = 809.\n"
    "- NEVER return an unnamed value or a bare number. A '-'/blank/empty cell "
    "means no value — skip it. If unreadable, OMIT it. Never invent values.\n"
    "- Output ONLY the JSON array."
)


# The focused prompt for the Sonnet escalation — a chart digitisation engine.
# Structured output only, no narrative (which also avoids the JSON-in-prose
# parsing issues), plus the grounding guardrail (omit what you can't read).
CHART_DIGITISATION_PROMPT = (
    "You are a chart digitisation engine. Read ONLY the CHARTS and GRAPHS in "
    "this image and return their data points as a JSON array — no prose:\n"
    '[{"label": "<series name from legend | category/x-axis label>", '
    '"value": <number>, "unit": "<%/£m/bps/count/etc>", '
    '"source_type": "chart"}]\n'
    "\n"
    "Every item MUST include \"source_type\". Set it to \"chart\" for a value read "
    "from a chart/graph/plot. If you (incorrectly) read something from a table "
    "set \"table\"; from text set \"text\" — but you should not be returning those "
    "at all (see the ABSOLUTE RULE below).\n"
    "*** ABSOLUTE RULE — CHARTS ONLY ***\n"
    "- Extract data ONLY from charts, graphs and plots: bar charts, line charts, "
    "pie/donut charts, radial charts, scatter/area plots — anything with bars, "
    "lines, wedges or plotted points read against an axis or legend.\n"
    "- NEVER extract from TABLES. If numbers sit in a grid of rows and columns, "
    "that is a TABLE — IGNORE it completely. Do not return any value from a table.\n"
    "- NEVER extract from TEXT, paragraphs, headings, bullet points or figures "
    "of prose. IGNORE all plain text and tabular numbers.\n"
    "- Tables and text are extracted separately by another system. Your ONLY job "
    "is the charts. If a value is not plotted in a chart, DO NOT return it.\n"
    "- If the image contains NO chart at all (only tables/text/logos), return an "
    "empty array [].\n"
    "\n"
    "Chart-reading rules:\n"
    "- The chart may COMBINE types (bars AND a line) — read BOTH series. Combo "
    "charts may have TWO y-axes; use the correct axis for each series.\n"
    "- RADIAL/circular bar charts: read each segment against its radial scale.\n"
    "- COLOUR LEGEND: bars/lines are distinguished by colour with a legend/"
    "footnote mapping colour -> series name (e.g. blue = 'DEF exposure'). Map "
    "each coloured bar/line to its series name and put it in the label. Never "
    "report a value without its series name.\n"
    "- Read every value across ALL series against the axis scale; estimate "
    "between gridlines if needed.\n"
    "- CRITICAL — read the DATA, not the axis. Do NOT return the numbers printed "
    "on the y-axis scale / gridlines (e.g. 0, 2000, 4000, 6000). Those are the "
    "SCALE, not data. For each BAR, estimate the value at the TOP of the bar by "
    "its height against the scale; for each LINE point, estimate its value by "
    "its vertical position. If a bar top sits between 4000 and 6000 nearer 6000, "
    "return about 5500 — its actual height — NOT 4000 or 6000.\n"
    "- If a data label is printed on or above a bar/point, use that exact number "
    "instead of estimating.\n"
    "- If you cannot read a value, OMIT it. Never invent values.\n"
    "- Output ONLY the JSON array."
)


@dataclass
class HybridConfig:
    """Tuning for the hybrid gate."""
    figure_confidence_threshold: float = 0.75  # below -> escalate to Sonnet
    escalate_null_content: bool = True         # figure with null content -> Sonnet
    table_confidence_threshold: float = 0.6    # below -> table escalates to Sonnet image
    escalate_low_conf_tables: bool = True      # gate tables too (image-tables can be unreliable)


@dataclass
class ParsedElement:
    """One normalised element out of ai_parse_document."""
    el_type: str
    content: Optional[str]
    confidence: Optional[float]
    page: Optional[int]
    bbox: Optional[list]
    description: Optional[str] = None


@dataclass
class HybridResult:
    """Outcome of hybrid extraction over one document."""
    used_ai_parse: list = field(default_factory=list)   # elements taken from ai_parse
    escalated: list = field(default_factory=list)       # elements sent to Sonnet
    sonnet_points: list = field(default_factory=list)    # data points Sonnet returned


def parse_ai_parse_response(resp: Any) -> list[ParsedElement]:
    """Normalise an ai_parse_document response into a flat list of ParsedElement.

    Accepts what ai_parse_document actually returns across surfaces:
      * a Spark VARIANT (VariantVal) — converted via its JSON form;
      * a JSON string;
      * a dict / Row.
    Reads document.elements, pulling type/content/confidence/bbox/description.
    Returns [] on anything unreadable.
    """
    if resp is None:
        return []
    data = resp
    # Spark VARIANT (VariantVal) -> get its JSON, then load. VariantVal exposes
    # toJson()/to_json() depending on version; fall back to str().
    if type(resp).__name__ == "VariantVal":
        try:
            js = None
            for meth in ("toJson", "to_json"):
                if hasattr(resp, meth):
                    js = getattr(resp, meth)()
                    break
            if js is None:
                js = str(resp)
            data = json.loads(js)
        except (ValueError, TypeError):
            return []
    elif isinstance(resp, str):
        try:
            data = json.loads(resp)
        except (ValueError, TypeError):
            return []
    elif hasattr(resp, "asDict"):
        try:
            data = resp.asDict(recursive=True)
        except Exception:
            return []
    if not isinstance(data, dict):
        return []
    doc = data.get("document") or {}
    elements = doc.get("elements") or []
    out: list[ParsedElement] = []
    for e in elements:
        if not isinstance(e, dict):
            continue
        page = None
        bbox = e.get("bbox")
        try:
            if bbox and isinstance(bbox, list) and isinstance(bbox[0], dict):
                page = bbox[0].get("page_id")
        except Exception:
            page = None
        out.append(ParsedElement(
            el_type=str(e.get("type", "")).lower(),
            content=e.get("content"),
            confidence=e.get("confidence"),
            page=page,
            bbox=bbox,
            description=e.get("description"),
        ))
    return out


def needs_sonnet_escalation(el: ParsedElement, cfg: HybridConfig) -> bool:
    """Decide whether a figure element should be escalated to Sonnet.

    Only figures are candidates. A figure escalates when ai_parse is not
    confident enough OR returned no content (both mean "ai_parse couldn't
    digitise this chart"). Non-figures never escalate — ai_parse handles them.
    """
    if el.el_type != "figure":
        return False
    if cfg.escalate_null_content and not (el.content or "").strip():
        return True
    if el.confidence is not None and el.confidence < cfg.figure_confidence_threshold:
        return True
    return False


def parse_sonnet_chart_points(raw: str) -> list[dict]:
    """Parse Sonnet's chart-digitisation output (a JSON array of points).
    Tolerant: strips prose around the array, returns [] if unparseable."""
    if not raw:
        return []
    import re
    s = raw.strip()
    # whole thing is JSON?
    try:
        val = json.loads(s)
        if isinstance(val, list):
            return [p for p in val if isinstance(p, dict)]
    except (ValueError, TypeError):
        pass
    # extract the first [...] array from prose
    m = re.search(r"\[.*\]", s, re.DOTALL)
    if m:
        try:
            val = json.loads(m.group(0))
            if isinstance(val, list):
                return [p for p in val if isinstance(p, dict)]
        except (ValueError, TypeError):
            pass
    return []


def run_hybrid_extraction(ai_parse_response: Any,
                          sonnet_read_figure: Callable[[ParsedElement], str],
                          cfg: Optional[HybridConfig] = None) -> HybridResult:
    """Orchestrate the hybrid over one document's ai_parse response.

    ``sonnet_read_figure`` is injected: given a figure ParsedElement (with its
    bbox/page so the caller can crop the image), it returns Sonnet's raw
    digitisation output. Kept as a callback so this function stays pure/testable
    and the actual Sonnet/image plumbing lives in the caller.
    """
    cfg = cfg or HybridConfig()
    result = HybridResult()
    for el in parse_ai_parse_response(ai_parse_response):
        if el.el_type in _NON_FIGURE_TYPES:
            result.used_ai_parse.append(el)          # ai_parse handles it
        elif el.el_type == "figure":
            if needs_sonnet_escalation(el, cfg):
                result.escalated.append(el)
                raw = sonnet_read_figure(el)          # hard chart -> Sonnet
                result.sonnet_points.extend(parse_sonnet_chart_points(raw))
            else:
                result.used_ai_parse.append(el)       # ai_parse got the chart
        else:
            result.used_ai_parse.append(el)           # unknown type -> keep
    return result


# ---- figure classification: logo / scale-only / data-blob / usable ----

# ratio of "round" numbers (multiples of a power of ten) above which a figure's
# content is treated as SCALE-ONLY (axis gridlines, not real data).
_SCALE_ROUND_RATIO = 0.8


def _numbers_in_text(text: str) -> list[float]:
    import re
    out = []
    for m in re.finditer(r"-?\d[\d,]*(?:\.\d+)?", text or ""):
        try:
            out.append(float(m.group(0).replace(",", "")))
        except (ValueError, TypeError):
            pass
    return out


def _is_round(n: float) -> bool:
    """True for 'round' gridline-style numbers: 0, 100, 1000, 2500, 5000 —
    i.e. a multiple of a sizable power of ten. Real data values are usually not
    all round."""
    n = abs(n)
    if n == 0:
        return True
    for base in (1000, 500, 100):
        if n % base == 0:
            return True
    return False


def classify_figure(el: ParsedElement) -> str:
    """Classify a figure element into how it should be handled:
      'logo'       — the description says logo/emblem, or there is no data at all
                     -> skip.
      'data'       — the description (or content) contains real numeric values
                     -> usable (structure it from the description).
      'no_content' — no description and no content -> nothing to read without an
                     image (needs vision if use_sonnet).

    IMPORTANT: for a figure, ai_parse puts the chart's data in ``description``
    (a multimodal description like "Q1 at $12M, Q2 at $15M..."); ``content`` is
    usually NULL. So we classify primarily on the DESCRIPTION, falling back to
    content.
    """
    desc = (el.description or "").strip()
    content = (el.content or "").strip()
    # For a figure, ai_parse usually puts the chart's data in DESCRIPTION and
    # leaves content NULL — but content can carry OCR'd text too. Use BOTH so
    # neither source is missed; description leads because it holds the
    # interpreted values, content is appended for any extra text.
    text = " ".join(x for x in (desc, content) if x).strip()
    low = text.lower()

    # explicit logo/emblem -> skip
    if any(w in low for w in ("logo", "emblem", "icon", "crest", "letterhead")):
        return "logo"

    if not text:
        return "no_content"                       # nothing to read without vision

    nums = _numbers_in_text(text)
    if not nums:
        return "no_content"                       # a described figure with no values

    return "data"                                 # description/content has values


def table_route(el: ParsedElement, cfg: Optional[HybridConfig] = None) -> str:
    """Decide the route for a TABLE (or text) element:
      'use_ai_parse' — ai_parse's content is reliable -> use directly.
      'sonnet_image' — low confidence (typically a complex IMAGE-table ai_parse
                       struggled with) -> send the table image to Sonnet to read.
    Only tables are image-escalated; plain text always uses ai_parse.
    """
    cfg = cfg or HybridConfig()
    if el.el_type != "table":
        return "use_ai_parse"
    if not cfg.escalate_low_conf_tables:
        return "use_ai_parse"
    if not (el.content or "").strip():
        return "sonnet_image"   # empty table content -> must read the image
    if el.confidence is not None and el.confidence < cfg.table_confidence_threshold:
        return "sonnet_image"   # low-confidence (complex image-table) -> Sonnet
    return "use_ai_parse"


def figure_route(el: ParsedElement, cfg: Optional[HybridConfig] = None) -> str:
    """Decide the ROUTE for a figure:
      'skip'          — logo / no data worth extracting.
      'sonnet_image'  — needs Sonnet to READ THE IMAGE (no_content or scale_only,
                        or low ai_parse confidence).
      'sonnet_text'   — ai_parse got real data as a blob; Sonnet STRUCTURES the
                        text (cheap, no image) into labelled pairs.
      'use_ai_parse'  — ai_parse's content is already usable as-is.
    """
    cfg = cfg or HybridConfig()
    kind = classify_figure(el)
    if kind == "logo":
        return "skip"
    if kind in ("no_content", "scale_only"):
        return "sonnet_image"
    # kind == "data": trust it if confident, else structure the text
    if el.confidence is not None and el.confidence < cfg.figure_confidence_threshold:
        return "sonnet_image"
    return "sonnet_text"


# ---- mapping ai_parse elements + Sonnet points into Claim objects ----

def ai_parse_table_to_claims(el: ParsedElement, filename: str,
                             team=None, report_type=None, entity_ref=None,
                             claim_cls=None) -> list:
    """A table/text element from ai_parse becomes claim(s). Tables keep their
    markdown content as a single TABLE-derived claim carrying the content;
    downstream metric parsing runs over it as with any table."""
    if claim_cls is None:
        from ..shared.schema import Claim as claim_cls
    content = (el.content or "").strip()
    if not content:
        return []
    eid = f"{filename}-p{el.page}-aiparse{el.el_type}"
    return [claim_cls(
        field_name=el.el_type, canonical_metric=None, value=content,
        source_element_id=eid, entity_ref=entity_ref, page=el.page,
        confidence=el.confidence or 1.0, model_used="ai_parse_document")]


def sonnet_points_to_claims(points: list[dict], el: ParsedElement, filename: str,
                            entity_ref=None, claim_cls=None) -> list:
    """Sonnet's chart claims (from EXTRACTION_TOOL_SCHEMA -> tool_arguments.claims,
    each carrying field_name / value / unit / scale / confidence / as_at_date)
    become one Claim each, tagged LLM_ESTIMATED (read by a vision model from the
    rendered figure image — estimates, not deterministic parses). Tolerant of the
    older {label, value} shape too."""
    if claim_cls is None:
        from ..shared.schema import Claim as claim_cls
    try:
        from ..shared.schema import CitationTier
        tier = CitationTier.LLM_ESTIMATED
    except Exception:
        tier = None
    eid = f"{filename}-p{el.page}-fig"
    out = []
    for p in points:
        if not isinstance(p, dict):
            continue
        # ENFORCE charts-only: Sonnet self-reports source_type per value; keep
        # only 'chart' values. A 'table'/'text' value means Sonnet strayed onto
        # content the rule-based path already handles — drop it (no duplication).
        stype = str(p.get("source_type") or "chart").strip().lower()
        if stype not in ("chart", "graph", "plot", ""):
            continue
        # the schema uses 'field_name'; older callers used 'label'
        name = str(p.get("field_name") or p.get("label") or "").strip()
        val = p.get("value")
        if val is None or not name:
            continue
        conf = p.get("confidence")
        try:
            conf = float(conf) if conf is not None else 0.7
        except (TypeError, ValueError):
            conf = 0.7
        out.append(claim_cls(
            field_name=name,
            canonical_metric=name,
            value=str(val),
            unit=str(p.get("unit")) if p.get("unit") else None,
            scale=str(p.get("scale")) if p.get("scale") else None,
            as_at_date=str(p.get("as_at_date")) if p.get("as_at_date") else None,
            element_type="figure",        # the type column: chart values are figures
            source_element_id=eid, entity_ref=entity_ref, page=el.page,
            confidence=conf, citation_tier=tier,
            model_used="sonnet_chart_digitiser"))
    return out


def _sonnet_read_image(run_async_fn, llm_client, model_key, prompt, content, img_b64):
    """Call the vision extract() correctly — it needs the EXTRACTION_TOOL_SCHEMA
    (forced tool-use) and returns structured data in ``tool_arguments``. Returns
    a list of {label, value, unit} points parsed from either the tool arguments
    or the text, tolerant of both shapes."""
    try:
        from ..shared.llm_client import EXTRACTION_TOOL_SCHEMA
    except Exception:
        EXTRACTION_TOOL_SCHEMA = {"name": "extract"}
    resp = run_async_fn(llm_client.extract(
        model_key, prompt, content or "", EXTRACTION_TOOL_SCHEMA,
        image_base64=img_b64))
    # prefer structured tool_arguments; fall back to text JSON.
    ta = getattr(resp, "tool_arguments", None)
    if isinstance(ta, dict):
        # the schema records results under 'claims'; be tolerant of a few names
        for key in ("claims", "metrics", "points", "data", "values"):
            if isinstance(ta.get(key), list):
                return [p for p in ta[key] if isinstance(p, dict)]
        # or a flat list of fields -> wrap as one
        if ta:
            return [ta]
    return parse_sonnet_chart_points(getattr(resp, "text", "") or "")


def hybrid_extract_document(ai_parse_response: Any, filename: str,
                            run_async_fn: Callable = None,
                            llm_client: Any = None,
                            crop_figure_image: Optional[Callable] = None,
                            team=None, report_type=None, entity_ref=None,
                            cfg: Optional[HybridConfig] = None,
                            vision_model_key: str = "tier2",
                            use_sonnet: bool = False) -> list:
    """END-TO-END extraction for one document -> Claim objects for storage.

    ``use_sonnet`` controls the cost/quality trade-off:
      * False (default) — EVERYTHING is structured by RULE (no LLM cost):
        HTML tables -> composite-named claims, text-embedded numbers -> claims,
        figures -> ai_parse's own content/description parsed for values. This is
        the cheap path (ai_parse DBUs only). Use this first and inspect quality.
      * True — the hybrid: rule-based tables/text, but figures (and low-quality
        tables) escalate to Sonnet for vision reading. Flip this on only if the
        rule-based figure extraction proves inadequate.

    When use_sonnet=True, run_async_fn + llm_client must be provided.
    """
    cfg = cfg or HybridConfig()
    claims: list = []
    elements = parse_ai_parse_response(ai_parse_response)

    from .ai_parse_structurer import (html_table_to_claims, numbers_from_text,
                                      chart_description_to_claims)

    # link a nearby title (a preceding title/heading element) to each element,
    # in document order, so composite names carry it.
    current_title = None
    current_section = None
    current_page = None

    # layout-marker element types carry no metric data -> always skipped.
    _SKIP_TYPES = {"page_header", "page_footer", "page_number"}

    for el in elements:
        etype = el.el_type
        content = el.content or ""
        clean_content = re.sub(r"<[^>]+>", "", content).strip()

        # PAGE BOUNDARY: a heading only governs its own page. When the page
        # changes, clear the carried title/section so a heading on page 1 does
        # not wrongly prefix metrics on page 2.
        if el.page is not None and el.page != current_page:
            current_page = el.page
            current_title = None
            current_section = None

        # layout noise: page headers/footers/numbers -> skip entirely.
        if etype in _SKIP_TYPES:
            continue

        # title -> document-level prefix; section_header -> section prefix.
        if etype == "title":
            if clean_content:
                current_title = clean_content
            continue
        if etype == "section_header":
            if clean_content:
                current_section = clean_content
            continue
        # caption -> context for the nearby figure/table; also scan it for
        # numbers (captions sometimes state a figure's key value).
        if etype == "caption":
            claims.extend(numbers_from_text(
                clean_content, filename, page=el.page,
                title=_prefix(current_title, current_section),
                team=team, report_type=report_type, entity_ref=entity_ref))
            continue
        # footnote -> often carries meaningful caveats AND numbers (e.g.
        # "*restated to £4.2m"); extract any numeric values from it.
        if etype == "footnote":
            claims.extend(numbers_from_text(
                clean_content, filename, page=el.page,
                title=_prefix(current_title, current_section, suffix="footnote"),
                team=team, report_type=report_type, entity_ref=entity_ref))
            continue

        if etype == "table":
            # TABLES ALWAYS use the rule-based path — ai_parse structures tables
            # very well as HTML, so they never go to Sonnet (by design). Parse
            # the HTML; if that yields nothing, fall back to ai_parse's own
            # content — but never vision.
            rule_claims = html_table_to_claims(
                content, filename, page=el.page,
                title=_prefix(current_title, current_section),
                team=team, report_type=report_type, entity_ref=entity_ref)
            if rule_claims:
                claims.extend(rule_claims)
                continue
            claims.extend(ai_parse_table_to_claims(
                el, filename, team, report_type, entity_ref))
            continue

        if etype == "text":
            claims.extend(numbers_from_text(
                clean_content, filename, page=el.page,
                title=_prefix(current_title, current_section),
                team=team, report_type=report_type, entity_ref=entity_ref))
            continue

        if etype == "figure":
            kind = classify_figure(el)
            if kind == "logo":
                continue
            # figure content is OFTEN NULL, but ai_parse's DESCRIPTION is
            # multimodal and typically contains the chart's actual values
            # (e.g. "Q1 at $12M, Q2 at $15M..."). Combine description + any
            # content (some figures carry OCR text) and parse for values —
            # rule-based, free, no vision needed when the values are present.
            fig_text = " ".join(x for x in (el.description, content) if x).strip()
            desc_claims = chart_description_to_claims(
                fig_text, filename, page=el.page,
                title=_prefix(current_title, current_section),
                team=team, report_type=report_type, entity_ref=entity_ref)
            if desc_claims and not use_sonnet:
                claims.extend(desc_claims)
                continue
            if not use_sonnet:
                # description had no usable values AND sonnet off -> figure
                # yields nothing; flip use_sonnet=True to vision-read it.
                claims.extend(desc_claims)
                continue
            # use_sonnet=True: READ THE IMAGE. ai_parse rendered the page (or
            # figure) to an image; send it to Sonnet to read the chart directly.
            # We always use the image path here (not a text .complete call) —
            # the point of use_sonnet is that Sonnet SEES the chart.
            if crop_figure_image is None:
                # no image available -> fall back to whatever the description gave
                claims.extend(desc_claims)
                continue
            try:
                img_b64 = crop_figure_image(el)
                if not img_b64:
                    claims.extend(desc_claims)
                    continue
                pts = _sonnet_read_image(
                    run_async_fn, llm_client, vision_model_key,
                    CHART_DIGITISATION_PROMPT, content, img_b64)
                if pts:
                    claims.extend(sonnet_points_to_claims(pts, el, filename, entity_ref))
                else:
                    claims.extend(desc_claims)   # vision found nothing -> keep desc
            except Exception:
                claims.extend(desc_claims)       # never lose the description fallback
            continue

        # any unrecognised type with content -> try number extraction.
        if clean_content:
            claims.extend(numbers_from_text(
                clean_content, filename, page=el.page,
                title=_prefix(current_title, current_section),
                team=team, report_type=report_type, entity_ref=entity_ref))

    # No value-based de-duplication: the chart prompt instructs Sonnet to read
    # CHARTS ONLY (never tables/text), so its output does not overlap with the
    # rule-based table/text claims. Separation is by SOURCE (Sonnet = charts,
    # rule-based = tables/text), which avoids the value-collision risk of
    # comparing raw values.
    return claims


def _dedup_sonnet_vs_rules(claims: list) -> list:
    """When figures are read from the WHOLE PAGE image, Sonnet may re-read values
    already captured from the page's tables/text by the rule-based path. Drop a
    Sonnet chart claim (model_used='sonnet_chart_digitiser') when a rule-based
    claim on the SAME page already has the same value — keeping the deterministic
    (parsed) one. Rule-based claims are always kept."""
    def _norm_val(v):
        import re as _re
        return _re.sub(r"[,£$€%\s]", "", str(v or "").lower())

    # index rule-based values per page
    rule_vals = {}
    for c in claims:
        if getattr(c, "model_used", "") != "sonnet_chart_digitiser":
            rule_vals.setdefault(getattr(c, "page", None), set()).add(
                _norm_val(getattr(c, "value", "")))

    out = []
    for c in claims:
        if getattr(c, "model_used", "") == "sonnet_chart_digitiser":
            page = getattr(c, "page", None)
            if _norm_val(getattr(c, "value", "")) in rule_vals.get(page, set()):
                continue    # duplicate of a rule-based value on this page -> drop
        out.append(c)
    return out


def _prefix(title, section, suffix=None):
    """Build the context prefix from the current title + section (+ optional
    marker like 'footnote')."""
    parts = [x for x in (title, section) if x]
    if suffix:
        parts.append(suffix)
    return " | ".join(parts) if parts else None

# ** AI PARSE STRUCTURER
"""Rule-based structuring of ai_parse_document output into composite-named
claims — NO LLM cost. ai_parse returns tables as HTML (``<table><tr><th>...``)
and text as blobs; both are structured enough to parse with code, so we build
the ``<section> | <entity/row> | <column> = value`` metric names deterministically
instead of paying for an LLM to structure them.

Two extractors:
  * ``html_table_to_claims`` — parses an HTML table (headers + rows, incl.
    multi-level headers and colspan/rowspan) into one claim per data cell, with
    a composite name combining the row identity and the full column-header path.
  * ``numbers_from_text`` — pulls monetary/numeric values embedded in prose
    (e.g. "X Account purchased £125m" -> "X Account purchased" = £125m) with the
    surrounding phrase as the metric name.

Titles/sections linked upstream are passed in and prefixed onto the names.
Everything returns standard Claim objects for the existing storage path.
"""

from __future__ import annotations

import re
from html.parser import HTMLParser
from typing import Optional


# ---- HTML table parsing ----

class _TableParser(HTMLParser):
    """Minimal HTML table parser: collects rows as lists of (text, is_header,
    colspan, rowspan). Tolerant of the ai_parse table markup."""

    def __init__(self):
        super().__init__()
        self.rows: list[list[dict]] = []
        self._cur_row = None
        self._cur_cell = None

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "tr":
            self._cur_row = []
        elif tag in ("td", "th"):
            self._cur_cell = {"text": "", "header": tag == "th",
                              "colspan": int(a.get("colspan", 1) or 1),
                              "rowspan": int(a.get("rowspan", 1) or 1)}

    def handle_endtag(self, tag):
        if tag == "tr" and self._cur_row is not None:
            self.rows.append(self._cur_row)
            self._cur_row = None
        elif tag in ("td", "th") and self._cur_cell is not None:
            if self._cur_row is not None:
                self._cur_row.append(self._cur_cell)
            self._cur_cell = None

    def handle_data(self, data):
        if self._cur_cell is not None:
            self._cur_cell["text"] += data


_NUM_RE = re.compile(r"-?\d[\d,]*(?:\.\d+)?")

# unit tokens that may accompany a value (£m, %, bps, etc.)
_UNIT_RE = re.compile(r"(£|\$|€|%|bps|bn|m\b|k\b|million|billion)", re.I)


def _extract_unit(value: str) -> Optional[str]:
    """Pull a unit/currency token from a value string (e.g. '£125m' -> '£m',
    '15%' -> '%'). Returns None if no unit is present."""
    if not value:
        return None
    tokens = _UNIT_RE.findall(value)
    if not tokens:
        return None
    # normalise: currency symbol + scale (e.g. £ + m -> £m)
    cur = next((t for t in tokens if t in ("£", "$", "€")), "")
    scale = next((t.lower() for t in tokens
                  if t.lower() in ("m", "bn", "k", "million", "billion")), "")
    pct = next((t for t in tokens if t in ("%", "bps")), "")
    unit = (cur + scale) if (cur or scale) else pct
    return unit or (tokens[0] if tokens else None)


def _ai_parse_tier(from_figure: bool = False):
    """Citation tier for ai_parse-derived claims, honest about the source:
      * TABLES and TEXT -> PARSED. ai_parse returns tables as structured HTML and
        text verbatim; our rule-based parser reads them DETERMINISTICALLY, so
        these are 'deterministic structural output' — the PARSED definition.
      * FIGURES -> LLM_ESTIMATED. A figure's values come from ai_parse's
        multimodal DESCRIPTION (an LLM interpreting the chart image), which is an
        estimate, not a deterministic read.
    """
    try:
        from ..shared.schema import CitationTier
        return CitationTier.LLM_ESTIMATED if from_figure else CitationTier.PARSED
    except Exception:
        return None


def _has_number(s: str) -> bool:
    return bool(_NUM_RE.search(s or ""))


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def html_table_to_markdown(html: str) -> Optional[str]:
    """Render an ai_parse HTML table as a clean MARKDOWN table for the index —
    readable AND structure-preserving (unlike a raw-HTML blob or flat lines).

    Reuses the SAME grid model as ``html_table_to_claims`` so complex tables are
    handled faithfully: colspan/rowspan headers are expanded into a full grid,
    multi-level headers are flattened per column, merged cells repeat. This means
    the index gets a table the model can actually read and reason over — e.g.
    'what was Japan's WWE THS' — at no LLM cost.
    """
    if not html or "<" not in html:
        return None
    try:
        p = _TableParser()
        p.feed(html)
    except Exception:
        return None
    rows = [r for r in p.rows if r]
    if not rows:
        return None

    # build the grid honouring colspan + rowspan (same as the claim parser)
    grid: dict = {}
    occupied: set = set()
    n_cols = 0
    for ri, r in enumerate(rows):
        c = 0
        for cell in r:
            while (ri, c) in occupied:
                c += 1
            cs = max(1, cell.get("colspan", 1))
            rs = max(1, cell.get("rowspan", 1))
            for dr in range(rs):
                for dc in range(cs):
                    grid[(ri + dr, c + dc)] = _clean(cell["text"])
                    occupied.add((ri + dr, c + dc))
            c += cs
            n_cols = max(n_cols, c)
    n_rows = len(rows)
    if n_cols == 0:
        return None

    # detect how many leading rows are HEADER rows (mostly non-numeric),
    # so multi-level headers merge into ONE markdown header row per column.
    def _row_cells(ri):
        return [grid.get((ri, c), "") for c in range(n_cols)]

    def _is_num(s):
        return bool(re.search(r"\d", s or ""))

    n_header = 0
    for ri in range(min(n_rows, 3)):
        cells = _row_cells(ri)
        if any(_is_num(c) for c in cells):
            break
        n_header += 1
    n_header = max(1, n_header)

    # merge the header rows per column (dedup repeated span text)
    header = []
    for c in range(n_cols):
        parts = []
        for ri in range(n_header):
            t = grid.get((ri, c), "")
            if t and t not in parts:
                parts.append(t)
        header.append(" ".join(parts))

    lines = []
    lines.append("| " + " | ".join(header) + " |")
    lines.append("| " + " | ".join("---" for _ in range(n_cols)) + " |")
    for ri in range(n_header, n_rows):
        lines.append("| " + " | ".join(_row_cells(ri)) + " |")
    return "\n".join(lines)


def html_table_to_claims(html: str, filename: str, page: Optional[int] = None,
                         title: Optional[str] = None, team=None,
                         report_type=None, entity_ref=None, claim_cls=None) -> list:
    """Parse an ai_parse HTML table into one claim per data cell, handling:
      * MULTI-LEVEL column headers (nested <th> rows joined by column) ;
      * SECTION rows (a full-width row spanning all columns with no numbers,
        e.g. 'CASH - OUTFLOWS') used as a prefix for the rows beneath ;
      * NESTED row labels (several leading non-numeric cells, e.g.
        portfolio->ty->position) combined into the row identity.
    Each numeric data cell -> '<title> | <section> | <row path> | <column> =
    value'. Deterministic, no LLM.
    """
    if claim_cls is None:
        from ..shared.schema import Claim as claim_cls
    if not html or "<" not in html:
        return []
    try:
        p = _TableParser()
        p.feed(html)
    except Exception:
        return []
    rows = [r for r in p.rows if r]
    if not rows:
        return []

    # Build a proper GRID that honours BOTH colspan and rowspan, so header cells
    # like 'Region' (rowspan=2) occupy their column across both header rows, and
    # 'date' (colspan=2) headers occupy two columns. grid[r][c] = (text, header).
    # This is what makes multi-level headers WITH rowspan row-labels align.
    grid: dict = {}          # (row, col) -> {"text":, "header":}
    occupied: set = set()
    n_cols = 0
    for ri, r in enumerate(rows):
        c = 0
        for cell in r:
            while (ri, c) in occupied:      # skip cells already filled by a rowspan
                c += 1
            cs = max(1, cell.get("colspan", 1))
            rs = max(1, cell.get("rowspan", 1))
            for dr in range(rs):
                for dc in range(cs):
                    grid[(ri + dr, c + dc)] = {"text": cell["text"],
                                               "header": cell["header"]}
                    occupied.add((ri + dr, c + dc))
            c += cs
            n_cols = max(n_cols, c)
    n_rows = len(rows)

    # which rows are HEADER rows (mostly header cells, no data numbers)?
    def _row_cells(ri):
        return [grid.get((ri, c), {"text": "", "header": False})
                for c in range(n_cols)]
    header_row_idx = []
    for ri in range(n_rows):
        cells = _row_cells(ri)
        hdr = sum(1 for c in cells if c["header"]) >= max(1, n_cols // 2) \
            and not any(_has_number(c["text"]) for c in cells if not c["header"])
        if hdr and (not header_row_idx or ri == header_row_idx[-1] + 1):
            header_row_idx.append(ri)
        elif header_row_idx:
            break
    body_row_idx = [ri for ri in range(n_rows) if ri not in header_row_idx]

    # column-header path per column: join the header rows' cells for that column
    # (rowspan cells repeat, so a spanned label like 'Region' appears once).
    col_headers: dict[int, str] = {}
    for c in range(n_cols):
        parts = []
        for ri in header_row_idx:
            t = _clean(grid.get((ri, c), {}).get("text", ""))
            if t and t not in parts:        # dedup repeated rowspan text
                parts.append(t)
        col_headers[c] = " ".join(parts)

    # how many LEADING columns are row-label columns (their header is a label,
    # and body cells are non-numeric) — captures nested row hierarchy.
    n_label_cols = 0
    for c in range(min(n_cols, 5)):
        body_texts = [_clean(grid.get((ri, c), {}).get("text", ""))
                      for ri in body_row_idx]
        if any(_has_number(t) for t in body_texts if t):
            break
        if not any(body_texts):
            break
        n_label_cols += 1
    n_label_cols = max(1, n_label_cols)

    claims = []
    eid = f"{filename}-p{page}-aiparsetbl"
    section = None
    carry: dict = {}                        # carry merged/blank nested labels down

    for ri in body_row_idx:
        cells = _row_cells(ri)
        texts = [_clean(c["text"]) for c in cells]
        non_empty = [t for t in texts if t]
        joined = " ".join(non_empty)

        # SECTION row: a single distinct label spanning the width, no numbers.
        # In the grid a colspan section fills multiple cells with the SAME text,
        # so dedup before counting.
        distinct = list(dict.fromkeys(non_empty))
        if len(distinct) == 1 and not _has_number(distinct[0]) and n_cols > 1:
            section = distinct[0]
            continue

        # nested ROW path from the leading label columns (carry blanks down).
        row_parts = []
        for c in range(n_label_cols):
            t = texts[c] if c < len(texts) else ""
            if t:
                carry[c] = t
                row_parts.append(t)
            elif c in carry:
                row_parts.append(carry[c])
        row_label = " | ".join(row_parts)

        # a claim per numeric data cell (columns after the label columns).
        for c in range(n_label_cols, n_cols):
            val = texts[c] if c < len(texts) else ""
            if val and _has_number(val):
                header = col_headers.get(c, "")
                parts = [x for x in (title, section, row_label, header) if x]
                name = " | ".join(parts) if parts else (header or row_label or "value")
                claims.append(claim_cls(
                    field_name=header or "value", canonical_metric=name,
                    value=val, unit=_extract_unit(val), element_type="table",
                    source_element_id=eid,
                    page=page, entity_ref=entity_ref, confidence=0.9,
                    citation_tier=_ai_parse_tier(),
                    model_used="ai_parse_rule_structured"))
    return claims


# ---- numbers embedded in text ----

# a value = a currency/number token, optionally with a unit suffix.
_VALUE_RE = re.compile(
    r"(?:[£$€]\s?\d[\d,]*(?:\.\d+)?\s?(?:m|bn|k|bps|%)?"
    r"|\d[\d,]*(?:\.\d+)?\s?(?:m|bn|k|bps|%|million|billion))",
    re.I)


def chart_description_to_claims(description: str, filename: str,
                                page: Optional[int] = None, title: Optional[str] = None,
                                team=None, report_type=None, entity_ref=None,
                                claim_cls=None) -> list:
    """Parse an ai_parse figure DESCRIPTION (multimodal, often contains the
    actual chart values) into label->value claims. E.g.

      "A vertical bar chart titled 'Quarterly Revenue 2025' ... Q1 at
       approximately $12M, Q2 at $15M, Q3 at $14M, and Q4 peaking at $22M."

    -> 'Quarterly Revenue 2025 | Q1 = $12M', '... | Q2 = $15M', etc.

    Rule-based, no LLM. Pairs each value with the LABEL that precedes it,
    stripping filler words ('at', 'approximately', 'peaking at'). Uses the
    chart's own title (parsed from the description) as the name prefix when
    present.
    """
    if claim_cls is None:
        from ..shared.schema import Claim as claim_cls
    desc = _clean(description)
    if not desc:
        return []

    # chart title, if the description states one: titled '...' or "..."
    chart_title = None
    m = re.search(r"titled\s+['\"]([^'\"]+)['\"]", desc, re.I)
    if m:
        chart_title = m.group(1).strip()
    prefix = " | ".join(x for x in (title, chart_title) if x) or None

    # filler tokens between a label and its value.
    _FILLER = re.compile(
        r"\b(?:at|of|is|was|are|reached|reaching|peaking|approximately|approx|"
        r"about|around|roughly|circa|=|:|to)\b", re.I)

    claims = []
    eid = f"{filename}-p{page}-fig"
    # find each value, then take the label = the token(s) just before it,
    # stripped of filler.
    for m in _VALUE_RE.finditer(desc):
        val = _clean(m.group(0))
        before = desc[:m.start()]
        # last clause before the value (split on comma/'and'/semicolon)
        clause = re.split(r",|;|\band\b", before, flags=re.I)[-1]
        clause = _FILLER.sub(" ", clause)          # drop filler words
        # strip leading narrative phrases ("the data shows", "the chart shows")
        clause = re.sub(r".*\b(?:shows?|shown|following|below|are)\b[:\s]*", "",
                        clause, flags=re.I)
        label = _clean(clause).split()
        label = " ".join(label[-4:])               # last few words = the label
        label = _clean(label)
        if not label:
            continue
        name = " | ".join(x for x in (prefix, label) if x)
        claims.append(claim_cls(
            field_name=label, canonical_metric=name, value=val,
            unit=_extract_unit(val), element_type="text", source_element_id=eid, page=page,
            entity_ref=entity_ref, confidence=0.75,
            citation_tier=_ai_parse_tier(from_figure=True),
            model_used="ai_parse_desc_structured"))
    return claims


def numbers_from_text(text: str, filename: str, page: Optional[int] = None,
                      title: Optional[str] = None, team=None, report_type=None,
                      entity_ref=None, claim_cls=None) -> list:
    """Extract numeric/monetary values embedded in prose, with the surrounding
    phrase as the metric name. E.g. 'X Account purchased £125m' ->
    name 'X Account purchased', value '£125m'. Rule-based, no LLM.

    Only fires on values carrying a unit or currency (£125m, 15%, 30bps) — a
    bare integer in prose is too ambiguous to name reliably and is skipped.
    """
    if claim_cls is None:
        from ..shared.schema import Claim as claim_cls
    t = _clean(text)
    if not t:
        return []
    claims = []
    eid = f"{filename}-p{page}-aiparsetxt"
    for m in _VALUE_RE.finditer(t):
        val = _clean(m.group(0))
        # the metric name = the few words immediately BEFORE the value.
        before = t[:m.start()].rstrip()
        # take up to the last ~6 words before the number as the name
        name_words = before.split()[-6:]
        name = _clean(" ".join(name_words))
        if not name:
            continue                          # a number with no context -> skip
        if title:
            name = f"{title} | {name}"
        claims.append(claim_cls(
            field_name=name, canonical_metric=name, value=val,
            unit=_extract_unit(val), element_type="text", source_element_id=eid, page=page,
            entity_ref=entity_ref, confidence=0.7,
            citation_tier=_ai_parse_tier(),
            model_used="ai_parse_rule_structured"))
    return claims

# ** SCHEMA
"""Shared data contracts used across every module.

Uses Pydantic v2. Validation failures route to a quarantine table rather
than crash a run (see ``build_chunk`` / ``build_claim``). The ``metrics``
field is deliberately a free ``dict`` so document schemas that vary by
version are absorbed rather than rejected.
"""
from __future__ import annotations

import hashlib
import re
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


class Modality(str, Enum):
    TEXT = "text"
    TABLE = "table"
    IMAGE = "image"
    FIGURE = "figure"
    KEYVAL = "keyval"


class ChunkType(str, Enum):
    RAW_TEXT = "raw_text"
    STRUCTURED_NL = "structured_nl"
    NORMALISATION_CONFLICT = "normalisation_conflict"


class CitationTier(str, Enum):
    """How a value was obtained — its source provenance, independent of the
    model's confidence or the review lifecycle. A deterministically parsed
    table cell and a value read from a chart image carry different real-world
    reliability even at the same confidence. (Adopted from the uploaded
    codebase's four-way tier.)"""
    PARSED = "parsed"                # deterministic structural output (table/text)
    LLM_ESTIMATED = "llm_estimated"  # read from interpreting a figure/chart image
    DERIVED = "derived"              # computed/matched downstream (e.g. claim link)
    MANUAL = "manual"                # entered/corrected by a human reviewer


class ReviewTier(str, Enum):
    """Where a value sits in the human-review lifecycle — a separate axis from
    CitationTier (provenance). A parsed value can still be human-confirmed;
    a chart-read value can still be auto-accepted."""
    MODEL_AUTO = "review_auto"
    HUMAN_CONFIRMED = "review_confirmed"
    HUMAN_OVERRIDE = "review_override"


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


# --- value parsing --------------------------------------------------------

_NUM_RE = re.compile(r"-?\d[\d,]*\.?\d*")


def parse_numeric(raw: str) -> Optional[float]:
    """Best-effort numeric parse of a regulatory value string.

    Handles '14.2%', '1,234', '£5m', ' 12.3 bps'. Returns ``None`` when no
    number can be recovered (caller decides how to treat that). Suffixes like
    'm'/'bn' are NOT scaled here — scale is a first-class structural field and
    scaling belongs in normalisation, not parsing.
    """
    if raw is None:
        return None
    m = _NUM_RE.search(str(raw).replace(",", ""))
    if not m:
        return None
    try:
        return float(m.group())
    except ValueError:
        return None


class Element(BaseModel):
    element_id: str
    modality: Modality
    content: str
    source_document: str
    page: Optional[int] = None
    bbox: Optional[tuple[float, float, float, float]] = None
    team: Optional[str] = None
    report_type: Optional[str] = None
    entity_ref: Optional[str] = None
    content_hash: str = ""
    # Raw bytes of an embedded image (e.g. a chart picture in a .pptx/.docx),
    # when the parser can extract it directly. When present, the vision path
    # uses these bytes as-is instead of rendering+cropping a page — higher
    # quality and works for formats with no page renderer.
    image_bytes: Optional[bytes] = None
    # Structural signals from the parser, used by complexity routing and the
    # crop-and-zoom path. description carries the parser's caption for a
    # figure (used to detect chart-like figures); has_merged_cells marks
    # structurally complex tables.
    description: Optional[str] = None
    has_merged_cells: bool = False

    @field_validator("content_hash", mode="before")
    @classmethod
    def _fill_hash(cls, v: str, info: Any) -> str:
        return v or content_hash(info.data.get("content", ""))


class Claim(BaseModel):
    field_name: str                     # raw name as found
    element_type: Optional[str] = None  # source element: table / text / figure
    canonical_metric: Optional[str] = None
    value: str
    unit: Optional[str] = None
    reporting_basis: Optional[str] = None
    netting: Optional[str] = None
    scale: Optional[str] = None
    as_at_date: Optional[str] = None
    confidence: float = 1.0
    source_element_id: str
    entity_ref: Optional[str] = None
    model_used: Optional[str] = None
    needs_review: bool = False
    # Grounding: whether the value was found in the source text ("grounded"),
    # not found ("ungrounded" — possible hallucination), or "not_applicable"
    # for image/chart claims that can't be text-grounded. A hallucination guard.
    grounding: str = "grounded"
    # Provenance (how obtained) and review lifecycle are separate axes.
    citation_tier: CitationTier = CitationTier.PARSED
    review_tier: ReviewTier = ReviewTier.MODEL_AUTO
    # Structural classification (metric_normaliser): measure shape + period,
    # independent of the metric's name. reporting_basis above doubles as the
    # normaliser's "basis" axis.
    measure_type: Optional[str] = None
    period: Optional[str] = None
    # Figure locality: page + bbox let a chart-derived claim be cropped/zoomed
    # for re-reading and highlighted for a human reviewer.
    page: Optional[int] = None
    bbox: Optional[tuple[float, float, float, float]] = None
    # Advisory content-domain tag (risk / financial / tax / ... / unknown) so
    # teams can filter to their components. Advisory only — nothing is gated on
    # it. tag_source records which cascade rung produced it (heading/metric/
    # llm/unknown) for tag-quality auditing.
    domain_tag: Optional[str] = None
    tag_source: Optional[str] = None


class RegulatoryChunk(BaseModel):
    """A chunk pushed to Databricks AI Search.

    We own chunking and push ``content`` as plain text; AI Search embeds it.
    ``chunk_id`` must be a valid AI Search document key (letters, digits,
    underscore, dash, equals) — ``content_hash`` satisfies this.
    """
    chunk_id: str
    chunk_type: ChunkType = ChunkType.RAW_TEXT
    content: str
    entity_ref: str = Field(..., description="the pipeline firm reference number or LEI")
    source_document_id: str
    content_hash: str
    page: Optional[int] = None
    team: Optional[str] = None
    report_type: Optional[str] = None
    schema_version: str = "1.0"
    metrics: dict[str, Any] = Field(default_factory=dict)

    @field_validator("entity_ref")
    @classmethod
    def _entity_ref_required(cls, v: str) -> str:
        if not v:
            raise ValueError("entity_ref required (the pipeline ref or LEI)")
        return v


class GoldMetric(BaseModel):
    entity_ref: str
    canonical_metric: str
    field_name: Optional[str] = None
    value: str
    # Parsed numeric form of `value` for dashboards/aggregation (None when the
    # value isn't numeric, e.g. "N/A"). The string `value` stays authoritative
    # for audit; numeric_value is the derived, chart-ready number.
    numeric_value: Optional[float] = None
    # Team + report_type as COLUMNS (not just in the table name) so chat/SQL can
    # filter (WHERE team = ...). report_date is the document's reporting date,
    # populated for every row of that document.
    team: Optional[str] = None
    report_type: Optional[str] = None
    report_date: Optional[str] = None
    # Provenance so a user can verify a metric against the source: which
    # document and which page/slide it came from.
    source_document: Optional[str] = None
    page: Optional[int] = None
    unit: Optional[str] = None
    reporting_basis: Optional[str] = None
    scale: Optional[str] = None
    as_at_date: Optional[str] = None
    netting: Optional[str] = None
    measure_type: Optional[str] = None
    period: Optional[str] = None
    citation_tier: str = CitationTier.PARSED.value
    review_tier: str = ReviewTier.MODEL_AUTO.value
    # Advisory content-domain tag, carried into Gold so teams can filter to
    # their components when querying the curated table.
    domain_tag: Optional[str] = None


def build_chunk(raw: dict, quarantine: list[dict]) -> Optional[RegulatoryChunk]:
    """Validate a raw dict into a chunk; route failures to quarantine."""
    try:
        return RegulatoryChunk(**raw)
    except Exception as exc:  # pydantic.ValidationError or TypeError
        quarantine.append({"kind": "chunk", "raw": raw, "error": str(exc)})
        return None


def build_claim(raw: dict, quarantine: list[dict]) -> Optional[Claim]:
    """Validate a raw dict into a Claim; route failures to quarantine.

    Mirrors ``build_chunk`` so a malformed model row flags-not-crashes,
    consistent with the pipeline's flag-never-reject principle.
    """
    try:
        return Claim(**raw)
    except Exception as exc:
        quarantine.append({"kind": "claim", "raw": raw, "error": str(exc)})
        return None
    
# LLM CLIENT
"""Unified async LLM client over Databricks serving endpoints.

Databricks exposes Foundation Model APIs and External Models through a
single OpenAI-compatible surface, so one client handles every model tier
by switching the ``model`` (endpoint) string. No provider-specific classes.

The client speaks the OpenAI Chat Completions shape, including tool-calling
with forced ``tool_choice`` so extraction output is always structured JSON.

**Async:** ``complete`` and ``extract`` are coroutines using the async OpenAI
client. Concurrency is bounded by a semaphore (so many element calls run at
once without exceeding the endpoint's rate limit), and rate-limit / transient
errors are retried with exponential backoff. This is what lets the per-element
pipeline make its many small calls in overlapping waves rather than one-by-one.
"""
from __future__ import annotations

import asyncio
import json
import random
from dataclasses import dataclass
from typing import Optional

from .config import CONFIG

# Optional MLflow tracing of every model call (prompt/response/latency/tokens).
# Enabled only when MLflow is importable AND config asks for it — so the pipeline
# runs unchanged where MLflow isn't present. Databricks has MLflow built in.
def _mlflow_trace(name):
    def _decorator(fn):
        if not getattr(getattr(CONFIG, "observability", None), "mlflow_tracing", False):
            return fn
        try:
            import mlflow
            return mlflow.trace(name=name)(fn)
        except Exception:
            return fn
    return _decorator

# Bound concurrent in-flight model calls. Tune to the endpoint's rate limit;
# too high just trades waiting for throttling + retries. Overridable via config.
_MAX_CONCURRENCY = getattr(CONFIG, "max_model_concurrency", 8)
_RETRY_ATTEMPTS = 8
_RETRY_BASE_DELAY = 2.0   # seconds; doubles each attempt with jitter

# Substrings of endpoint names whose models reject temperature/top_p/top_k
# (e.g. Claude Sonnet 5 returns 400 if any are sent). Matched by substring so
# it covers versioned endpoint names like databricks-claude-sonnet-5.
_NO_SAMPLING_PARAM_MODELS = ("claude-sonnet-5",)


@dataclass
class LLMResponse:
    text: str
    tool_arguments: Optional[dict] = None
    input_tokens: int = 0
    output_tokens: int = 0


class LLMClient:
    """Thin wrapper around the Databricks OpenAI-compatible endpoint.

    Locally (no Databricks host) it raises on use unless a base URL and key
    are supplied, keeping with the no-silent-fallback principle: tests use
    the stub client in ``tests/`` rather than a hidden fake here.
    """

    def __init__(self, base_url: Optional[str] = None,
                 api_key: Optional[str] = None,
                 max_concurrency: int = _MAX_CONCURRENCY):
        self._base_url = base_url or _default_base_url()
        self._api_key = api_key or CONFIG.databricks_token or CONFIG.anthropic_api_key
        self._client = None  # lazily created AsyncOpenAI client
        # One semaphore per client instance caps how many calls are in flight
        # at once across all concurrent element extractions.
        self._sem = asyncio.Semaphore(max_concurrency)

    def _ensure_client(self):
        if self._client is None:
            # openai>=1.0 async client, base_url pointing at Databricks.
            from openai import AsyncOpenAI  # imported lazily; optional at import

            if not self._base_url or not self._api_key:
                raise RuntimeError(
                    "LLMClient needs a base_url and api_key. Configure the "
                    "Databricks serving endpoint host and a secret; refusing "
                    "to fall back to a stub."
                )
            self._client = AsyncOpenAI(base_url=self._base_url,
                                       api_key=self._api_key)
        return self._client

    async def _create(self, **kwargs):
        """Make one chat-completion call under the concurrency cap, retrying
        rate-limit / transient errors with exponential backoff + jitter."""
        # Some Databricks foundation models reject sampling params: Claude
        # Sonnet 5 returns 400 if temperature / top_p / top_k are present. Strip
        # them for those endpoints rather than let the call fail.
        model = kwargs.get("model", "")
        if any(tok in model for tok in _NO_SAMPLING_PARAM_MODELS):
            for pname in ("temperature", "top_p", "top_k"):
                kwargs.pop(pname, None)
        client = self._ensure_client()
        last_exc = None
        for attempt in range(_RETRY_ATTEMPTS):
            try:
                async with self._sem:
                    return await client.chat.completions.create(**kwargs)
            except Exception as exc:  # narrow at integration time to RateLimitError etc.
                last_exc = exc
                if not _is_retryable(exc) or attempt == _RETRY_ATTEMPTS - 1:
                    raise
                delay = _RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0, 0.5)
                await asyncio.sleep(delay)
        raise last_exc  # pragma: no cover

    async def complete(self, model_key: str, prompt: str,
                       content: str) -> LLMResponse:
        """Plain completion, used by the query router's classification call."""
        spec = CONFIG.model(model_key)
        resp = await self._create(
            model=spec.endpoint,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": content},
            ],
            temperature=0,
        )
        usage = resp.usage
        return LLMResponse(
            text=resp.choices[0].message.content or "",
            input_tokens=getattr(usage, "prompt_tokens", 0),
            output_tokens=getattr(usage, "completion_tokens", 0),
        )

    async def extract(self, model_key: str, prompt: str, content: str,
                      tool_schema: dict,
                      image_base64: Optional[str] = None) -> LLMResponse:
        """Forced tool-use so the model returns structured JSON, not prose.

        When ``image_base64`` is provided (a cropped-and-zoomed figure region
        from ``search.figure_preprocessor``), it is sent as a multimodal image
        block alongside the text, so the model reads the chart from a legible
        crop rather than a full page. Text-only extraction is unchanged when
        it is None.

        Not every endpoint honours forced ``tool_choice`` identically (the
        FMAPI OpenAI-compatible surface for Claude/Llama has historically
        differed from External Models). We therefore parse defensively: if
        no tool call comes back, ``tool_arguments`` is ``None`` and the
        caller escalates or quarantines rather than crashing.
        """
        spec = CONFIG.model(model_key)
        tool_name = tool_schema["name"]

        # A figure/chart element has an image but often EMPTY text content (the
        # data lives in the image). An empty text block — or an empty text-only
        # message — is rejected by the API ("messages must have non-empty
        # content"). So fall back to a minimal instruction when content is blank.
        text_part = content if (content and content.strip()) else (
            "Extract all metrics visible in this image." if image_base64
            else "Extract all metrics from this element.")

        if image_base64 is not None:
            user_content = [
                {"type": "image_url",
                 "image_url": {"url": f"data:image/png;base64,{image_base64}"}},
                {"type": "text", "text": text_part},
            ]
        else:
            user_content = text_part

        resp = await self._create(
            model=spec.endpoint,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": user_content},
            ],
            tools=[{"type": "function", "function": tool_schema}],
            tool_choice={"type": "function", "function": {"name": tool_name}},
            temperature=0,
        )
        msg = resp.choices[0].message
        args = _safe_tool_args(msg)
        usage = resp.usage
        return LLMResponse(
            text=msg.content or "",
            tool_arguments=args,
            input_tokens=getattr(usage, "prompt_tokens", 0),
            output_tokens=getattr(usage, "completion_tokens", 0),
        )


def _is_retryable(exc: Exception) -> bool:
    """Whether an error is worth retrying with backoff. Rate-limit and
    transient server/network errors are; a bad request is not. Matched by
    class name so we don't hard-import openai's exception types at module load
    (kept optional). Narrow this at integration time to the real classes.
    """
    name = type(exc).__name__
    retryable = {"RateLimitError", "APITimeoutError", "APIConnectionError",
                 "InternalServerError", "APIError"}
    return name in retryable


def _safe_tool_args(msg) -> Optional[dict]:
    """Extract and JSON-parse tool-call arguments, tolerating malformed output.

    Returns ``None`` if there is no usable JSON — never raises. This keeps a
    single bad model response from killing a whole element's extraction.

    Two sources: a proper tool call (preferred), or — for models that return
    the JSON as plain text content instead of a tool call (e.g. reasoning
    models like gpt-oss) — the message content parsed as JSON. Without the
    content fallback, every such response yields no claims and quarantines.
    """
    import re

    # 1. preferred: a real tool call with JSON arguments
    tool_calls = getattr(msg, "tool_calls", None)
    if tool_calls:
        raw = tool_calls[0].function.arguments
        if raw:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return parsed
            except (json.JSONDecodeError, TypeError):
                pass

    # 2. fallback: the model returned JSON as text content. Reasoning models
    #    sometimes wrap it in prose, so locate the first {...claims...} object
    #    rather than assuming the whole string is JSON.
    content = getattr(msg, "content", None)
    if isinstance(content, str) and content.strip():
        try:
            parsed = json.loads(content)
            if isinstance(parsed, dict):
                return parsed
        except (json.JSONDecodeError, TypeError):
            pass
        m = re.search(r"\{.*\"claims\".*\}", content, re.DOTALL)
        if m:
            try:
                parsed = json.loads(m.group(0))
                if isinstance(parsed, dict):
                    return parsed
            except (json.JSONDecodeError, TypeError):
                pass
    return None


def _default_base_url() -> Optional[str]:
    import os

    host = os.environ.get("DATABRICKS_HOST")
    if host:
        return f"{host.rstrip('/')}/serving-endpoints"
    return None


# Aligned with the ``Claim`` contract: every optional structural field the
# reconciler / normaliser / Gold layer relies on is requestable here, so the
# model can actually return scale, netting and as_at_date (previously inert).
EXTRACTION_TOOL_SCHEMA: dict = {
    "name": "record_claims",
    "description": "Record structured regulatory claims found in the content.",
    "parameters": {
        "type": "object",
        "properties": {
            "claims": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "field_name": {
                            "type": "string",
                            "description": "Metric name exactly as written.",
                        },
                        "value": {
                            "type": "string",
                            "description": "Value exactly as written, incl. sign.",
                        },
                        "unit": {
                            "type": "string",
                            "description": "e.g. '%', 'bps', 'GBP'. Empty if none.",
                        },
                        "reporting_basis": {
                            "type": "string",
                            "description": "e.g. 'consolidated', 'solo'. Empty if unstated.",
                        },
                        "netting": {
                            "type": "string",
                            "description": "e.g. 'net of deductions', 'gross'. Empty if unstated.",
                        },
                        "scale": {
                            "type": "string",
                            "description": "e.g. 'millions', 'thousands', 'ratio'. Empty if unstated.",
                        },
                        "as_at_date": {
                            "type": "string",
                            "description": "Reporting/reference date if stated (ISO-8601 preferred).",
                        },
                        "confidence": {
                            "type": "number",
                            "description": "0-1 confidence this claim is correct.",
                        },
                    },
                    "required": ["field_name", "value", "confidence"],
                },
            }
        },
        "required": ["claims"],
    },
}
