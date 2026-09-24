# ================== hybrid extraction ====================
# Change 1 — add the coord field to the ParsedElement dataclass. Find:
@dataclass
class ParsedElement:
    """One normalised element out of ai_parse_document."""
    el_type: str
    content: Optional[str]
    confidence: Optional[float]
    page: Optional[int]
    bbox: Optional[list]
    description: Optional[str] = None

# Replace with
@dataclass
class ParsedElement:
    """One normalised element out of ai_parse_document."""
    el_type: str
    content: Optional[str]
    confidence: Optional[float]
    page: Optional[int]
    bbox: Optional[list]
    description: Optional[str] = None
    coord: Optional[list] = None

# Change 2 — extract the coord in parse_ai_parse_response. Find the loop that builds elements:
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

# Replace with
    for e in elements:
        if not isinstance(e, dict):
            continue
        page = None
        coord = None
        bbox = e.get("bbox")
        try:
            if bbox and isinstance(bbox, list) and isinstance(bbox[0], dict):
                page = bbox[0].get("page_id")
                b0 = bbox[0]
                # the rectangle can live under several keys depending on the
                # ai_parse version — surface whichever is present.
                coord = (b0.get("coord") or b0.get("bbox") or b0.get("polygon")
                         or b0.get("rectangle") or b0.get("box"))
                if coord is None and all(k in b0 for k in ("x0", "y0", "x1", "y1")):
                    coord = [b0.get("x0"), b0.get("y0"), b0.get("x1"), b0.get("y1")]
                if coord is None and all(k in b0 for k in ("left", "top", "right", "bottom")):
                    coord = [b0.get("left"), b0.get("top"), b0.get("right"), b0.get("bottom")]
        except Exception:
            page = None
            coord = None
        out.append(ParsedElement(
            el_type=str(e.get("type", "")).lower(),
            content=e.get("content"),
            confidence=e.get("confidence"),
            page=page,
            bbox=bbox,
            description=e.get("description"),
            coord=coord,
        ))
    return out

# Change 3 — add the diagnostic function. Add this new function right BEFORE def parse_ai_parse_response(:
def inspect_bbox_structure(resp: Any, max_show: int = 5) -> list:
    """DIAGNOSTIC: show the RAW bbox structure ai_parse returns for figures, so
    you can see exactly which key holds the coordinate. Prints and returns the
    raw bbox dicts. Use this to confirm the coord key name."""
    import json as _json
    data = resp
    if type(resp).__name__ == "VariantVal":
        for meth in ("toJson", "to_json"):
            if hasattr(resp, meth):
                data = _json.loads(getattr(resp, meth)()); break
    elif isinstance(resp, str):
        data = _json.loads(resp)
    elif hasattr(resp, "asDict"):
        data = resp.asDict(recursive=True)
    doc = (data or {}).get("document") or {}
    out, shown = [], 0
    for e in doc.get("elements") or []:
        if not isinstance(e, dict):
            continue
        if str(e.get("type", "")).lower() == "figure":
            bbox = e.get("bbox")
            out.append(bbox)
            if shown < max_show:
                print(f"figure bbox raw structure: {bbox}")
                if bbox and isinstance(bbox, list) and isinstance(bbox[0], dict):
                    print(f"  keys in bbox[0]: {list(bbox[0].keys())}")
                shown += 1
    if not out:
        print("no figure elements found")
    return out

# ==== ai_parse_pipeline =============
#Change 1 — in run_ai_parse_batch (around line 327-330). Find:
                from .storage import derive_report_date
                rdate = derive_report_date(claims, None, path=path)
# replace with
                from .storage import derive_report_date
                from .hybrid_extraction import parse_ai_parse_response
                _title_els = parse_ai_parse_response(parsed)
                rdate = derive_report_date(claims, _title_els, path=path)
# change 2
            from .storage import derive_report_date
            rdate = derive_report_date(claims, None, path=path)
#replace with
            from .storage import derive_report_date
            from .hybrid_extraction import parse_ai_parse_response
            _title_els = parse_ai_parse_response(parsed)
            rdate = derive_report_date(claims, _title_els, path=path)

# ============== storage.py ==================
# Find this section (source 2, the TITLE logic):
    # 2. TITLE / heading text — search the first several text elements for a
    #    date/period embedded anywhere in the heading.
    if elements:
        text_els = [e for e in elements
                    if str(getattr(getattr(e, "modality", None), "value",
                                   getattr(e, "modality", ""))) in ("text", "TEXT")]
        for e in text_els[:5]:  # titles/headings are near the top
            content = (getattr(e, "content", "") or "")
            found = _extract_date_from_text(content)
            if found:
                return found

# Replace with
    # 2. TITLE / heading text — search the first several text elements for a
    #    date/period embedded anywhere in the heading.
    if elements:
        def _is_text_el(e):
            # support both shapes: ai_parse ParsedElement (.el_type) and the
            # older modality-based elements (.modality.value).
            et = getattr(e, "el_type", None)
            if et is not None:
                return str(et).lower() in ("text", "title", "section_header",
                                           "caption", "heading")
            mod = getattr(getattr(e, "modality", None), "value",
                          getattr(e, "modality", ""))
            return str(mod).lower() == "text"
        text_els = [e for e in elements if _is_text_el(e)]
        for e in text_els[:8]:  # titles/headings are near the top
            content = (getattr(e, "content", "") or "") or (getattr(e, "description", "") or "")
            found = _extract_date_from_text(content)
            if found:
                return found

# new ai_parse_pipeline_20260924
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


def _extract_pages_array(parsed_response):
    """Pull ai_parse's ``pages`` array (each {id, image_uri}) from the response,
    across the surfaces ai_parse returns (VariantVal / JSON string / dict). This
    gives the RELIABLE page->image mapping. Returns [] if not present."""
    import json as _json
    data = parsed_response
    try:
        if type(parsed_response).__name__ == "VariantVal":
            for meth in ("toJson", "to_json"):
                if hasattr(parsed_response, meth):
                    data = _json.loads(getattr(parsed_response, meth)())
                    break
        elif isinstance(parsed_response, str):
            data = _json.loads(parsed_response)
        elif hasattr(parsed_response, "asDict"):
            data = parsed_response.asDict(recursive=True)
    except Exception:
        return []
    if not isinstance(data, dict):
        return []
    # pages can be at the top level or under 'document'
    pages = data.get("pages")
    if pages is None:
        pages = (data.get("document") or {}).get("pages")
    return pages if isinstance(pages, list) else []


def _build_figure_crop_fn(spark, image_output_path, dbutils_ref,
                          all_elements=None, save_crops_to=None,
                          pages=None):
    """Build a crop_figure_image(el) callable for Option A.

    ai_parse renders WHOLE PAGES to images. We send the figure's FULL PAGE image
    (full resolution) to Sonnet — Sonnet is instructed (prompt) and enforced
    (source_type) to read CHARTS ONLY.

    PAGE->IMAGE MAPPING: we use ai_parse's ``pages`` array, which gives an exact
    ``image_uri`` per page id — a STABLE, correct mapping. The old approach
    (sort images by modification time and map page N -> Nth image) was
    unreliable: if images didn't write in page order, a figure mapped to the
    WRONG page's image (e.g. a page of tables), so Sonnet read tables, not the
    chart — and the mapping could differ between runs. image_uri fixes that.
    Modification-time order is kept only as a last-resort fallback.

    save_crops_to: optional folder — saves each page image sent to Sonnet.
    """
    from PIL import Image

    # RELIABLE map: page_id -> image path, from ai_parse's pages array.
    page_uri = {}
    for pg in (pages or []):
        if isinstance(pg, dict):
            pid = pg.get("id")
            uri = pg.get("image_uri") or pg.get("imageUri") or pg.get("image")
            if pid is not None and uri:
                page_uri[pid] = uri

    # fallback only: images sorted by modification time (old, unreliable).
    try:
        imgs = sorted(dbutils_ref.fs.ls(image_output_path),
                      key=lambda f: f.modificationTime)
    except Exception:
        imgs = []

    def _norm(p):
        if not p:
            return None
        if p.startswith("dbfs:/Volumes"):
            return p.replace("dbfs:", "")
        if p.startswith("dbfs:"):
            return p.replace("dbfs:", "/dbfs")
        return p

    def _page_image(page_id):
        # PRIMARY: exact image_uri for this page id (ai_parse pages array).
        # ai_parse page ids are commonly 0-based in the pages array; try the id
        # as-is and page_id-1 to be safe.
        for key in (page_id, (page_id - 1) if page_id else 0):
            if key in page_uri:
                try:
                    return Image.open(_norm(page_uri[key]))
                except Exception:
                    pass
        # FALLBACK: modification-time order (only if pages array missing).
        idx = (page_id - 1) if page_id else 0
        if 0 <= idx < len(imgs):
            try:
                return Image.open(_norm(imgs[idx].path))
            except Exception:
                return None
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
                # build the whole-page image fn. Pass ai_parse's pages array so
                # the page->image mapping uses the reliable image_uri (not the
                # unstable modification-time sort that sent wrong pages/tables).
                _pages = _extract_pages_array(parsed)
                crop_fn = _build_figure_crop_fn(
                    spark, image_output_path, dbutils,
                    all_elements=_els, save_crops_to=_SAVE_CROPS_TO,
                    pages=_pages)

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
                from .hybrid_extraction import parse_ai_parse_response
                _title_els = parse_ai_parse_response(parsed)
                rdate = derive_report_date(claims, _title_els, path=path)
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
            from .hybrid_extraction import parse_ai_parse_response
            _title_els = parse_ai_parse_response(parsed)
            rdate = derive_report_date(claims, _title_els, path=path)

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

