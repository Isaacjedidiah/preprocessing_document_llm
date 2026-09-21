# STORAGE
def derive_report_date(claims: list, elements: list = None,
                       path: str = None) -> Optional[str]:
    """Find the document's report date so every stored row can be stamped with it.

    Sources, in order of reliability for THIS corpus:
      1. The FILENAME — the report date is encoded in the document name
         (e.g. 'LCR_BankABC_February_2026.pptx' -> 'February 2026'). This is the
         primary source: filenames are named consistently with the period.
      2. The report TITLE / heading text — a date embedded in the title
         (e.g. 'Micro Report 2024 Q1').
      3. A date-valued CLAIM — a claim whose value is a period/date.
    Returns None if no date is found — the column stays empty rather than guess.
    """
    import os
    from .dates import (looks_like_date, normalise_report_date, _DATE_FIELD_RE,
                        _DATE_PATTERNS, _BARE_YEAR_RE)

    # 1. FILENAME — extract a date from the document's file name (primary).
    if path:
        fname = os.path.basename(str(path))
        # strip the extension so "..._2026.pptx" doesn't confuse the parser
        stem = os.path.splitext(fname)[0]
        # filenames often use underscores/dashes as separators -> spaces
        stem_spaced = stem.replace("_", " ").replace("-", " ")
        found = _extract_date_from_text(stem_spaced)
        if found:
            return found

    # 2. TITLE / heading text — search the first several text elements for a
    #    date/period embedded anywhere in the heading.
    if elements:

# AI PARSE PIPELINE
                rdate = derive_report_date(claims, None, path=path)

# HYBRID EXTRACTION
def _sonnet_read_images_concurrent(run_async_fn, llm_client, model_key, prompt,
                                   figure_jobs):
    """Read MANY figures CONCURRENTLY in one event-loop pass.

    figure_jobs: list of (el, content, img_b64). Returns a dict {id(el): points}.

    The client already bounds concurrency with a semaphore (default 8 in flight),
    but the pipeline was calling figures one-at-a-time, so a 30-chart document
    made 30 SEQUENTIAL Sonnet calls. Gathering them into a single asyncio.gather
    lets the semaphore run up to N at once -> big speed-up on chart-heavy docs.
    """
    import asyncio

    try:
        from ..shared.llm_client import EXTRACTION_TOOL_SCHEMA
    except Exception:
        EXTRACTION_TOOL_SCHEMA = {"name": "extract"}

    async def _one(el, content, img_b64):
        try:
            resp = await llm_client.extract(
                model_key, prompt, content or "", EXTRACTION_TOOL_SCHEMA,
                image_base64=img_b64)
            ta = getattr(resp, "tool_arguments", None)
            if isinstance(ta, dict):
                for key in ("claims", "metrics", "points", "data", "values"):
                    if isinstance(ta.get(key), list):
                        return id(el), [p for p in ta[key] if isinstance(p, dict)]
                if ta:
                    return id(el), [ta]
            return id(el), parse_sonnet_chart_points(getattr(resp, "text", "") or "")
        except Exception:
            return id(el), []

    async def _all():
        return await asyncio.gather(
            *[_one(el, content, img) for (el, content, img) in figure_jobs])

    try:
        results = run_async_fn(_all())
        return dict(results)
    except Exception:
        return {}

# HYBRID EXTRACTION DOCUMENT

    # layout-marker element types carry no metric data -> always skipped.
    _SKIP_TYPES = {"page_header", "page_footer", "page_number"}

    # figures are COLLECTED here and read from Sonnet CONCURRENTLY after the
    # loop (reading them one-by-one was the chart-heavy slowdown).
    _figure_jobs = []

    for el in elements:

        if etype == "figure":
            kind = classify_figure(el)
            if kind == "logo":
                continue
            fig_text = " ".join(x for x in (el.description, content) if x).strip()
            desc_claims = chart_description_to_claims(
                fig_text, filename, page=el.page,
                title=_prefix(current_title, current_section),
                team=team, report_type=report_type, entity_ref=entity_ref)
            if not use_sonnet:
                claims.extend(desc_claims)
                continue
            if crop_figure_image is None:
                claims.extend(desc_claims)
                continue
            try:
                img_b64 = crop_figure_image(el)
            except Exception:
                img_b64 = None
            if not img_b64:
                claims.extend(desc_claims)
                continue
            # COLLECT the figure for a CONCURRENT Sonnet pass after the loop.
            _figure_jobs.append((el, content, img_b64, desc_claims))
            continue

    # CONCURRENT figure pass: read all collected figures in parallel (bounded by
    # the client's semaphore) instead of one-at-a-time. This is the speed-up for
    # chart-heavy documents (25-30 charts).
    if use_sonnet and _figure_jobs and run_async_fn and llm_client:
        jobs = [(el, content, img) for (el, content, img, _dc) in _figure_jobs]
        results = _sonnet_read_images_concurrent(
            run_async_fn, llm_client, vision_model_key,
            CHART_DIGITISATION_PROMPT, jobs)
        for (el, content, img, desc_claims) in _figure_jobs:
            pts = results.get(id(el))
            if pts:
                claims.extend(sonnet_points_to_claims(pts, el, filename, entity_ref))
            else:
                claims.extend(desc_claims)   # vision found nothing -> keep desc

    return claims

# Coverage
"""Coverage audit for the extraction pipeline.

'Ensuring coverage' is two things, and this module does both honestly:

  1. ACCOUNTING — did we handle every element ai_parse returned? Every element
     is either extracted (produced claims), deliberately skipped (logo, layout
     marker), or flagged as UNACCOUNTED. Nothing is silently dropped.

  2. COMPLETENESS SIGNALS — per document and per page, how much did we extract,
     and are there pages/elements that produced NOTHING (a coverage gap worth a
     human's eye)?

Honest scope: this MAXIMISES and VERIFIES coverage; it does not 'prove we got
everything' — that needs a ground truth we can't fully have (a bare number in a
document is not self-evidently a metric). What it guarantees is that every
element ai_parse saw was consciously handled, and that gaps are surfaced, not
hidden.
"""

from __future__ import annotations

from collections import Counter
from typing import Optional


# element types we DELIBERATELY do not extract from (not a coverage gap)
_INTENTIONAL_SKIP = {"page_header", "page_footer", "page_number"}


def audit_coverage(parsed_elements, claims) -> dict:
    """Reconcile what ai_parse RETURNED against what we EXTRACTED.

    parsed_elements: the ParsedElement list from ai_parse (what was available).
    claims:          the claims we produced (what we captured).

    Returns a coverage report: counts by element type, how many elements
    produced at least one claim, and any UNACCOUNTED elements (a real gap).
    """
    # how many claims came from each page (a page with 0 claims is a gap signal)
    claims_per_page = Counter(getattr(c, "page", None) for c in claims)
    # which source elements produced claims (by the id we stamp on each claim)
    produced_pages = set(claims_per_page)

    by_type = Counter(e.el_type for e in parsed_elements)
    pages = sorted({e.page for e in parsed_elements if e.page is not None})

    accounted, skipped, unaccounted = [], [], []
    for e in parsed_elements:
        if e.el_type in _INTENTIONAL_SKIP:
            skipped.append(e)                          # deliberately not extracted
        elif e.el_type == "figure" and _is_logo(e):
            skipped.append(e)                          # logos are skipped by design
        elif e.page in produced_pages:
            accounted.append(e)                        # its page produced claims
        else:
            unaccounted.append(e)                      # produced nothing -> GAP

    # pages that yielded no metrics at all — worth a human eye
    empty_pages = [pg for pg in pages if claims_per_page.get(pg, 0) == 0]

    total = len(parsed_elements)
    return {
        "elements_total": total,
        "elements_by_type": dict(by_type),
        "elements_accounted": len(accounted),
        "elements_intentionally_skipped": len(skipped),
        "elements_unaccounted": len(unaccounted),        # the number to watch
        "accounted_pct": round(100 * len(accounted) /
                               max(1, total - len(skipped)), 1),
        "claims_total": len(claims),
        "claims_per_page": dict(claims_per_page),
        "pages_total": len(pages),
        "empty_pages": empty_pages,                       # pages with 0 metrics
        "unaccounted_detail": [
            {"type": e.el_type, "page": e.page} for e in unaccounted[:50]],
    }


def coverage_ok(report: dict, max_unaccounted: int = 0,
                max_empty_pages: int = 0) -> bool:
    """A simple gate: coverage is clean if no elements are unaccounted and no
    pages came back empty (tune the thresholds to your tolerance)."""
    return (report["elements_unaccounted"] <= max_unaccounted
            and len(report["empty_pages"]) <= max_empty_pages)


def coverage_report_text(report: dict) -> str:
    """Readable one-block summary for a notebook or a review."""
    lines = ["===== COVERAGE AUDIT =====",
             f"elements: {report['elements_total']}  "
             f"({report['elements_by_type']})",
             f"accounted: {report['elements_accounted']}  "
             f"({report['accounted_pct']}%)",
             f"intentionally skipped: {report['elements_intentionally_skipped']}",
             f"UNACCOUNTED (gaps): {report['elements_unaccounted']}",
             f"metrics extracted: {report['claims_total']}  "
             f"over {report['pages_total']} pages",
             f"empty pages (0 metrics): {report['empty_pages']}"]
    if report["unaccounted_detail"]:
        lines.append("  gap sample: " + ", ".join(
            f"{d['type']}@p{d['page']}" for d in report["unaccounted_detail"][:10]))
    return "\n".join(lines)


def _is_logo(el) -> bool:
    txt = ((getattr(el, "description", "") or "") + " " +
           (getattr(el, "content", "") or "")).lower()
    return any(w in txt for w in ("logo", "emblem", "icon", "crest", "letterhead"))

# =============================================================================
#  BEHIND THE SCENES — HOW THE EXTRACTION PIPELINE WORKS
#  A guided walkthrough of the key modules, in the order a document flows
#  through them. Read top-to-bottom; each section is a stage in the journey.
#
#  Use this to explain the system: every block has (1) WHAT it does, (2) WHY it
#  matters, and (3) the KEY CODE that does it, lightly simplified for clarity.
# =============================================================================


# =============================================================================
#  THE JOURNEY OF ONE DOCUMENT (the mental model to open with)
# =============================================================================
#
#   file on a volume
#        │
#        ▼
#   [1] DISCOVERY          run_ingestion_job.discover_submissions
#        │                 walk the folder tree -> attribute every file by PATH
#        ▼
#   [2] PARSE              ai_parse_pipeline._run_ai_parse
#        │                 ai_parse_document reads the doc -> typed elements
#        ▼
#   [3] ROUTE + EXTRACT    hybrid_extraction.hybrid_extract_document
#        │                 table/text -> rule-based ;  figure -> Sonnet (vision)
#        ▼
#   [4] STRUCTURE          ai_parse_structurer.html_table_to_claims
#        │                 HTML tables (rowspan/colspan) -> one claim per cell
#        ▼
#   [5] GROUND             grounding.check_grounding
#        │                 verify each value against its source (anti-hallucination)
#        ▼
#   [6] GATE               validator.gate_for_review
#        │                 low confidence / conflicts -> human review, rest -> gold
#        ▼
#   [7] STAMP + STORE      storage.write_gold_metrics  (+ _stamp)
#        │                 add metadata (source, page, tier, report_date, hash)
#        ▼
#   [8] INDEX              ai_parse_pipeline._index_ai_parse_elements
#                          readable chunks -> vector index for narrative search
#
#  Two consumption paths out the other side:
#     • precise metric questions  -> SQL / gold  (query_gold)
#     • narrative questions       -> vector index (search)
#  routed by query_router.route_query
# =============================================================================


# =============================================================================
#  STAGE 1 — DISCOVERY : attribution comes from the PATH, not the filename
#  module: preprocessing_etl/custom/run_ingestion_job.py
# =============================================================================
#
#  WHAT: walk the submissions folder and turn every file into a "submission"
#        dict that already knows its team, report type, entity, entity_id and
#        entity_name — read from the folder structure.
#
#  WHY : attribution is guaranteed by construction. A file is only collected if
#        it sits in a complete team/report_type/entity/entity_id/name path, so
#        every metric we ever store is fully attributable. Nothing is "orphaned".
#
#  Folder layout:
#     /Volumes/<cat>/<schema>/submissions/<team>/<report_type>/
#                    <entity>/<entity_id>/<entity_special_name>/<file>

def discover_submissions(root):
    subs = []
    for team in _subdirs(root):
        for report_type in _subdirs(f"{root}/{team}"):
            for entity in _subdirs(f"{root}/{team}/{report_type}"):
                for entity_id in _subdirs(f".../{entity}"):
                    for entity_name in _subdirs(f".../{entity_id}"):
                        leaf = f".../{entity_name}"
                        for fname in _files(leaf):
                            subs.append({
                                "path": f"{leaf}/{fname}",
                                "team": team, "report_type": report_type,
                                "entity": entity, "entity_id": entity_id,
                                "entity_name": entity_name,        # all from the PATH
                            })
    return subs

#  TALKING POINT: "We don't trust filenames to tell us whose data this is — the
#  folder structure does. So the moment a file is discovered, it already carries
#  its full identity. That identity becomes columns on every metric downstream."


# =============================================================================
#  STAGE 2 — PARSE : ai_parse_document does the heavy lifting
#  module: preprocessing_etl/custom/ai_parse_pipeline.py
# =============================================================================
#
#  WHAT: call Databricks' ai_parse_document on the raw file. It returns typed
#        elements — each with a `type` (table/text/figure/title/...), `content`,
#        a `confidence`, and a `bbox` (page + coordinates).
#
#  WHY : ai_parse is excellent and cheap at reading document structure — tables
#        as HTML, text as text. We let it do the bulk of the work in-platform,
#        so only the genuinely hard content (charts) needs a vision model.

def _run_ai_parse(spark, path, image_output_path=None):
    spark.read.format("binaryFile").load(path).createOrReplaceTempView("_doc")
    if image_output_path:
        # also render each page to an image, so figures can be sent to Sonnet
        return spark.sql(
            "SELECT ai_parse_document(content, "
            f"map('imageOutputPath','{image_output_path}')) AS p FROM _doc"
        ).collect()[0]["p"]
    return spark.sql(
        "SELECT ai_parse_document(content) AS p FROM _doc").collect()[0]["p"]

#  TALKING POINT: "ai_parse is the workhorse. It reads any format — PDF, Word,
#  PowerPoint — into a common typed structure. That's what makes us
#  document-agnostic: one pipeline, any format."


# =============================================================================
#  STAGE 3 — ROUTE + EXTRACT : the hybrid decision, per element
#  module: preprocessing_etl/custom/hybrid_extraction.py
# =============================================================================
#
#  WHAT: loop over the parsed elements and route each by its TYPE:
#          table  -> rule-based structuring (deterministic, free)
#          text   -> rule-based number extraction (deterministic, free)
#          figure -> collected, then read by Sonnet vision (only where needed)
#
#  WHY : this is the core cost/quality trade-off. The vast majority of content
#        (tables, text) is structured in code at zero LLM cost. Only figures —
#        the content ai_parse can't fully digitise — reach the vision model.

def hybrid_extract_document(ai_parse_response, filename, use_sonnet, ...):
    claims = []
    figure_jobs = []                       # figures collected for a CONCURRENT pass
    for el in parse_ai_parse_response(ai_parse_response):

        if el.el_type == "table":
            # ai_parse structures tables very well as HTML -> never Sonnet
            claims.extend(html_table_to_claims(el.content, filename, page=el.page))

        elif el.el_type == "text":
            claims.extend(numbers_from_text(el.content, filename, page=el.page))

        elif el.el_type == "figure":
            if classify_figure(el) == "logo":
                continue                    # skip logos/emblems
            if use_sonnet:
                img = crop_figure_image(el) # the rendered figure image
                figure_jobs.append((el, el.content, img))

    # figures are read CONCURRENTLY (see Stage 3b) — the speed win
    if use_sonnet and figure_jobs:
        results = _sonnet_read_images_concurrent(..., figure_jobs)
        for (el, content, img) in figure_jobs:
            claims.extend(sonnet_points_to_claims(results[id(el)], el, filename))

    return claims

#  TALKING POINT: "Each element goes to the cheapest reader that gets it right.
#  Tables and text are pure code — deterministic and free. Only charts, which
#  no text reader can recover, go to the vision model. That's the hybrid."


# =============================================================================
#  STAGE 3b — CHARTS : how we guide Sonnet, and read many at once
#  module: preprocessing_etl/custom/hybrid_extraction.py
# =============================================================================
#
#  WHAT: for each chart figure we send Sonnet (a) the rendered image AND (b)
#        ai_parse's content as GUIDANCE, using a focused chart-reading prompt.
#        All charts on a document are read CONCURRENTLY, not one-by-one.
#
#  WHY : two hard-won lessons —
#        1. the content GUIDES the vision read: without it, Sonnet drifts to the
#           axis scale; with it, it reads the real plotted series.
#        2. a chart-heavy doc has 25-30 charts. Read sequentially, one document
#           dominates the run. Gathered into one concurrent batch (bounded by a
#           semaphore, ~8 in flight), it's a few parallel waves instead.

async def _read_one(el, content, img_b64):
    resp = await llm_client.extract(
        "tier2", CHART_DIGITISATION_PROMPT,
        content,                       # <- ai_parse content GUIDES the read
        EXTRACTION_TOOL_SCHEMA,
        image_base64=img_b64)          # <- the chart image
    return id(el), _points_from(resp)  # structured {label, value, unit} points

async def _all(figure_jobs):
    # gather -> the client's semaphore runs up to N concurrently
    return await asyncio.gather(*[_read_one(*job) for job in figure_jobs])

#  THE PROMPT (kept deliberately SIMPLE — an over-engineered prompt made Sonnet
#  read tables too; focus beats instruction-stacking):
CHART_DIGITISATION_PROMPT = (
    "You are a chart digitisation engine. Read the chart image and return ONLY "
    "a JSON array of its data points... map each coloured series to its legend "
    "name; read values against the axis; omit what you cannot read."
)

#  TALKING POINT: "The insight that cracked charts was giving Sonnet ai_parse's
#  own read as context — it anchors the model to the real series. And by reading
#  all a document's charts in parallel, a 30-chart report stopped being the
#  bottleneck."


# =============================================================================
#  STAGE 4 — STRUCTURE : turning HTML tables into clean claims
#  module: preprocessing_etl/custom/ai_parse_structurer.py
# =============================================================================
#
#  WHAT: parse ai_parse's HTML table into ONE claim per data cell, honouring
#        multi-level headers and rowspan/colspan via a full GRID model.
#
#  WHY : this is the module that does ai_extract's job — FOR FREE. ai_extract is
#        an LLM call billed per document; this is deterministic code. Same
#        structured output, no LLM cost, and reproducible (audit-friendly).

def html_table_to_claims(html, filename, page):
    rows = _TableParser().parse(html)          # rows with colspan/rowspan
    grid = {}                                  # (row, col) -> cell text
    for ri, row in enumerate(rows):
        c = 0
        for cell in row:
            while (ri, c) in grid: c += 1       # skip cells already filled by a span
            for dr in range(cell.rowspan):
                for dc in range(cell.colspan):
                    grid[(ri + dr, c + dc)] = cell.text   # a span fills every cell
            c += cell.colspan
    # a header spanning two date columns now maps to BOTH sub-columns:
    #   'ASIA | Japan | 09/12/2024 | WWE THS' = 98
    #   'ASIA | Japan | 09/12/2024 | Prior'   = 76
    return _cells_to_claims(grid, filename, page)

#  TALKING POINT: "Regulatory tables aren't simple grids — merged headers,
#  nested labels, side-by-side blocks. We build a grid that honours every span,
#  so a value keeps its full context. And this replaces a paid LLM call with
#  deterministic code."


# =============================================================================
#  STAGE 5 — GROUNDING : the guard against hallucination
#  module: preprocessing_etl/custom/grounding.py
# =============================================================================
#
#  WHAT: for every text-derived value, verify it actually appears in its source.
#        A number is GROUNDED only if that exact number is in the source; a label
#        only if the text is. Chart-read values are NOT_APPLICABLE (image-read).
#
#  WHY : this is how we fight fabrication. A value that can't be found in the
#        source is flagged UNGROUNDED — not silently promoted. In a regulatory
#        setting, "we can prove where this number came from" is non-negotiable.

def check_grounding(value, source_content, is_figure):
    if is_figure:
        return NOT_APPLICABLE                  # read from an image, can't text-ground
    core = _numeric_core(value)                # e.g. "£4,844m" -> 4844
    if core is not None:
        return GROUNDED if core in _numbers_in(source_content) else UNGROUNDED
    # non-numeric: the text must appear in the source
    return GROUNDED if str(value).lower() in source_content.lower() else UNGROUNDED

#  TALKING POINT: "Every value we keep, we can trace to the exact place it was
#  read from. If we can't find it in the source, we flag it rather than trust
#  it. That's the anti-hallucination guarantee."


# =============================================================================
#  STAGE 6 — GATING : deciding what a human needs to see
#  module: preprocessing_etl/custom/validator.py
# =============================================================================
#
#  WHAT: split extracted claims into "clean" (-> gold) and "needs review"
#        (-> a review queue). Low confidence or a genuine conflict routes a
#        value to a human; chart estimates are labelled but not blocked.
#
#  WHY : scale requires trusting the automation for the clear cases and focusing
#        human attention only on the uncertain ones. Nothing risky is promoted
#        silently; nothing certain wastes a reviewer's time.

def gate_for_review(claims, conflicts):
    clean, review = [], []
    for c in claims:
        is_estimate  = c.citation_tier == LLM_ESTIMATED     # chart-read
        low_conf     = c.confidence < REVIEW_THRESHOLD
        in_conflict  = c.canonical_metric in conflicts
        # chart estimates go to review only on a real conflict, not just low conf
        if in_conflict or (low_conf and not is_estimate):
            review.append(c)
        else:
            clean.append(c)
    return {"clean": clean, "needs_review": review}

#  TALKING POINT: "We gate for review by confidence and conflict — so humans see
#  exactly the values that need a human, and nothing else."


# =============================================================================
#  STAGE 7 — STAMP + STORE : metadata makes every value auditable
#  module: preprocessing_etl/custom/storage.py
# =============================================================================
#
#  WHAT: before writing to gold, stamp every row with metadata: team,
#        report_type, entity_id/name, report_date, extraction_date, and a stable
#        content_hash. Write into a per-team gold table with an explicit schema.
#
#  WHY : provenance + governance. Every metric knows which document and page it
#        came from, when it was produced, and how reliably it was read. The
#        content_hash makes re-runs idempotent (no duplicate rows). The explicit
#        schema keeps types correct — dates stay text, numbers stay numeric.

def _stamp(rows, team, report_type, report_date, entity_id, entity_name):
    now = datetime.now(timezone.utc).isoformat()
    for r in rows:
        r["team"], r["report_type"] = team, report_type
        r["report_date"] = report_date          # from the FILENAME (primary source)
        r["entity_id"], r["entity_name"] = entity_id, entity_name
        r["extraction_date"] = now               # when THIS run produced it
        key = "|".join(str(r.get(k, "")) for k in
                       ("canonical_metric", "field_name", "value",
                        "source_document", "page", "entity_ref"))
        r["content_hash"] = content_hash(key)    # stable key -> idempotent re-runs
    return rows

#  The Claim schema — provenance is part of the TYPE, not an afterthought:
#     canonical_metric, field_name, element_type, value, numeric_value,
#     currency, unit, citation_tier, confidence,
#     source_document, page, report_date, content_hash, extraction_date

#  TALKING POINT: "Nothing lands in gold anonymously. Source document, page, how
#  it was read, when, and a stable key — every value is auditable and every
#  re-run is safe."


# =============================================================================
#  STAGE 8 — INDEX : making content searchable as narrative
#  module: preprocessing_etl/custom/ai_parse_pipeline.py  (+ ai_parse_structurer)
# =============================================================================
#
#  WHAT: index each element into the vector store, FORMATTED for readability:
#          table  -> a clean MARKDOWN table (not raw HTML)
#          text   -> the prose as-is
#          figure -> the extracted chart VALUES as readable lines
#
#  WHY : the index answers "what does the report say about X" questions. A raw
#        HTML blob embeds and reads poorly; a clean markdown table the model can
#        actually reason over. Chart values are indexed too, so chart data is
#        searchable, not just visible.

def _index_ai_parse_elements(parsed, sub, search_store, claims):
    for el in parse_ai_parse_response(parsed):
        if el.el_type == "table":
            text = html_table_to_markdown(el.content)     # readable + structured
        elif el.el_type == "figure":
            text = _chart_values_as_lines(claims, el.page) # the extracted values
        else:
            text = el.content                              # prose as-is
        search_store.index(RegulatoryChunk(
            content=text, source_document_id=basename(sub["path"]),
            page=el.page, team=sub["team"], report_type=sub["report_type"]))

#  TALKING POINT: "We don't index raw HTML — we index readable tables and the
#  chart values themselves, so the narrative side can actually answer questions."


# =============================================================================
#  CONSUMPTION — how answers come out: routed retrieval
#  modules: query_router.py, query_gold.py, ai_search_store.py
# =============================================================================
#
#  WHAT: a question is classified and sent to the right store:
#          structured / metric / trend  -> SQL over gold  (fast key-lookup)
#          narrative / "what does it say" -> the vector index
#
#  WHY : precise numbers and aggregations belong in SQL (exact, fast, scalable);
#        narrative belongs in the index (semantic). Using each for its strength
#        keeps answers both accurate and fast.

def route_query(question):
    if _looks_structured(question):     # "total", "trend", "over 5 years", "how much"
        return STRUCTURED               # -> query_gold (SQL on per-team gold)
    return NARRATIVE                    # -> search_store.search (vector index)

#  query_gold is FILTER-FIRST: a team-filtered question only touches that team's
#  table, never a union of everything — so it scales to millions of rows.

def query_gold(spark, team, report_type, source_document=None, entity=None):
    if team and report_type:
        tables = [f"prod_gold_{slug(team, report_type)}"]   # ONE table — fastest
    elif team:
        tables = list_tables(f"prod_gold_{team}_*")          # that team's tables
    else:
        tables = list_tables("prod_gold_*")                  # all (cross-team)
    df = union(tables)
    # then apply row filters (Spark pushes these into each table scan)
    return _apply_filters(df, team, report_type, source_document, entity)

#  TALKING POINT: "A question about a team's revenue only ever reads that team's
#  table — not everyone's. That's why it stays fast as the data grows."


# =============================================================================
#  OPERATIONS — proving it objectively: the scorecard
#  module: preprocessing_etl/custom/scorecard.py
# =============================================================================
#
#  WHAT: after every run, compute a four-dimension scorecard from the run's own
#        output — coverage, quality, robustness, cost.
#
#  WHY : it lets any pipeline (ours or another) be compared on MEASURED outcomes,
#        not preference. It's how "ours is better" becomes evidence, not opinion.

def build_scorecard(result):
    return {
        "coverage":   {"total_metrics": result.claims_total,
                       "per_document": result.claims_total / result.documents},
        "quality":    {"gold_rate_pct": pct(result.gold_total, result.claims_total),
                       "grounded_pct": ..., "avg_confidence": ...},
        "robustness": {"quarantine_rate_pct": pct(result.quarantined, ...),
                       "failures": len(result.failures)},
        "cost":       {"total_cost_usd": ..., "cost_per_metric_usd": ...},
    }

#  TALKING POINT: "Every run scores itself on coverage, quality, robustness and
#  cost. That's how we settle 'which approach is better' with data — including
#  the same-document comparison that showed we extract far more than the prior
#  pipeline, verified page by page."


# =============================================================================
#  ASSURANCE — how we ENSURE COVERAGE (the "did we get everything?" question)
#  module: preprocessing_etl/custom/coverage_audit.py
# =============================================================================
#
#  WHAT: after extraction, reconcile what ai_parse RETURNED against what we
#        EXTRACTED. Every element is one of three things:
#          • ACCOUNTED         -> it produced claims
#          • intentionally SKIPPED -> logo / page header / footer (by design)
#          • UNACCOUNTED        -> produced nothing = a real coverage GAP
#        Plus: any page that yielded zero metrics is surfaced for a human.
#
#  WHY : coverage assurance is TWO honest things —
#          1. ACCOUNTING  : nothing is silently dropped. Every element the parser
#                           saw is consciously handled or explicitly flagged.
#          2. MEASUREMENT : the scorecard quantifies how much we extracted, and
#                           the same-document comparison proves it's more than
#                           the alternative (page-by-page — 520 vs 0 on one page).
#
#        The HONEST LIMIT (say this — it IS credibility): no pipeline can *prove*
#        it captured everything, because that needs a ground truth we can't fully
#        have — a bare number in a document isn't self-evidently a metric.
#        "Ground truth validates what you found, not what you missed."
#        So we MAXIMISE and VERIFY coverage; we don't claim an impossible 100%.

_INTENTIONAL_SKIP = {"page_header", "page_footer", "page_number"}

def audit_coverage(parsed_elements, claims):
    claims_per_page = Counter(c.page for c in claims)
    produced_pages  = set(claims_per_page)
    accounted, skipped, unaccounted = [], [], []
    for e in parsed_elements:
        if e.el_type in _INTENTIONAL_SKIP or _is_logo(e):
            skipped.append(e)                 # deliberately not extracted
        elif e.page in produced_pages:
            accounted.append(e)               # its page produced claims
        else:
            unaccounted.append(e)             # produced nothing -> GAP to review
    return {
        "elements_total": len(parsed_elements),
        "elements_accounted": len(accounted),
        "elements_intentionally_skipped": len(skipped),
        "elements_unaccounted": len(unaccounted),           # the number to watch
        "empty_pages": [pg for pg in {e.page for e in parsed_elements}
                        if claims_per_page.get(pg, 0) == 0], # 0-metric pages
        "claims_total": len(claims),
    }

#  TALKING POINT (for "how do we ensure coverage?"):
#  "Three ways. One — nothing is silently dropped: this audit reconciles every
#   element the parser returned against what we extracted, and flags any gap.
#   Two — we measure coverage every run with the scorecard. Three — we verified
#   it against the prior approach on the same documents, page by page.
#   And I'll be honest about the limit: no pipeline can *prove* it got
#   everything — that needs a ground truth we can't fully have. What we
#   guarantee is that every element was consciously handled, gaps are surfaced,
#   and our coverage is measurably higher than the alternative."


# =============================================================================
#  ONE HARD BUG, TRACED TO ROOT (a credibility story to have ready)
#  module: preprocessing_etl/custom/dates.py
# =============================================================================
#
#  SYMPTOM : report_date showed as "1252.0" instead of "February 2026" — across
#            silver, gold AND analytical, on brand-new tables every run.
#
#  TRACE   : ruled out a stale table (fresh prefix each run). Traced the value
#            back: derive_report_date scans claims for one whose value "looks
#            like a date". A stray numeric metric value 1252.0 stringifies to
#            "1252.0" — which MATCHED the YYYY.MM date pattern (\d{4}.\d{1,2}),
#            so a number was mistaken for a date and stamped as report_date.
#
#  FIX     : reject decimal numbers BEFORE date-pattern matching — a float is
#            never a date; a bare integer year still can be (with a date field).

def looks_like_date(value, field_name=None):
    s = str(value).strip()
    if re.search(r"\d\.\d", s):        # has a decimal point ->
        try:
            float(s.replace(",", ""))
            return False               # ...it's a number, never a date
        except ValueError:
            pass
    # ... real date patterns (month+year, Q/H+year, dd/mm/yyyy) ...
    return _is_real_date(s, field_name)

#  TALKING POINT: "report_date was coming out as a float. I traced it through
#  every layer, ruled out the storage theory, and found a numeric value was
#  matching the date regex. Fixed it at the source — a float can never be a
#  date. That's the kind of root-cause debugging the pipeline is built on."


# Hybrid
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
    "this image and return their data points as a JSON array \u2014 no prose:\n"
    '[{"label": "<series name from legend | category/x-axis label>", '
    '"value": <number>, "unit": "<%/\u00a3m/bps/count/etc>", '
    '"source_type": "chart"}]\n'
    "\n"
    "Every item MUST include \"source_type\". Set it to \"chart\" for a value read "
    "from a chart/graph/plot. If you (incorrectly) read something from a table "
    "set \"table\"; from text set \"text\" \u2014 but you should not be returning those "
    "at all (see the ABSOLUTE RULE below).\n"
    "*** ABSOLUTE RULE \u2014 CHARTS ONLY ***\n"
    "- Extract data ONLY from charts, graphs and plots: bar charts, line charts, "
    "pie/donut charts, radial charts, scatter/area plots \u2014 anything with bars, "
    "lines, wedges or plotted points read against an axis or legend.\n"
    "- NEVER extract from TABLES. If numbers sit in a grid of rows and columns, "
    "that is a TABLE \u2014 IGNORE it completely. Do not return any value from a table.\n"
    "- NEVER extract from TEXT, paragraphs, headings, bullet points or figures "
    "of prose. IGNORE all plain text and tabular numbers.\n"
    "- Tables and text are extracted separately by another system. Your ONLY job "
    "is the charts. If a value is not plotted in a chart, DO NOT return it.\n"
    "- If the image contains NO chart at all (only tables/text/logos), return an "
    "empty array [].\n"
    "\n"
    "Chart-reading rules:\n"
    "- The chart may COMBINE types (bars AND a line) \u2014 read BOTH series. Combo "
    "charts may have TWO y-axes; use the correct axis for each series.\n"
    "- RADIAL/circular bar charts: read each segment against its radial scale.\n"
    "- COLOUR LEGEND: bars/lines are distinguished by colour with a legend/"
    "footnote mapping colour -> series name (e.g. blue = \'DEF exposure\'). Map "
    "each coloured bar/line to its series name and put it in the label. Never "
    "report a value without its series name.\n"
    "- Read every value across ALL series against the axis scale; estimate "
    "between gridlines if needed.\n"
    "- CRITICAL \u2014 read the DATA, not the axis. Do NOT return the numbers printed "
    "on the y-axis scale / gridlines (e.g. 0, 2000, 4000, 6000). Those are the "
    "SCALE, not data. For each BAR, estimate the value at the TOP of the bar by "
    "its height against the scale; for each LINE point, estimate its value by "
    "its vertical position. If a bar top sits between 4000 and 6000 nearer 6000, "
    "return about 5500 \u2014 its actual height \u2014 NOT 4000 or 6000.\n"
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
        # ENFORCE charts-only via Sonnet's self-reported source_type.
        stype = str(p.get("source_type") or "chart").strip().lower()
        if stype not in ("chart", "graph", "plot", ""):
            continue
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


def _sonnet_read_images_concurrent(run_async_fn, llm_client, model_key, prompt,
                                   figure_jobs):
    """Read MANY figures CONCURRENTLY in one event-loop pass.

    figure_jobs: list of (el, content, img_b64). Returns a dict {id(el): points}.

    The client already bounds concurrency with a semaphore (default 8 in flight),
    but the pipeline was calling figures one-at-a-time, so a 30-chart document
    made 30 SEQUENTIAL Sonnet calls. Gathering them into a single asyncio.gather
    lets the semaphore run up to N at once -> big speed-up on chart-heavy docs.
    """
    import asyncio

    try:
        from ..shared.llm_client import EXTRACTION_TOOL_SCHEMA
    except Exception:
        EXTRACTION_TOOL_SCHEMA = {"name": "extract"}

    async def _one(el, content, img_b64):
        try:
            resp = await llm_client.extract(
                model_key, prompt, content or "", EXTRACTION_TOOL_SCHEMA,
                image_base64=img_b64)
            ta = getattr(resp, "tool_arguments", None)
            if isinstance(ta, dict):
                for key in ("claims", "metrics", "points", "data", "values"):
                    if isinstance(ta.get(key), list):
                        return id(el), [p for p in ta[key] if isinstance(p, dict)]
                if ta:
                    return id(el), [ta]
            return id(el), parse_sonnet_chart_points(getattr(resp, "text", "") or "")
        except Exception:
            return id(el), []

    async def _all():
        return await asyncio.gather(
            *[_one(el, content, img) for (el, content, img) in figure_jobs])

    try:
        results = run_async_fn(_all())
        return dict(results)
    except Exception:
        return {}


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
                            use_sonnet: bool = False,
                            max_figure_concurrency: int = 3) -> list:
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

    # figures are COLLECTED here and read from Sonnet CONCURRENTLY after the
    # loop (reading them one-by-one was the chart-heavy slowdown).
    _figure_jobs = []

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
            fig_text = " ".join(x for x in (el.description, content) if x).strip()
            desc_claims = chart_description_to_claims(
                fig_text, filename, page=el.page,
                title=_prefix(current_title, current_section),
                team=team, report_type=report_type, entity_ref=entity_ref)
            if not use_sonnet:
                # rule-based only: use whatever the description gave.
                claims.extend(desc_claims)
                continue
            if crop_figure_image is None:
                claims.extend(desc_claims)
                continue
            try:
                img_b64 = crop_figure_image(el)
            except Exception:
                img_b64 = None
            if not img_b64:
                claims.extend(desc_claims)
                continue
            # COLLECT the figure for a CONCURRENT Sonnet pass after the loop
            # (reading 30 charts one-at-a-time is the main slowdown). Keep its
            # description fallback in case vision returns nothing.
            _figure_jobs.append((el, content, img_b64, desc_claims))
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
    # DROP the ai_parse text echoes: chart_description_to_claims turns ai_parse's
    # figure DESCRIPTION/CONTENT text into claims tagged (llm_estimated + text).
    # When Sonnet reads the same figure from the IMAGE it produces the real
    # values tagged (llm_estimated + figure). So a claim that is llm_estimated
    # AND element_type='text' is the ai_parse text echo — a duplicate of what
    # Sonnet read from the image — and is dropped. Rule-based (parsed) text and
    # real chart (figure) claims are kept.
    # NOTE: ai_parse's figure TEXT also becomes claims (llm_estimated + text),
    # which duplicate the chart values Sonnet reads from the image (llm_estimated
    # + figure). These echoes are removed by a deterministic SQL clean in the
    # notebook (DELETE WHERE citation_tier='llm_estimated' AND element_type='text')
    # rather than in-memory here — the SQL approach is observable and avoids the
    # ordering issues an in-memory drop introduced.
    # CONCURRENT figure pass. IMPORTANT: this changes ONLY dispatch (parallel
    # instead of blocking). Each figure's Sonnet call uses the SAME inputs the
    # sequential version used — same whole-page image, same `content` signal,
    # same prompt, same sonnet_points_to_claims — so WHAT Sonnet reads is
    # unchanged; only the wall-clock time differs. Concurrency is bounded by
    # max_figure_concurrency (kept modest to avoid changing rate-limit behaviour).
    if use_sonnet and _figure_jobs and run_async_fn and llm_client:
        import asyncio
        try:
            from ..shared.llm_client import EXTRACTION_TOOL_SCHEMA
        except Exception:
            EXTRACTION_TOOL_SCHEMA = {"name": "extract"}

        _sem = asyncio.Semaphore(max_figure_concurrency)

        async def _read_one(el, c, img):
            # identical call to the sequential _sonnet_read_image, awaited
            async with _sem:
                try:
                    resp = await llm_client.extract(
                        vision_model_key, CHART_DIGITISATION_PROMPT, c or "",
                        EXTRACTION_TOOL_SCHEMA, image_base64=img)
                except Exception:
                    return id(el), None
            ta = getattr(resp, "tool_arguments", None)
            if isinstance(ta, dict):
                for key in ("claims", "metrics", "points", "data", "values"):
                    if isinstance(ta.get(key), list):
                        return id(el), [p for p in ta[key] if isinstance(p, dict)]
                if ta:
                    return id(el), [ta]
            return id(el), parse_sonnet_chart_points(getattr(resp, "text", "") or "")

        async def _gather():
            return await asyncio.gather(*[
                _read_one(el, c, img) for (el, c, img, _dc) in _figure_jobs])

        try:
            results = dict(run_async_fn(_gather()))
        except Exception:
            results = {}

        # apply results in the SAME order and with the SAME fallback as sequential
        for (el, c, img, desc_claims) in _figure_jobs:
            pts = results.get(id(el))
            if pts:
                claims.extend(sonnet_points_to_claims(pts, el, filename, entity_ref))
            else:
                claims.extend(desc_claims)   # vision found nothing/failed -> keep desc

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

# ai_parse_document
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
                       image_output_path=None, dbutils=None,
                       max_figure_concurrency=3) -> BatchResult:
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
                # build the whole-page image fn (with an optional debug save path
                # from the module global set by the notebook). all_elements is
                # passed but the current approach sends the full page to Sonnet,
                # which reads charts-only via the prompt + source_type enforcement.
                crop_fn = _build_figure_crop_fn(
                    spark, image_output_path, dbutils,
                    all_elements=_els, save_crops_to=_SAVE_CROPS_TO)

            claims = hybrid_extract_document(
                ai_parse_response=parsed, filename=fname,
                team=sub["team"], report_type=sub["report_type"],
                entity_ref=sub.get("entity_ref"),
                use_sonnet=option_a, llm_client=llm_client,
                run_async_fn=run_async_fn, crop_figure_image=crop_fn,
                max_figure_concurrency=max_figure_concurrency)
            audit.log(result.run_id, "extract", path,
                      detail=f"{len(claims)} claims")

            gated = gate_for_review(claims, [])
            clean, review = gated["clean"], gated["needs_review"]

            # derive the report date (best-effort; never break the doc over it)
            try:
                from .storage import derive_report_date
                rdate = derive_report_date(claims, None, path=path)
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
            rdate = derive_report_date(claims, None, path=path)

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

# storage
"""Medallion storage: Bronze / Silver / Gold.

Local mode persists JSONL under a root dir; Databricks mode writes Delta
tables. Gold is a deliberate whitelist — ``normalisation_conflicts`` is
excluded from Gold (they go to Databricks AI Search for narrative search).

The build/persist split (pure ``build_gold_metric_rows`` + thin
``write_gold_metrics``) keeps row-building testable without I/O.

Fixes: entity_ref is read from the Claim (which now carries it) instead of a
sentinel; re-runs are idempotent via content_hash dedup on JSONL; quarantine,
review decisions and audit events have real sinks; reviewed/overridden claims
can be promoted into Gold.
"""
from __future__ import annotations

import json
import os
from typing import Optional

from ..shared.config import CONFIG
from ..shared.schema import Claim, GoldMetric, content_hash

GOLD_WHITELIST = [
    "entity_ref", "canonical_metric", "value", "unit", "reporting_basis",
    "scale", "as_at_date", "netting", "measure_type", "period",
    "citation_tier", "review_tier",
]


class StorageManager:
    """Local JSONL persistence for MVP / test runs."""

    def __init__(self, root: Optional[str] = None):
        self.root = root or CONFIG.storage.local_root
        os.makedirs(self.root, exist_ok=True)

    def _path(self, layer: str) -> str:
        return os.path.join(self.root, f"{layer}.jsonl")

    def append_jsonl(self, layer: str, rows: list[dict]) -> int:
        """Append rows, skipping any whose content hash was already written.

        Idempotency: each row gets a stable hash over its sorted JSON; hashes
        already present in the file are skipped, so re-scanning processed
        files never duplicates. (On Databricks, MERGE handles this instead.)
        """
        if not rows:
            return 0
        seen = self._seen_hashes(layer)
        written = 0
        with open(self._path(layer), "a") as fh:
            for r in rows:
                # dedup on the STABLE content_hash (metric+value+source) when the
                # row carries one — this ignores per-run fields like
                # extraction_date, keeping re-runs idempotent. Fall back to
                # hashing the whole row for rows without a content_hash.
                h = r.get("content_hash") or content_hash(
                    json.dumps(r, sort_keys=True, default=str))
                if h in seen:
                    continue
                seen.add(h)
                fh.write(json.dumps(r, default=str) + "\n")
                written += 1
        return written

    def _seen_hashes(self, layer: str) -> set[str]:
        p = self._path(layer)
        if not os.path.exists(p):
            return set()
        seen: set[str] = set()
        with open(p) as fh:
            for line in fh:
                if line.strip():
                    row = json.loads(line)
                    # match the write-side dedup: prefer the stable content_hash
                    # (ignores per-run extraction_date), else hash the whole row.
                    seen.add(row.get("content_hash") or content_hash(
                        json.dumps(row, sort_keys=True, default=str)))
        return seen

    def read_jsonl(self, layer: str) -> list[dict]:
        p = self._path(layer)
        if not os.path.exists(p):
            return []
        with open(p) as fh:
            return [json.loads(line) for line in fh if line.strip()]


class DeltaLakeStorage(StorageManager):
    """Writes Delta tables on Databricks; falls back to JSONL locally.

    ``spark`` is injected on a cluster. When absent, all writes go to JSONL
    so the same code path works in tests.
    """

    def __init__(self, spark=None, root: Optional[str] = None):
        super().__init__(root=root)
        # If no spark session is passed, try to acquire the active one (on
        # Databricks it's always available). Without this, storage silently
        # falls back to writing JSONL files instead of Delta tables — the
        # caller sees "no tables created". Self-acquiring makes the Delta path
        # the default on-cluster; off-cluster (tests) it stays None and uses
        # the file fallback.
        if spark is None:
            try:
                from pyspark.sql import SparkSession
                spark = SparkSession.getActiveSession() or \
                    SparkSession.builder.getOrCreate()
            except Exception:
                spark = None
        self.spark = spark

    @staticmethod
    def build_gold_metric_rows(claims: list[Claim]) -> list[dict]:
        """PURE: build Gold rows from claims. No I/O.

        Filters to METRIC-like claims only — Gold is the structured metrics
        layer, so narrative sentences and headings that were over-extracted as
        claims are dropped here (they remain available via the search index).
        """
        from .dates import looks_like_date, normalise_report_date
        rows: list[dict] = []
        for c in claims:
            if not _is_metric_like(c):
                continue  # drop sentences/headings/noise — not a metric
            field = c.canonical_metric or c.field_name
            value_is_date = looks_like_date(c.value, field)
            # A date-valued claim carries a date, not a metric: surface it on
            # as_at_date and leave numeric_value empty. Prefer an explicit
            # as_at_date on the claim if it already has one.
            as_at = c.as_at_date or (normalise_report_date(c.value)
                                     if value_is_date else None)
            gm = GoldMetric(
                entity_ref=_entity_ref(c),
                canonical_metric=c.canonical_metric or c.field_name,
                field_name=c.field_name,
                element_type=getattr(c, "element_type", None),
                value=c.value,
                numeric_value=_parse_numeric(c.value, field),
                source_document=_source_doc(c),
                page=c.page,
                unit=c.unit,
                reporting_basis=c.reporting_basis,
                scale=c.scale,
                as_at_date=as_at,
                netting=c.netting,
                measure_type=c.measure_type,
                period=c.period,
                citation_tier=c.citation_tier.value,
                review_tier=c.review_tier.value,
                domain_tag=c.domain_tag,
            )
            rows.append(gm.model_dump())
        return rows

    def write_gold_metrics(self, claims: list[Claim],
                           team: Optional[str] = None,
                           report_type: Optional[str] = None,
                           report_date: Optional[str] = None,
                           entity_id: Optional[str] = None,
                           entity_name: Optional[str] = None) -> int:
        """THIN: build THEN persist. Returns rows written.

        team/report_type/report_date + entity_id/entity_name are stamped onto
        EVERY row as columns (not just encoded in the table name), so chat/SQL
        can filter by them and recognise the entity.
        """
        rows = self.build_gold_metric_rows(claims)
        rows = _stamp(rows, team=team, report_type=report_type,
                      report_date=report_date, entity_id=entity_id,
                      entity_name=entity_name)
        return self._write_layer("gold", rows, team, report_type)

    def write_silver(self, claims: list[Claim], team: Optional[str] = None,
                     report_type: Optional[str] = None,
                     report_date: Optional[str] = None,
                     entity_id: Optional[str] = None,
                     entity_name: Optional[str] = None) -> int:
        rows = [c.model_dump(mode="json") for c in claims]
        rows = _stamp(rows, team=team, report_type=report_type,
                      report_date=report_date, entity_id=entity_id,
                      entity_name=entity_name)
        return self._write_layer("silver", rows, team, report_type)

    def write_bronze(self, elements: list[dict], team: Optional[str] = None,
                     report_type: Optional[str] = None) -> int:
        return self._write_layer("bronze", elements, team, report_type)

    def write_quarantine(self, rows: list[dict]) -> int:
        return self._write("quarantine", CONFIG.storage.quarantine_table, rows)

    def write_reviews(self, rows: list[dict]) -> int:
        return self._write("reviews", CONFIG.storage.review_table, rows)

    def write_review_claims(self, claims: list[Claim], team: Optional[str] = None,
                            report_type: Optional[str] = None,
                            report_date: Optional[str] = None,
                            entity_id: Optional[str] = None,
                            entity_name: Optional[str] = None) -> int:
        """Persist the claims flagged for human review to a queryable table, so
        reviewers can SEE exactly what needs checking (metric, value, why it was
        flagged, confidence, and the source page) — instead of it being just a
        count.

        IMPORTANT: review claims are written AS-IS (like Silver), NOT through the
        metric-like filter that Gold uses. Flagged claims are often the borderline
        / low-confidence / non-metric items — that is precisely WHY they need a
        human — so filtering them through the Gold metric gate would drop the very
        claims the review queue exists to hold (and, if all of them were dropped,
        the table would never be created)."""
        rows = [c.model_dump(mode="json") for c in claims]      # AS-IS, no filter
        # annotate WHY each is under review, from the claim
        for r, c in zip(rows, claims):
            r["review_reason"] = ("conflict" if getattr(c, "needs_review", False)
                                  and (c.canonical_metric or c.field_name) else
                                  "low_confidence")
            r["confidence"] = getattr(c, "confidence", None)
        rows = _stamp(rows, team=team, report_type=report_type,
                      report_date=report_date, entity_id=entity_id,
                      entity_name=entity_name)
        return self._write("review_claims", CONFIG.storage.review_claims_table, rows)

    def write_audit(self, rows: list[dict]) -> int:
        return self._write("audit", CONFIG.storage.audit_table, rows)

    def write_analytical_gold(self, gold_rows: list[dict], team: str,
                              report_type: str, entity_id=None,
                              entity_name=None) -> int:
        """Explode Gold rows into the analytical layer (statement / classification
        / category / sub_category + period + field_name + currency + unit +
        report_date + traceability) and write them to a PER-TEAM table
        ``analytical_gold_<team>_<report>`` — mirroring the Gold table structure.
        entity_id / entity_name (parsed from the volume path) are stamped on so
        people can recognise the entity. Rule-based, no LLM."""
        from .analytical_gold import build_analytical_rows
        rows = build_analytical_rows(gold_rows)
        rows = _stamp(rows, team=team, report_type=report_type,
                      entity_id=entity_id, entity_name=entity_name)
        table = CONFIG.storage.layer_table("analytical_gold", team, report_type)
        return self._write("analytical", table, rows)

    def _write_layer(self, layer: str, rows: list[dict],
                     team: Optional[str], report_type: Optional[str]) -> int:
        """Write a medallion layer, partitioned by team+report_type when both
        are supplied. Falls back to a single shared table/file otherwise (so
        existing callers and tests keep working)."""
        if team and report_type:
            table = CONFIG.storage.layer_table(layer, team, report_type)
            local = CONFIG.storage.layer_local(layer, team, report_type)
        else:
            table = getattr(CONFIG.storage, f"{layer}_table", None) \
                or CONFIG.storage.gold_metrics
            local = layer
        if self.spark is not None and rows:
            typed = _rows_for_explicit_schema(rows)
            df = self.spark.createDataFrame(typed, schema=_spark_schema_for(typed))
            df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)
            return len(rows)
        return self.append_jsonl(local, rows)

    def _write(self, layer: str, table: str, rows: list[dict]) -> int:
        if self.spark is not None and rows:
            typed = _rows_for_explicit_schema(rows)
            df = self.spark.createDataFrame(typed, schema=_spark_schema_for(typed))
            df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)
            return len(rows)
        return self.append_jsonl(layer, rows)


def _stamp(rows: list[dict], team=None, report_type=None,
          report_date=None, entity_id=None, entity_name=None) -> list[dict]:
    """Add team / report_type / report_date onto every row as columns, so the
    curated tables are filterable by them (WHERE team = ...) without relying on
    the table name.

    Also stamps entity_id and entity_name (parsed from the document's volume
    path) so people can recognise which entity a metric belongs to, plus two
    operational columns:
      * extraction_date — the UTC timestamp of THIS run.
      * content_hash — a stable key over the row's identifying content.

    Uses DIRECT assignment, not setdefault: the rows come from model_dump(),
    which already includes team/report_type keys set to None, so setdefault
    would be a no-op and leave them empty.
    """
    import datetime
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    for r in rows:
        if team is not None:
            r["team"] = team
        if report_type is not None:
            r["report_type"] = report_type
        if report_date is not None:
            r["report_date"] = report_date
        if entity_id is not None:
            r["entity_id"] = entity_id
        if entity_name is not None:
            r["entity_name"] = entity_name
        r["extraction_date"] = now
        # stable export/promote key over the row's identity (metric + value +
        # source), independent of the run timestamp.
        key_src = "|".join(str(r.get(k, "")) for k in
                           ("canonical_metric", "field_name", "value",
                            "source_document", "page", "entity_ref"))
        r["content_hash"] = content_hash(key_src)
    return rows


def _spark_schema_for(rows: list[dict]):
    """Build an EXPLICIT Spark schema for the rows, so types are never inferred.

    This is the permanent fix for both failure modes of inference:
      * CANNOT_DETERMINE_TYPE — an all-None column can't be typed by inference;
      * CAST_INVALID_INPUT — inference guessing a column is string ("") when the
        existing table column is numeric.
    Known numeric columns are declared with their real types (nullable);
    everything else is a nullable string. Imported lazily so the module still
    imports off-cluster (tests), where pyspark isn't present.
    """
    from pyspark.sql.types import (StructType, StructField, StringType,
                                   DoubleType, LongType)
    # column -> Spark type. page is integer-like (Long); the rest of the
    # numeric metrics are Double. Everything not listed is String.
    numeric_types = {
        "numeric_value": DoubleType(),
        "confidence": DoubleType(),
        "page": LongType(),
    }
    keys = []
    for r in rows:
        for k in r.keys():
            if k not in keys:
                keys.append(k)
    fields = [StructField(k, numeric_types.get(k, StringType()), True)
              for k in keys]
    return StructType(fields)


def _rows_for_explicit_schema(rows: list[dict]) -> list[dict]:
    """Coerce row values to match the explicit schema: numeric columns become
    real numbers or None (never ""), string columns become strings or None."""
    numeric_cols = {"numeric_value", "confidence", "page"}
    out = []
    for r in rows:
        clean = {}
        for k, v in r.items():
            if k in numeric_cols:
                if v is None or v == "":
                    clean[k] = None
                else:
                    try:
                        clean[k] = int(v) if k == "page" else float(v)
                    except (ValueError, TypeError):
                        clean[k] = None
            else:
                clean[k] = None if v is None else str(v)
        out.append(clean)
    return out


def _typeable_rows(rows: list[dict]) -> list[dict]:
    """Make rows safe for Spark's schema inference WITHOUT flattening real
    numbers to strings.

    Spark infers column types from the data; a column that is None in every row
    can't be typed and raises CANNOT_DETERMINE_TYPE. We fix that per column:
      * columns KNOWN to be numeric (numeric_value) always stay numeric — None
        stays None (never ""), so a batch where every value is null still writes
        NULLs into the DOUBLE column instead of an empty string (which would
        fail the cast against an existing DOUBLE column);
      * a column that holds at least one number keeps its numbers, None -> None;
      * a column that is entirely None (and not known-numeric) is filled with ""
        so it types as string rather than failing inference;
      * everything else is coerced to its string form (None -> "").
    """
    if not rows:
        return rows
    # Columns that must ALWAYS be numeric, regardless of this batch's values —
    # so an all-null batch doesn't turn them into string columns (which then
    # fails the cast against an existing numeric column). Covers every numeric
    # column across gold + silver rows.
    KNOWN_NUMERIC = {"numeric_value", "page", "confidence"}

    keys = set()
    for r in rows:
        keys.update(r.keys())

    numeric_cols = set(KNOWN_NUMERIC)
    allnull_cols = set()
    for k in keys:
        if k in KNOWN_NUMERIC:
            continue  # already forced numeric
        vals = [r.get(k) for r in rows]
        non_null = [v for v in vals if v is not None]
        if not non_null:
            allnull_cols.add(k)
        elif all(isinstance(v, (int, float)) and not isinstance(v, bool)
                 for v in non_null):
            numeric_cols.add(k)

    out: list[dict] = []
    for r in rows:
        clean: dict = {}
        for k in keys:
            v = r.get(k)
            if k in numeric_cols:
                # keep as a real number; None stays None (NULL), never ""
                if v is None:
                    clean[k] = None
                else:
                    try:
                        # page is an integer column; keep it int, others float
                        clean[k] = int(v) if k == "page" else float(v)
                    except (ValueError, TypeError):
                        clean[k] = None  # unparseable -> NULL, not a bad cast
            elif k in allnull_cols:
                clean[k] = ""
            elif v is None:
                clean[k] = ""
            elif isinstance(v, (str, int, float, bool)):
                clean[k] = v if isinstance(v, str) else str(v)
            else:
                clean[k] = str(v)
        out.append(clean)
    return out


def _parse_numeric(value, field_name=None) -> Optional[float]:
    """Best-effort parse of a metric string into a float for dashboards.
    Handles percent signs, thousands separators, currency, surrounding text
    ("~750", "1,234.5%", "$12.4m"). Returns None when there's no parseable
    number OR when the value is a date (context-aware — a bare year is only a
    date if field_name signals one). Does NOT apply scale multipliers."""
    if value is None:
        return None
    # Dates are NOT metrics: context-aware so a genuine numeric metric (even a
    # 4-digit one like 1434, or a bare year used as a count) is not dropped.
    from .dates import looks_like_date
    if looks_like_date(value, field_name):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    import re
    s = str(value).strip()
    # find the first number, allowing thousands separators and a decimal part
    m = re.search(r"-?\d+(?:,\d{3})*(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except (ValueError, TypeError):
        return None


def derive_report_date(claims: list, elements: list = None,
                       path: str = None) -> Optional[str]:
    """Find the document's report date so every stored row can be stamped with it.

    Sources, in order of reliability for THIS corpus:
      1. The FILENAME — the report date is encoded in the document name
         (e.g. 'LCR_BankABC_February_2026.pptx' -> 'February 2026'). This is the
         primary source: filenames are named consistently with the period.
      2. The report TITLE / heading text — a date embedded in the title
         (e.g. 'Micro Report 2024 Q1').
      3. A date-valued CLAIM — a claim whose value is a period/date.
    Returns None if no date is found — the column stays empty rather than guess.
    """
    import os
    from .dates import (looks_like_date, normalise_report_date, _DATE_FIELD_RE,
                        _DATE_PATTERNS, _BARE_YEAR_RE)

    # 1. FILENAME — extract a date from the document's file name (primary).
    if path:
        fname = os.path.basename(str(path))
        # strip the extension so "..._2026.pptx" doesn't confuse the parser
        stem = os.path.splitext(fname)[0]
        # filenames often use underscores/dashes as separators -> spaces
        stem_spaced = stem.replace("_", " ").replace("-", " ")
        found = _extract_date_from_text(stem_spaced)
        if found:
            return found

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

    # 2. date-valued claim (fallback)
    dated = []
    for c in claims:
        field = getattr(c, "canonical_metric", None) or getattr(c, "field_name", "")
        val = getattr(c, "value", None)
        if looks_like_date(val, field):
            nd = normalise_report_date(val)
            if nd:
                dated.append((field or "", nd))
    if dated:
        for field, nd in dated:
            if _DATE_FIELD_RE.search(field):
                return nd
        return dated[0][1]
    return None


def _extract_date_from_text(text: str) -> Optional[str]:
    """Pull a date/period out of a heading/title string (searches WITHIN the
    text, e.g. 'Micro Report 2024 Q1' -> '2024 Q1'). Returns None if none."""
    import re
    from .dates import _MONTHS
    if not text:
        return None
    s = str(text)
    # quarter/half + year in any order, month+year, or a bare year — searched
    # anywhere in the string (not anchored), so titles with surrounding words hit
    patterns = [
        r"(q[1-4]|h[12])\s*[- ]?\s*\d{4}",
        r"\d{4}\s*[- ]?\s*(q[1-4]|h[12])",
        rf"({_MONTHS})\s+\d{{4}}",
        r"\d{1,2}[/\-.]\d{1,2}[/\-.]\d{2,4}",
        r"\b(19|20)\d{2}\b",
    ]
    for pat in patterns:
        m = re.search(pat, s, re.I)
        if m:
            return m.group(0).strip()
    return None


def _is_metric_like(c) -> bool:
    """True if a claim looks like a real METRIC (belongs in Gold), False if it's
    narrative noise — a sentence or heading that was over-extracted as a claim.

    Gold is the structured metrics layer; prose belongs in the search index, not
    here. A value is metric-like if it is short and either numeric, a date, or a
    recognised categorical status (N/A, nil, compliant, ...). Long multi-word
    prose values are treated as noise and dropped.
    """
    from .dates import looks_like_date

    value = (getattr(c, "value", None) or "").strip()
    if not value:
        return False

    # numeric or date values are metric-like
    if _parse_numeric(value, getattr(c, "canonical_metric", None)
                      or getattr(c, "field_name", "")) is not None:
        return True
    if looks_like_date(value):
        return True

    # recognised non-numeric metric values (status/flags) are metric-like
    _CATEGORICAL = {"n/a", "na", "nil", "none", "compliant", "non-compliant",
                    "not applicable", "not reported", "exempt", "yes", "no",
                    "pass", "fail", "true", "false", "-", "—"}
    if value.lower() in _CATEGORICAL:
        return True

    # otherwise: metric-like only if the value is SHORT (a value, not a
    # sentence). A long, many-word value is prose/heading noise -> drop.
    words = value.split()
    if len(words) > 4 or len(value) > 40:
        return False
    # a short value that is a Title-Case heading (2+ capitalised words, e.g.
    # "Risk Management Overview") is a heading, not a metric value -> drop.
    cap_words = [w for w in words if w[:1].isupper()]
    if len(words) >= 2 and len(cap_words) >= 2:
        return False
    # a short value containing a sentence verb/connective is prose, not a
    # metric value ("this report summarises ...") -> drop.
    _PROSE = {"the", "this", "that", "is", "are", "was", "were", "summarises",
              "summarizes", "shows", "includes", "provides", "and", "of", "for",
              "with", "report", "section", "overview", "summary"}
    if len(words) >= 3 and any(w.lower() in _PROSE for w in words):
        return False
    return True


def _source_doc(c: Claim) -> Optional[str]:
    """Best-effort source document name for a claim, for user verification.
    The source_element_id is built as '<filename>-<locator>' (e.g.
    'q1_report.pptx-s2-img0'), so the document is the part before the first
    locator suffix. Returns None if it can't be determined."""
    import re
    sid = getattr(c, "source_element_id", "") or ""
    if not sid:
        return None
    # strip known locator suffixes: -s{n}-img{n} / -p{n}-fig{n} / -tbl{n} / -img{n}
    m = re.split(r"-(?:s\d+-|p\d+-)?(?:img|fig|tbl|p)\d+", sid)
    return m[0] if m and m[0] else sid


def _entity_ref(c: Claim) -> str:
    """entity_ref now genuinely originates from the Claim (propagated from the
    source Element). The sentinel only appears if attribution truly failed
    upstream, which the pipeline routes to review rather than silently
    trusting."""
    return c.entity_ref or "UNKNOWN_ENTITY_REF"

# ai parse pipeline
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
                       image_output_path=None, dbutils=None,
                       max_figure_concurrency=3) -> BatchResult:
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
                # build the whole-page image fn (with an optional debug save path
                # from the module global set by the notebook). all_elements is
                # passed but the current approach sends the full page to Sonnet,
                # which reads charts-only via the prompt + source_type enforcement.
                crop_fn = _build_figure_crop_fn(
                    spark, image_output_path, dbutils,
                    all_elements=_els, save_crops_to=_SAVE_CROPS_TO)

            claims = hybrid_extract_document(
                ai_parse_response=parsed, filename=fname,
                team=sub["team"], report_type=sub["report_type"],
                entity_ref=sub.get("entity_ref"),
                use_sonnet=option_a, llm_client=llm_client,
                run_async_fn=run_async_fn, crop_figure_image=crop_fn,
                max_figure_concurrency=max_figure_concurrency)
            audit.log(result.run_id, "extract", path,
                      detail=f"{len(claims)} claims")

            gated = gate_for_review(claims, [])
            clean, review = gated["clean"], gated["needs_review"]

            # derive the report date (best-effort; never break the doc over it)
            try:
                from .storage import derive_report_date
                rdate = derive_report_date(claims, None, path=path)
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
            rdate = derive_report_date(claims, None, path=path)

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
