"""FIRM-SPECIFIC x-axis period resolution for charts with poor x-axis labelling.

Scoped to named firms only (FIRM_XAXIS). For a firm NOT listed, this does
nothing and the normal (working) chart path is used — so global success is
unaffected.

The problem it fixes (one company's charts): values are read fine but the
PERIOD each value belongs to is wrong, because the x-axis labels are sparse /
have a varying start month / miss the first or last label. Approach:
  * group chart elements into OBSERVATION SLOTS by x-position (one slot = one
    period on the axis),
  * align printed labels to slots, rejecting labels that don't cleanly fit,
  * anchor the timeline to a label that carries a YEAR, then reconstruct one
    period per slot by the monthly step — filling missing first/last/middle
    labels and rolling the year over correctly,
  * VALIDATE the reconstruction against every printed label; if any printed
    label disagrees, return valid=False so the caller can flag for review.

resolve_axis_labels() is the standalone, tested resolver. apply_to_claims()
wires it into the pipeline: it maps each Sonnet chart claim to its resolved
period (via the element positions), for the configured firm only.
"""
from __future__ import annotations
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta
from statistics import median


# ---------------------------------------------------------------------------
# Firms to apply x-axis resolution to, keyed by entity_id or entity_name
# (case-insensitive, substring-tolerant). Each value is config for that firm.
# Empty -> no firm affected (global behaviour unchanged).
# ---------------------------------------------------------------------------
FIRM_XAXIS: dict = {
    # "IkeBank": {"monthly_step": 1, "x_tolerance": 8},
}


_MONTHS = {
    "january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3,
    "april": 4, "apr": 4, "may": 5, "june": 6, "jun": 6, "july": 7, "jul": 7,
    "august": 8, "aug": 8, "september": 9, "sept": 9, "sep": 9, "october": 10,
    "oct": 10, "november": 11, "nov": 11, "december": 12, "dec": 12,
}


def _firm_config(firm_id=None, firm_name=None):
    """Return the x-axis config for this firm, or None if not configured."""
    if not FIRM_XAXIS:
        return None
    cands = [str(x).strip().lower().replace(" ", "") for x in (firm_id, firm_name) if x]
    for key, cfg in FIRM_XAXIS.items():
        k = str(key).strip().lower().replace(" ", "")
        if k and any(k in c or c in k for c in cands):
            return cfg or {}
    return None


def resolve_axis_labels(detected_bars, detected_labels, monthly_step=1,
                        x_tolerance=8):
    """Resolve each chart element's period. Returns (id_to_period, valid).
    Returns (id_to_period, status):
      status "geometry" -> periods resolved by geometry, use them
      status "normal"   -> not a date/numeric sparse-axis case (text/pie/
                           categorical) -> caller should use the NORMAL Sonnet
                           chart extraction, NOT quarantine
      status "flag"     -> a date/numeric case that failed validation -> review
    """
    if not detected_bars:
        return {}, "normal"

    elements = sorted(detected_bars, key=lambda b: float(b.get('x_center', 0)))

    # group elements into observation SLOTS (one slot = one period on the axis)
    slots = []
    for element in elements:
        x = float(element.get("x_center", 0))
        if not slots:
            slots.append({"x_center": x, "elements": [element]})
            continue
        last = slots[-1]
        if abs(x - last["x_center"]) <= x_tolerance:
            last["elements"].append(element)
            last["x_center"] = sum(float(e.get("x_center", 0)) for e in last["elements"]) / len(last["elements"])
        else:
            slots.append({"x_center": x, "elements": [element]})
    for i, slot in enumerate(slots):
        slot["slot_index"] = i

    typical_gap = None
    if len(slots) >= 2:
        gaps = [slots[i+1]["x_center"] - slots[i]["x_center"] for i in range(len(slots)-1)]
        pos = [g for g in gaps if g > 0]
        typical_gap = median(pos) if pos else None

    # align each printed label to its nearest slot, rejecting poor fits
    assignments = {}
    for label in detected_labels:
        if not slots:
            break
        lx = float(label.get("x_center", 0))
        best = min(slots, key=lambda s: abs(s["x_center"] - lx))
        if typical_gap:
            if abs(best["x_center"] - lx) / typical_gap > 0.45:
                continue   # reject: label doesn't cleanly fit a slot
        assignments[best["slot_index"]] = label.get("text", "")

    def _parse_date(text):
        cleaned = re.sub(r"\s+", " ", str(text).replace("'", "").replace("`", "").strip())
        for fmt in ("%B %Y", "%b %Y", "%B %y", "%b %y", "%B-%Y", "%b-%Y",
                    "%B-%y", "%b-%y", "%Y-%m"):
            try:
                p = datetime.strptime(cleaned, fmt)
                return {"month": p.month, "year": p.year, "had_year": True}
            except ValueError:
                continue
        m = _MONTHS.get(cleaned.lower().rstrip("."))
        if m:
            return {"month": m, "year": None, "had_year": False}
        return None

    def _parse_number(text):
        """A plain integer/number x-label (e.g. 'months spent' = 1,2,3)."""
        cleaned = str(text).strip().rstrip(".")
        m = re.fullmatch(r"-?\d+(?:\.\d+)?", cleaned)
        return float(cleaned) if m else None

    # classify the axis by the MAJORITY of printed labels: date vs numeric.
    date_parses, num_parses = [], []
    for slot_index, text in assignments.items():
        d = _parse_date(text)
        if d:
            date_parses.append((slot_index, d, str(text).strip()))
        else:
            n = _parse_number(text)
            if n is not None:
                num_parses.append((slot_index, n, str(text).strip()))

    is_date_axis = len(date_parses) >= len(num_parses) and len(date_parses) > 0
    is_num_axis = (not is_date_axis) and len(num_parses) > 0

    # ---------- DATE AXIS (what worked: anchor to a year, reconstruct, validate) ----------
    if is_date_axis:
        year_anchors = [(i, d) for (i, d, _) in date_parses if d["had_year"]]
        if not year_anchors:
            # MONTHS but NO year on the axis. The axis can still be MISPLACED
            # (values attached to the wrong month), so we DO use geometry to fix
            # the month SEQUENCE by position — but we do NOT invent a year. We
            # anchor on the first month label and reconstruct month names only;
            # the YEAR comes from the chart title downstream (it's already in the
            # canonical_metric). Output = month name (no year).
            date_parses.sort(key=lambda x: x[0])
            anchor_slot, a0, _ = date_parses[0]
            anchor_month = a0["month"]   # 1-12

            slot_mname = {}
            for slot in slots:
                diff = slot["slot_index"] - anchor_slot
                mnum = ((anchor_month - 1) + diff * monthly_step) % 12 + 1
                slot_mname[slot["slot_index"]] = datetime(2000, mnum, 1).strftime("%B")

            # validate month sequence against every printed month label
            valid = True
            for (i, d, _) in date_parses:
                if slot_mname[i] != datetime(2000, d["month"], 1).strftime("%B"):
                    valid = False

            out = {}
            for slot in slots:
                for e in slot["elements"]:
                    out[e["id"]] = slot_mname[slot["slot_index"]]
            # status "geometry_month" -> periods are MONTH-ONLY; caller keeps the
            # title/year from the canonical_metric and uses these months.
            return out, ("geometry_month" if valid else "flag")
        year_anchors.sort(key=lambda x: x[0])
        anchor_slot, a = year_anchors[0]
        anchor_date = datetime(a["year"], a["month"], 1)

        slot_period = {}
        for slot in slots:
            diff = slot["slot_index"] - anchor_slot
            inferred = anchor_date + relativedelta(months=(diff * monthly_step))
            slot_period[slot["slot_index"]] = inferred.strftime("%B %Y")

        valid = True
        for (i, d, _) in date_parses:
            dd = datetime.strptime(slot_period[i], "%B %Y")
            if dd.month != d["month"] or (d["had_year"] and dd.year != d["year"]):
                valid = False

        out = {}
        for slot in slots:
            for e in slot["elements"]:
                out[e["id"]] = slot_period[slot["slot_index"]]
        return out, ("geometry" if valid else "flag")

    # ---------- NUMERIC AXIS (same pattern: anchor to a number, reconstruct, validate) ----------
    if is_num_axis:
        num_parses.sort(key=lambda x: x[0])
        # derive the numeric step per slot from the two furthest-apart number labels
        (i0, n0, _), (i1, n1, _) = num_parses[0], num_parses[-1]
        span = i1 - i0
        num_step = ((n1 - n0) / span) if span else 1
        if num_step == 0:
            num_step = 1
        anchor_slot, anchor_num = i0, n0

        def _fmt(v):
            return str(int(v)) if float(v).is_integer() else str(v)

        slot_val = {}
        for slot in slots:
            v = anchor_num + (slot["slot_index"] - anchor_slot) * num_step
            slot_val[slot["slot_index"]] = _fmt(v)

        valid = True
        for (i, n, _) in num_parses:
            if slot_val[i] != _fmt(n):
                valid = False

        out = {}
        for slot in slots:
            for e in slot["elements"]:
                out[e["id"]] = slot_val[slot["slot_index"]]
        return out, ("geometry" if valid else "flag")

    # ---------- neither date nor numeric ----------
    # Not an axis the geometry is for (text labels, pie slices, categorical).
    # These do NOT have the sparse date/number alignment problem — the normal
    # Sonnet chart prompt reads them correctly. Signal "normal" so the caller
    # uses the standard extraction instead of quarantining.
    return {}, "normal"


# ---------------------------------------------------------------------------
# Pipeline hook — firm-scoped. For a CONFIGURED firm, re-reads each chart figure
# to capture element + label POSITIONS, resolves the period per slot, and sets
# each matching chart claim's period (as_at_date / canonical_metric suffix).
# For an unconfigured firm this is a no-op -> global behaviour unchanged.
# ---------------------------------------------------------------------------
_XAXIS_READ_SCHEMA = {
    "name": "record_chart",
    "description": "Record a chart's bars and x-axis labels with positions.",
    "parameters": {
        "type": "object",
        "properties": {
            "bars": {"type": "array", "items": {"type": "object", "properties": {
                "id": {"type": "integer"}, "series": {"type": "string"},
                "value": {"type": "string"}, "x_center": {"type": "number"}},
                "required": ["id", "value", "x_center"]}},
            "labels": {"type": "array", "items": {"type": "object", "properties": {
                "text": {"type": "string"}, "x_center": {"type": "number"}},
                "required": ["text", "x_center"]}},
        }, "required": ["bars", "labels"]},
}

_XAXIS_READ_PROMPT = (
    "Read this chart image. Record EVERY bar/point and EVERY printed x-axis label "
    "with its horizontal PIXEL position (x_center). For a bar's value use the "
    "number PRINTED on/beside it exactly. Include bars even with no printed label."
)


def resolve_periods_for_figure(img_b64, run_async_fn, llm_client,
                               model_key="tier2", monthly_step=1, x_tolerance=8):
    """For ONE figure image: read positions, resolve each element's period.
    Returns (value_to_period: {(series,value): period}, valid). Best-effort."""
    try:
        resp = run_async_fn(llm_client.extract(
            model_key, _XAXIS_READ_PROMPT, "", _XAXIS_READ_SCHEMA,
            image_base64=img_b64))
        ta = getattr(resp, "tool_arguments", None)
        if not isinstance(ta, dict):
            return {}, False
        bars = ta.get("bars", [])
        labels = ta.get("labels", [])
        id_to_period, status = resolve_axis_labels(
            bars, labels, monthly_step=monthly_step, x_tolerance=x_tolerance)
        # map (series,value) -> period so claims can be matched
        vp = {}
        for b in bars:
            period = id_to_period.get(b.get("id"))
            if period and period != "FLAG_REVIEW":
                vp[(str(b.get("series", "")), str(b.get("value", "")))] = period
        return vp, status      # status: "geometry" | "normal" | "flag"
    except Exception:
        return {}, "normal"    # on error, fall back to normal extraction
    
# MODULE 1: hybrid_extraction.py

# Change 1a — hybrid_extract_document signature. Find:

                            team=None, report_type=None, entity_ref=None,

                            vision_model_key: str = "tier2",
                            use_sonnet: bool = False) -> list:

# Replace with:

                            vision_model_key: str = "tier2",
                            use_sonnet: bool = False,
                            firm_xaxis_cfg=None) -> list:

# Change 1b — the figure block (attach resolved periods for the configured firm). Find:

                pts = _sonnet_read_image(
                    run_async_fn, llm_client, vision_model_key,
                    CHART_DIGITISATION_PROMPT, content, img_b64)
                if pts:
                    claims.extend(sonnet_points_to_claims(pts, el, filename, entity_ref))
                else:
                    claims.extend(desc_claims)   # vision found nothing -> keep desc

# Replace with:

                pts = _sonnet_read_image(
                    run_async_fn, llm_client, vision_model_key,
                    CHART_DIGITISATION_PROMPT, content, img_b64)
                if pts:
                    fig_claims = sonnet_points_to_claims(pts, el, filename, entity_ref)
                    # FIRM-SPECIFIC x-axis period resolution (only when configured
                    # for this firm). Re-reads the figure for positions, resolves
                    # the period per observation slot, and stamps it on each chart
                    # claim. Unconfigured firms skip this entirely.
                    if firm_xaxis_cfg:
                        try:
                            from .firm_xaxis import resolve_periods_for_figure
                            vp, _valid = resolve_periods_for_figure(
                                img_b64, run_async_fn, llm_client,
                                model_key=vision_model_key,
                                monthly_step=firm_xaxis_cfg.get("monthly_step", 1),
                                x_tolerance=firm_xaxis_cfg.get("x_tolerance", 8))
                            for c in fig_claims:
                                key = (str(getattr(c, "field_name", "") or ""),
                                       str(getattr(c, "value", "") or ""))
                                period = vp.get(key)
                                if period:
                                    # stamp the resolved period onto the claim
                                    setattr(c, "as_at_date", period)
                        except Exception:
                            pass
                    claims.extend(fig_claims)
                else:
                    claims.extend(desc_claims)   # vision found nothing -> keep desc

# MODULE 2: ai_parse_pipeline.py

# Change 2 — resolve the firm config + pass it to hybrid. Find:

            claims = hybrid_extract_document(
                ai_parse_response=parsed, filename=fname,
                team=sub["team"], report_type=sub["report_type"],
                entity_ref=sub.get("entity_ref"),
                use_sonnet=option_a, llm_client=llm_client,
                run_async_fn=run_async_fn, crop_figure_image=crop_fn)

#Replace with:

            # firm-specific x-axis config (None for unconfigured firms -> no-op)
            _fx_cfg = None
            try:
                from .firm_xaxis import _firm_config
                from .entity_path import parse_entity_path as _pep
                _ent = _pep(path)
                _fx_cfg = _firm_config(
                    firm_id=sub.get("entity_id") or _ent.get("entity_id"),
                    firm_name=sub.get("entity_name") or _ent.get("entity_name"))
            except Exception:
                _fx_cfg = None

            claims = hybrid_extract_document(
                ai_parse_response=parsed, filename=fname,
                team=sub["team"], report_type=sub["report_type"],
                entity_ref=sub.get("entity_ref"),
                use_sonnet=option_a, llm_client=llm_client,
                run_async_fn=run_async_fn, crop_figure_image=crop_fn,
                firm_xaxis_cfg=_fx_cfg)
            
# Evaluation Suite
"""Comprehensive extraction evaluation — ACCURACY, COVERAGE, ROBUSTNESS.

Built to an agreed spec:

ACCURACY (vs human-labelled ground truth, confusion matrix)
  * ground truth CSV rows: document, page, element_type, field_name, value,
    should_extract, [notes]
  * a claim matches a ground-truth row when ALL of: same DOCUMENT, same PAGE,
    FIELD matches (fuzzy: the human label's words appear in the canonical_metric),
    and VALUE matches — EXACT for table/text, within +-tol for figure (charts).
  * positives (should_extract=TRUE): matched -> TP, unmatched -> FN.
  * decoys (should_extract=FALSE): matched by a claim -> FP, else -> TN.
  * confusion matrix + precision / recall / accuracy / F1, overall & per type.

COVERAGE (structural completeness)
  * PAGE coverage  = distinct pages with claims / total pages (human-counted).
  * ELEMENT coverage = elements that produced >=1 claim / elements ai_parse found.

ROBUSTNESS (same document run twice)
  * fingerprint each claim (document, page, canonical_metric, value); compare the
    two runs. EXACT reproducibility for table/text; WITHIN-TOLERANCE for figures.
"""
from __future__ import annotations
import re
from collections import defaultdict, Counter


# ---------------------------------------------------------------------------
# normalisation helpers
# ---------------------------------------------------------------------------
def _norm_value(v) -> str:
    return re.sub(r"[,£$€%\s]", "", str(v if v is not None else "").lower())


def _as_number(v):
    try:
        return float(re.sub(r"[,£$€%\s]", "", str(v)))
    except (ValueError, TypeError):
        return None


def _norm_doc(name) -> str:
    """Document name without extension, lowercased, for matching."""
    s = str(name or "").lower().strip()
    return re.sub(r"\.(pptx|pdf|docx|xlsx)$", "", s)


def _field_words(text) -> set:
    return set(re.sub(r"[^a-z0-9 ]", " ", str(text or "").lower()).split())


def _field_matches(human_label, canonical) -> bool:
    """Fuzzy: the human label's words (mostly) appear in the canonical_metric."""
    hw = _field_words(human_label)
    if not hw:
        return True
    cw = _field_words(canonical)
    overlap = len(hw & cw)
    return overlap >= max(1, int(round(0.6 * len(hw))))   # >=60% of the human words


def _value_matches(extracted, truth, element_type, chart_tol):
    if _norm_value(extracted) == _norm_value(truth):
        return True
    if element_type in ("figure", "chart"):
        a, b = _as_number(extracted), _as_number(truth)
        if a is not None and b is not None and b != 0:
            return abs(a - b) / abs(b) <= chart_tol
    return False


def _claim_field(c):
    return getattr(c, "canonical_metric", None) or getattr(c, "field_name", "") or ""


def _claim_doc(c):
    return getattr(c, "source_document", None) or getattr(c, "source_document_id", "") or ""


# ---------------------------------------------------------------------------
# ACCURACY
# ---------------------------------------------------------------------------
def evaluate_accuracy(claims, truth_rows, chart_tol=0.03):
    """truth_rows: list of dicts (document,page,element_type,field_name,value,
    should_extract). Returns confusion matrix + metrics overall and per type."""
    positives = [t for t in truth_rows if str(t.get("should_extract", "")).strip().lower()
                 in ("true", "1", "yes", "y")]
    decoys = [t for t in truth_rows if t not in positives]

    # index claims by (doc, page) for quick lookup; track used so one claim can't
    # satisfy two truth rows.
    claims_by_dp = defaultdict(list)
    for c in claims:
        claims_by_dp[(_norm_doc(_claim_doc(c)), getattr(c, "page", None))].append(
            {"claim": c, "used": False})

    tp, fn = [], []
    for t in positives:
        key = (_norm_doc(t.get("document")), t.get("page"))
        et = (t.get("element_type") or "").lower()
        hit = None
        for rec in claims_by_dp.get(key, []):
            if rec["used"]:
                continue
            c = rec["claim"]
            if _field_matches(t.get("field_name"), _claim_field(c)) and \
               _value_matches(getattr(c, "value", None), t.get("value"), et, chart_tol):
                hit = rec
                break
        if hit:
            hit["used"] = True
            tp.append((hit["claim"], t))
        else:
            fn.append(t)

    # decoys: a claim matching a decoy -> FP; else TN
    fp, tn = [], []
    for t in decoys:
        key = (_norm_doc(t.get("document")), t.get("page"))
        matched = False
        for rec in claims_by_dp.get(key, []):
            c = rec["claim"]
            if _field_matches(t.get("field_name"), _claim_field(c)) and \
               _value_matches(getattr(c, "value", None), t.get("value"),
                              (t.get("element_type") or "").lower(), chart_tol):
                matched = True
                break
        (fp if matched else tn).append(t)

    def _metrics(ntp, nfp, nfn, ntn):
        prec = ntp / (ntp + nfp) if (ntp + nfp) else None
        rec = ntp / (ntp + nfn) if (ntp + nfn) else None
        acc = (ntp + ntn) / (ntp + ntn + nfp + nfn) if (ntp + ntn + nfp + nfn) else None
        f1 = (2 * prec * rec / (prec + rec)) if (prec and rec) else None
        return {"TP": ntp, "FP": nfp, "FN": nfn, "TN": ntn,
                "precision": _r(prec), "recall": _r(rec),
                "accuracy": _r(acc), "f1": _r(f1)}

    overall = _metrics(len(tp), len(fp), len(fn), len(tn))

    # per element type
    by_type = {}
    types = {(t.get("element_type") or "unknown").lower() for t in truth_rows}
    for ty in types:
        ttp = sum(1 for (_, t) in tp if (t.get("element_type") or "").lower() == ty)
        tfn = sum(1 for t in fn if (t.get("element_type") or "").lower() == ty)
        tfp = sum(1 for t in fp if (t.get("element_type") or "").lower() == ty)
        ttn = sum(1 for t in tn if (t.get("element_type") or "").lower() == ty)
        by_type[ty] = _metrics(ttp, tfp, tfn, ttn)

    return {
        "overall": overall,
        "by_element_type": by_type,
        "missed": [{"document": t.get("document"), "page": t.get("page"),
                    "field": t.get("field_name"), "value": t.get("value")} for t in fn],
        "false_positives": [{"document": t.get("document"), "page": t.get("page"),
                             "field": t.get("field_name"), "value": t.get("value")} for t in fp],
    }


# ---------------------------------------------------------------------------
# PAGE COUNT — read the RAW file (independent of ai_parse) so coverage catches
# EVERY skip, including pages ai_parse itself missed.
# ---------------------------------------------------------------------------
def count_pages(path):
    """Total pages/slides in the raw document, read directly from the file (NOT
    from ai_parse). PDF -> pdfplumber; PPTX -> python-pptx. Returns None for
    unsupported types (page coverage is then skipped for that doc)."""
    local = path
    if path.startswith("/Volumes"):
        local = path.replace("/Volumes", "/dbfs/Volumes")
    elif path.startswith("dbfs:"):
        local = path.replace("dbfs:", "/dbfs")
    low = path.lower()
    try:
        if low.endswith(".pdf"):
            import pdfplumber
            with pdfplumber.open(local) as pdf:
                return len(pdf.pages)
        if low.endswith((".pptx", ".ppt", ".potx")):
            from pptx import Presentation
            return len(Presentation(local).slides)
    except Exception:
        return None
    return None


def total_pages_for(submissions):
    """Build {document: total_pages} automatically from the raw files.
    document key = filename without extension (matches _norm_doc)."""
    import os
    out = {}
    for s in submissions:
        path = s.get("path") if isinstance(s, dict) else s
        if not path:
            continue
        name = os.path.basename(path)
        key = re.sub(r"\.(pptx|pdf|docx|xlsx|potx|ppt)$", "", name, flags=re.I)
        n = count_pages(path)
        if n is not None:
            out[key] = n
    return out


# ---------------------------------------------------------------------------
# COVERAGE
# ---------------------------------------------------------------------------
def evaluate_coverage(chunk_rows, total_pages_by_doc):
    """COVERAGE from the CHUNKS (search index) rows, which capture EVERY processed
    page -- including pages with text but NO values (silver/gold skip those). This
    makes coverage measure 'did we PROCESS every page', not 'did every page have a
    value'. A narrative/cover page has a chunk (so it counts as processed), while a
    genuinely SKIPPED page has no chunk (a real coverage gap).

    chunk_rows: list of dicts {source_document, page} (filter to the eval's
                entity_ref BEFORE passing in -- this is the entity-referenced set).
    total_pages_by_doc: {document: total_pages} -- the denominator (what should
                        exist). Human-counted or ai_parse's page count.
    """
    processed = defaultdict(set)   # doc -> set of pages that produced a chunk
    for r in chunk_rows:
        d = _norm_doc(r.get("source_document"))
        pg = r.get("page")
        if pg is not None:
            processed[d].add(pg)

    page_cov = {}
    for doc, total in total_pages_by_doc.items():
        nd = _norm_doc(doc)
        got = len(processed.get(nd, set()))
        page_cov[doc] = {
            "total_pages": total,
            "pages_processed": got,
            "coverage": _r(got / total) if total else None,
            "missing_pages": (total - got) if total else None,
        }

    tot = sum(v["total_pages"] for v in page_cov.values())
    got = sum(v["pages_processed"] for v in page_cov.values())
    return {
        "page_coverage": page_cov,
        "overall_page_coverage": _r(got / tot) if tot else None,
        "total_pages": tot, "pages_processed": got,
        "source": "chunks (every processed page, incl. value-less text)",
    }


# ---------------------------------------------------------------------------
# ROBUSTNESS (two runs)
# ---------------------------------------------------------------------------
def evaluate_robustness(claims_run_a, claims_run_b, chart_tol=0.03):
    """Compare two runs. EXACT reproducibility for table/text; WITHIN-TOLERANCE
    for figures. Fingerprint = (doc, page, canonical_metric, value)."""
    def _fp(c):
        return (_norm_doc(_claim_doc(c)), getattr(c, "page", None),
                _claim_field(c).lower().strip(),
                getattr(c, "element_type", None) or "", getattr(c, "value", None))

    A = [_fp(c) for c in claims_run_a]
    B = [_fp(c) for c in claims_run_b]

    # exact set comparison (ignores value tolerance)
    sa = Counter((d, p, f, et, _norm_value(v)) for (d, p, f, et, v) in A)
    sb = Counter((d, p, f, et, _norm_value(v)) for (d, p, f, et, v) in B)
    stable_exact = sum((sa & sb).values())
    only_a = sum((sa - sb).values())
    only_b = sum((sb - sa).values())

    # tolerance-aware: for figures, a value within tol counts as stable
    b_remaining = list((sb - sa).elements())
    a_remaining = list((sa - sb).elements())
    tol_recovered = 0
    for (d, p, f, et, v) in list(a_remaining):
        if et in ("figure", "chart"):
            av = _as_number(v)
            for (d2, p2, f2, et2, v2) in list(b_remaining):
                if (d, p, f) == (d2, p2, f2) and et2 == et:
                    bv = _as_number(v2)
                    if av is not None and bv is not None and bv != 0 and abs(av-bv)/abs(bv) <= chart_tol:
                        tol_recovered += 1
                        b_remaining.remove((d2, p2, f2, et2, v2))
                        break

    total = stable_exact + only_a + only_b
    stable_tol = stable_exact + tol_recovered

    # per type exact
    def _type_rate(ty):
        at = Counter((d,p,f,_norm_value(v)) for (d,p,f,et,v) in A if et==ty)
        bt = Counter((d,p,f,_norm_value(v)) for (d,p,f,et,v) in B if et==ty)
        st = sum((at & bt).values()); tot = st + sum((at-bt).values()) + sum((bt-at).values())
        return {"stable": st, "total": tot, "reproducibility": _r(st/tot) if tot else None}

    types = {et for (_,_,_,et,_) in A} | {et for (_,_,_,et,_) in B}
    return {
        "claims_run_a": len(claims_run_a), "claims_run_b": len(claims_run_b),
        "exact_reproducibility": _r(stable_exact / total) if total else None,
        "within_tolerance_reproducibility": _r(stable_tol / total) if total else None,
        "stable_exact": stable_exact, "only_in_a": only_a, "only_in_b": only_b,
        "by_element_type_exact": {ty: _type_rate(ty) for ty in types},
    }


def _r(x, n=4):
    return round(x, n) if isinstance(x, (int, float)) else x


# ---------------------------------------------------------------------------
# REPORT
# ---------------------------------------------------------------------------
def full_report(accuracy=None, coverage=None, robustness=None) -> str:
    L = ["=" * 64, "  EXTRACTION EVALUATION — Accuracy · Coverage · Robustness", "=" * 64]
    if accuracy:
        o = accuracy["overall"]
        L += ["", "ACCURACY (vs human ground truth)",
              f"  Confusion:  TP={o['TP']}  FP={o['FP']}  FN={o['FN']}  TN={o['TN']}",
              f"  Precision:  {_p(o['precision'])}   (of extracted, how much correct)",
              f"  Recall:     {_p(o['recall'])}   (of labelled, how much we got)",
              f"  Accuracy:   {_p(o['accuracy'])}   (reals extracted + decoys ignored)",
              f"  F1:         {_p(o['f1'])}",
              "  By element type:"]
        for ty, m in accuracy["by_element_type"].items():
            L.append(f"    {ty:8s} P={_p(m['precision'])} R={_p(m['recall'])} "
                     f"Acc={_p(m['accuracy'])}  (TP={m['TP']} FP={m['FP']} FN={m['FN']} TN={m['TN']})")
        if accuracy["missed"]:
            L.append(f"  MISSED ({len(accuracy['missed'])}):")
            for m in accuracy["missed"][:8]:
                L.append(f"    {m['document']} p{m['page']} {m['field']}={m['value']}")
    if coverage:
        L += ["", "COVERAGE (pages PROCESSED, from chunks — incl. value-less text)",
              f"  Overall PAGE coverage: {_p(coverage['overall_page_coverage'])} "
              f"({coverage['pages_processed']}/{coverage['total_pages']} pages)"]
        for doc, v in coverage["page_coverage"].items():
            flag = "  <-- MISSING PAGES (skipped)" if v["missing_pages"] else ""
            L.append(f"    {doc}: {v['pages_processed']}/{v['total_pages']} "
                     f"({_p(v['coverage'])}){flag}")
    if robustness:
        r = robustness
        L += ["", "ROBUSTNESS (same document, two runs)",
              f"  Run A: {r['claims_run_a']} claims   Run B: {r['claims_run_b']} claims",
              f"  EXACT reproducibility:          {_p(r['exact_reproducibility'])}",
              f"  WITHIN-TOLERANCE reproducibility: {_p(r['within_tolerance_reproducibility'])}",
              f"  (stable={r['stable_exact']} only_A={r['only_in_a']} only_B={r['only_in_b']})",
              "  By element type (exact):"]
        for ty, m in r["by_element_type_exact"].items():
            L.append(f"    {ty:8s} {_p(m['reproducibility'])}  ({m['stable']}/{m['total']})")
    L += ["", "=" * 64]
    return "\n".join(L)


def _p(x):
    return f"{x*100:.1f}%" if isinstance(x, (int, float)) else "n/a"

# Evaluation Notebook

# Databricks notebook source
# MAGIC %md
# MAGIC # Extraction Evaluation — Accuracy · Coverage · Robustness
# MAGIC **This notebook does NOT run extraction.** It READS already-stored results
# MAGIC and compares them to a human-labelled ground-truth CSV. No LLM calls.
# MAGIC
# MAGIC You run the extraction yourself beforehand:
# MAGIC - **once** for accuracy + coverage (writes to a table prefix, e.g. `runA_`)
# MAGIC - **a second time** into a DIFFERENT prefix (e.g. `runB_`) if you want robustness
# MAGIC
# MAGIC Then this notebook reads those stored tables and reports:
# MAGIC - **Accuracy** — confusion matrix (TP/FP/FN/TN) + precision/recall/accuracy/F1, per type
# MAGIC - **Coverage** — pages processed (chunks) vs total pages (auto-counted from raw files)
# MAGIC - **Robustness** — run A vs run B (two stored results compared)

# COMMAND ----------
# MAGIC %md ## 0 · Config — point at the ALREADY-STORED runs + the ground truth

# COMMAND ----------
from preprocessing_etl.custom.evaluation_suite import (
    evaluate_accuracy, evaluate_coverage, evaluate_robustness, full_report,
    total_pages_for)

# the table-set prefixes you already extracted into (run the pipeline first!)
PREFIX_A = "runA_"          # the run used for accuracy + coverage
PREFIX_B = "runB_"          # the SECOND run of the SAME docs, for robustness (optional)
DB = "docextract.reporting" # catalog.schema where the tables live

GROUND_TRUTH_CSV = "/Volumes/<catalog>/<schema>/<volume>/eval/ground_truth.csv"
CHART_TOL = 0.03            # charts are estimates: +-3%

# the eval documents (paths) — used ONLY to auto-count total pages (no extraction)
EVAL_SUBMISSIONS = [
    {"path": "/Volumes/<catalog>/<schema>/<volume>/docs/LCR_BankA.pptx", "entity_ref": "BankA"},
    # ... the other 4 ...
]
EVAL_ENTITY_REFS = [s.get("entity_ref") for s in EVAL_SUBMISSIONS if s.get("entity_ref")]

# COMMAND ----------
# MAGIC %md ## 1 · Load the ground-truth CSV

# COMMAND ----------
import csv
def load_ground_truth(path):
    local = (path.replace("/Volumes", "/dbfs/Volumes") if path.startswith("/Volumes")
             else path.replace("dbfs:", "/dbfs") if path.startswith("dbfs:") else path)
    rows = []
    with open(local, newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            rows.append({k.strip().lower(): (v.strip() if isinstance(v, str) else v)
                         for k, v in r.items()})
    for r in rows:
        try: r["page"] = int(float(r.get("page")))
        except (TypeError, ValueError): r["page"] = None
    return rows

truth_rows = load_ground_truth(GROUND_TRUTH_CSV)
print(f"loaded {len(truth_rows)} ground-truth rows")

# COMMAND ----------
# MAGIC %md ## 2 · Read the STORED claims + chunks (no extraction, just SQL)

# COMMAND ----------
class _C: pass

def read_claims(prefix):
    """Read the already-stored claims from the silver table (all extracted values)."""
    rows = spark.sql(f"""
        SELECT source_document, page, element_type, canonical_metric, value
        FROM {DB}.{prefix}silver
    """).collect()
    out = []
    for r in rows:
        c = _C()
        c.source_document=r["source_document"]; c.page=r["page"]
        c.element_type=r["element_type"]; c.canonical_metric=r["canonical_metric"]
        c.value=r["value"]
        out.append(c)
    return out

def read_chunks(prefix, entity_refs):
    where = ""
    if entity_refs:
        refs = ",".join(f"'{r}'" for r in entity_refs)
        where = f"WHERE entity_ref IN ({refs})"
    rows = spark.sql(f"""
        SELECT source_document_id AS source_document, page
        FROM {DB}.{prefix}search_chunks
        {where}
    """).collect()
    return [{"source_document": r["source_document"], "page": r["page"]} for r in rows]

claims_a = read_claims(PREFIX_A)
chunks_a = read_chunks(PREFIX_A, EVAL_ENTITY_REFS)
print(f"run A: {len(claims_a)} claims, {len(chunks_a)} chunks")

# COMMAND ----------
# MAGIC %md ## 3 · ACCURACY — confusion matrix vs ground truth (stored run A)

# COMMAND ----------
accuracy = evaluate_accuracy(claims_a, truth_rows, chart_tol=CHART_TOL)

# COMMAND ----------
# MAGIC %md ## 4 · COVERAGE — pages processed (chunks) vs total pages (auto from raw files)

# COMMAND ----------
TOTAL_PAGES = total_pages_for(EVAL_SUBMISSIONS)   # auto page count from raw files
print("total pages (auto):", TOTAL_PAGES)
coverage = evaluate_coverage(chunks_a, total_pages_by_doc=TOTAL_PAGES)

# COMMAND ----------
# MAGIC %md ## 5 · ROBUSTNESS — compare the TWO stored runs (A vs B)
# MAGIC Only if you extracted the same documents into PREFIX_B as well.

# COMMAND ----------
robustness = None
try:
    claims_b = read_claims(PREFIX_B)
    print(f"run B: {len(claims_b)} claims")
    robustness = evaluate_robustness(claims_a, claims_b, chart_tol=CHART_TOL)
except Exception as e:
    print(f"(skipping robustness — PREFIX_B not available: {e})")

# COMMAND ----------
# MAGIC %md ## 6 · FULL REPORT

# COMMAND ----------
print(full_report(accuracy=accuracy, coverage=coverage, robustness=robustness))

