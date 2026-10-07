# %% [markdown]
# # MODULE 1: llm_client.py

# %%
# Change 1a — extract signature. Find:

    async def extract(self, model_key: str, prompt: str, content: str,
                      tool_schema: dict,
                      image_base64: Optional[str] = None) -> LLMResponse:

# Replace

    async def extract(self, model_key: str, prompt: str, content: str,
                      tool_schema: dict,
                      image_base64: Optional[str] = None,
                      fewshot_examples: Optional[list] = None) -> LLMResponse:

# %%
# Change 1b — the messages block. Find:

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

# Replace

        # FEW-SHOT examples (list of {"image": <b64>, "output": <JSON str>}):
        # each is injected as a prior user(image)->assistant(correct output) turn
        # so the model learns correct reading by example. Empty/None = unchanged.
        messages = [{"role": "system", "content": prompt}]
        for ex in (fewshot_examples or []):
            img = ex.get("image"); out = ex.get("output")
            if img and out:
                messages.append({"role": "user", "content": [
                    {"type": "image_url",
                     "image_url": {"url": f"data:image/png;base64,{img}"}},
                    {"type": "text", "text": "Extract this chart's data points."},
                ]})
                messages.append({"role": "assistant", "content": out})
        messages.append({"role": "user", "content": user_content})

        resp = await self._create(
            model=spec.endpoint,
            messages=messages,
            tools=[{"type": "function", "function": tool_schema}],
            tool_choice={"type": "function", "function": {"name": tool_name}},
            temperature=0,
        )

# %% [markdown]
# # MODULE 2: hybrid_extraction.py

# %%
# Change 2a — _sonnet_read_image signature. Find:

def _sonnet_read_image(run_async_fn, llm_client, model_key, prompt, content, img_b64):
    
# Replace

def _sonnet_read_image(run_async_fn, llm_client, model_key, prompt, content, img_b64,
                       fewshot_examples=None):


# %%

# Change 2b — the extract call inside _sonnet_read_image. Find:

    resp = run_async_fn(llm_client.extract(
        model_key, prompt, content or "", EXTRACTION_TOOL_SCHEMA,
        image_base64=img_b64))

# replace

    resp = run_async_fn(llm_client.extract(
        model_key, prompt, content or "", EXTRACTION_TOOL_SCHEMA,
        image_base64=img_b64, fewshot_examples=fewshot_examples))

# %%
# Change 2c — hybrid_extract_document signature. Find:

                            vision_model_key: str = "tier2",
                            use_sonnet: bool = False) -> list:

# Replace

                            vision_model_key: str = "tier2",
                            use_sonnet: bool = False,
                            fewshot_examples=None) -> list:

# %%
# Change 2d — the _sonnet_read_image call in the figure block. Find:

                pts = _sonnet_read_image(
                    run_async_fn, llm_client, vision_model_key,
                    CHART_DIGITISATION_PROMPT, content, img_b64)

# Replace

                pts = _sonnet_read_image(
                    run_async_fn, llm_client, vision_model_key,
                    CHART_DIGITISATION_PROMPT, content, img_b64,
                    fewshot_examples=fewshot_examples)

# %% [markdown]
# # MODULE 3: ai_parse_pipeline.py

# %%
# Change 3a — add the firm-scoped loader. Add this function at module level (e.g. after set_crop_debug_dir):

def _get_fewshot_examples(firm_id=None, firm_name=None):
    """Auto-load FIRM-SCOPED few-shot examples from firm_few_shot.FIRM_EXAMPLES.
    Returns the current firm's example(s) as [{"image": <base64>, "output": <str>}],
    loading image_path -> base64 as needed. Firm with no entry (or any error) -> []
    so the normal (working) chart-reading path is used."""
    try:
        from . import firm_few_shot
    except Exception:
        return []
    table = getattr(firm_few_shot, "FIRM_EXAMPLES", {}) or {}
    if not table:
        return []
    cands = [str(x).strip().lower().replace(" ", "")
             for x in (firm_id, firm_name) if x]
    matched = None
    for key, examples in table.items():
        k = str(key).strip().lower().replace(" ", "")
        if k and any(k in c or c in k for c in cands):
            matched = examples
            break
    if not matched:
        return []
    import base64 as _b64
    out = []
    for ex in matched:
        if not isinstance(ex, dict) or not ex.get("output"):
            continue
        img_b64 = ex.get("image")
        if not img_b64 and ex.get("image_path"):
            pth = ex["image_path"].replace("dbfs:/Volumes", "/Volumes").replace("dbfs:", "/dbfs")
            try:
                with open(pth, "rb") as fh:
                    img_b64 = _b64.b64encode(fh.read()).decode()
            except Exception:
                continue
        if img_b64:
            out.append({"image": img_b64, "output": ex["output"]})
    return out

# %%
# Change 3b — resolve the firm + pass examples to extraction. Find:

            claims = hybrid_extract_document(
                ai_parse_response=parsed, filename=fname,
                team=sub["team"], report_type=sub["report_type"],
                entity_ref=sub.get("entity_ref"),
                use_sonnet=option_a, llm_client=llm_client,
                run_async_fn=run_async_fn, crop_figure_image=crop_fn)

# Replace

            # resolve the FIRM for this document (entity_id / entity_name) so
            # firm-scoped few-shot examples can be applied. No firm match -> [].
            from .entity_path import parse_entity_path as _pep
            _ent = _pep(path)
            _firm_id = sub.get("entity_id") or _ent.get("entity_id")
            _firm_name = sub.get("entity_name") or _ent.get("entity_name")
            _fewshots = _get_fewshot_examples(firm_id=_firm_id, firm_name=_firm_name)

            claims = hybrid_extract_document(
                ai_parse_response=parsed, filename=fname,
                team=sub["team"], report_type=sub["report_type"],
                entity_ref=sub.get("entity_ref"),
                use_sonnet=option_a, llm_client=llm_client,
                run_async_fn=run_async_fn, crop_figure_image=crop_fn,
                fewshot_examples=_fewshots)

# %% [markdown]
# # EVALUATION

# %%
"""Evaluation metrics for the content-extraction pipeline.

Four axes, chosen so the evaluation catches the REAL failure modes of an
EXTRACTION system — not just the ones a generative-AI eval looks for:

  1. COVERAGE   — did we extract EVERYTHING? (pages AND modality/element types).
                  This is the axis a hallucination-only eval misses: a page
                  skipped silently produces NO hallucination, so a faithfulness
                  metric would never flag it. For extraction, MISSING content is
                  the primary risk, so coverage leads.
  2. FAITHFULNESS — is what we extracted grounded in the source (not fabricated)?
                  Matters most for the ESTIMATIVE parts (chart/vision reads),
                  which are flagged llm_estimated.
  3. SPEED      — throughput: documents/pages per second, wall-clock.
  4. COST       — LLM spend (chart/vision calls) per document/page.

The module is reporting-only: it reads what the run produced (BatchResult, the
parsed elements, the claims, timing, token usage) and returns structured metrics
+ a readable table. It never changes extraction behaviour.
"""
from __future__ import annotations
from collections import Counter
from typing import Optional


# ---------------------------------------------------------------------------
# 1. COVERAGE — pages and modality
# ---------------------------------------------------------------------------
def coverage_metrics(parsed_elements, claims, pages_in_doc=None) -> dict:
    """Coverage for ONE document.

    parsed_elements : the ParsedElement list ai_parse returned (what SHOULD be
                      extractable).
    claims          : the claims actually produced.
    pages_in_doc    : total pages in the document (from ai_parse's pages array);
                      if None, inferred from the elements' page numbers.

    Catches the '20 skipped pages' failure: compares pages that HAVE elements
    (and claims) against the document's total page count, and reports any page
    that produced no claims.
    """
    # ---- PAGE coverage ----
    el_pages = {getattr(e, "page", None) for e in parsed_elements
                if getattr(e, "page", None) is not None}
    claim_pages = {getattr(c, "page", None) for c in claims
                   if getattr(c, "page", None) is not None}
    total_pages = pages_in_doc if pages_in_doc else (max(el_pages) + 1 if el_pages else 0)

    pages_with_elements = len(el_pages)
    pages_with_claims = len(claim_pages)
    # pages ai_parse saw content on but we produced NO claims for (a real gap)
    pages_missing_claims = sorted(p for p in el_pages if p not in claim_pages)
    # pages ai_parse did not report elements for at all (possible skipped pages)
    pages_no_elements = ([p for p in range(total_pages) if p not in el_pages]
                         if pages_in_doc else [])

    page_cov = (pages_with_claims / total_pages) if total_pages else 0.0

    # ---- MODALITY / element-type coverage ----
    el_by_type = Counter(getattr(e, "el_type", "?") for e in parsed_elements)
    # which element types produced at least one claim?
    claim_src_pages = {(getattr(c, "page", None)) for c in claims}
    # map claims back to element types via element_type on the claim where present
    claim_by_type = Counter(getattr(c, "element_type", None) or "unknown" for c in claims)
    # element types ai_parse found but that produced NO claims of that type
    types_with_elements = set(el_by_type)
    types_with_claims = {t for t in claim_by_type if t and t != "unknown"}
    types_missing = sorted(t for t in types_with_elements
                           if t not in types_with_claims
                           and t not in ("page_header", "page_footer", "page_number"))

    return {
        "total_pages": total_pages,
        "pages_with_elements": pages_with_elements,
        "pages_with_claims": pages_with_claims,
        "pages_missing_claims": pages_missing_claims,     # had content, no claims
        "pages_no_elements": pages_no_elements,           # ai_parse saw nothing
        "page_coverage": round(page_cov, 4),
        "elements_by_type": dict(el_by_type),
        "claims_by_type": {k: v for k, v in claim_by_type.items()},
        "modality_types_missing": types_missing,          # a type extracted nothing
        "n_elements": len(parsed_elements),
        "n_claims": len(claims),
    }


# ---------------------------------------------------------------------------
# 2. FAITHFULNESS — grounded vs estimated
# ---------------------------------------------------------------------------
def faithfulness_metrics(claims, grounded_fn=None) -> dict:
    """Faithfulness for ONE document's claims.

    Splits claims by provenance tier:
      * parsed / rule-based  -> deterministic, grounded by construction
      * llm_estimated        -> vision/chart estimates (the hallucination-risk set)
    If grounded_fn(claim) is provided (e.g. a check that the value appears in the
    source text), it is applied to report a grounded fraction; otherwise we report
    the tier split, which is the honest signal we have without ground-truth labels.
    """
    tiers = Counter(str(getattr(c, "citation_tier", "") or "unknown") for c in claims)
    n = len(claims) or 1
    estimated = sum(v for k, v in tiers.items() if "estimated" in k.lower())
    parsed = sum(v for k, v in tiers.items()
                 if "parsed" in k.lower() or "rule" in k.lower())

    out = {
        "n_claims": len(claims),
        "by_tier": dict(tiers),
        "grounded_parsed_frac": round(parsed / n, 4),      # deterministic share
        "llm_estimated_frac": round(estimated / n, 4),     # hallucination-risk share
    }
    if grounded_fn is not None:
        checked = [c for c in claims]
        grounded = sum(1 for c in checked if grounded_fn(c))
        out["grounded_verified_frac"] = round(grounded / (len(checked) or 1), 4)
    return out


# ---------------------------------------------------------------------------
# 3. SPEED
# ---------------------------------------------------------------------------
def speed_metrics(elapsed_seconds: float, documents: int,
                  total_pages: int = 0, claims: int = 0) -> dict:
    """Throughput for the run."""
    el = max(elapsed_seconds, 1e-9)
    return {
        "elapsed_seconds": round(elapsed_seconds, 2),
        "docs_per_min": round(documents / el * 60, 2),
        "pages_per_min": round(total_pages / el * 60, 2) if total_pages else None,
        "seconds_per_doc": round(el / documents, 2) if documents else None,
        "seconds_per_page": round(el / total_pages, 3) if total_pages else None,
        "claims_per_min": round(claims / el * 60, 1) if claims else None,
    }


# ---------------------------------------------------------------------------
# 4. COST
# ---------------------------------------------------------------------------
def cost_metrics(sonnet_calls: int = 0, input_tokens: int = 0,
                 output_tokens: int = 0, documents: int = 0, total_pages: int = 0,
                 in_rate_per_m: float = 3.00, out_rate_per_m: float = 15.00,
                 ai_parse_pages: int = 0, ai_parse_rate_per_page: float = 0.0) -> dict:
    """LLM + ai_parse cost for the run. Rates default to tier2 (Sonnet) pricing;
    override with your actual rates."""
    llm_cost = (input_tokens / 1e6) * in_rate_per_m + (output_tokens / 1e6) * out_rate_per_m
    parse_cost = ai_parse_pages * ai_parse_rate_per_page
    total = llm_cost + parse_cost
    return {
        "sonnet_calls": sonnet_calls,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "llm_cost_usd": round(llm_cost, 4),
        "ai_parse_cost_usd": round(parse_cost, 4),
        "total_cost_usd": round(total, 4),
        "cost_per_doc_usd": round(total / documents, 4) if documents else None,
        "cost_per_page_usd": round(total / total_pages, 5) if total_pages else None,
    }


# ---------------------------------------------------------------------------
# AGGREGATE — one evaluation across a run
# ---------------------------------------------------------------------------
def evaluate_run(per_doc_coverage: list, result=None, elapsed_seconds: float = 0.0,
                 sonnet_calls: int = 0, input_tokens: int = 0, output_tokens: int = 0,
                 all_claims: Optional[list] = None, cost_kwargs: Optional[dict] = None,
                 speed_kwargs: Optional[dict] = None) -> dict:
    """Combine per-document coverage + run-level faithfulness/speed/cost into one
    evaluation. per_doc_coverage is a list of coverage_metrics() dicts (one per
    document). result is the BatchResult (for document/claim totals)."""
    docs = getattr(result, "documents", len(per_doc_coverage)) if result else len(per_doc_coverage)
    total_pages = sum(d.get("total_pages", 0) for d in per_doc_coverage)
    pages_with_claims = sum(d.get("pages_with_claims", 0) for d in per_doc_coverage)
    pages_missing = sum(len(d.get("pages_missing_claims", [])) for d in per_doc_coverage)
    pages_no_elements = sum(len(d.get("pages_no_elements", [])) for d in per_doc_coverage)
    claims_total = getattr(result, "claims_total", 0) if result else (
        len(all_claims) if all_claims else sum(d.get("n_claims", 0) for d in per_doc_coverage))

    agg_coverage = {
        "documents": docs,
        "total_pages": total_pages,
        "pages_with_claims": pages_with_claims,
        "page_coverage": round(pages_with_claims / total_pages, 4) if total_pages else 0.0,
        "pages_missing_claims_total": pages_missing,     # had content, no claims
        "pages_no_elements_total": pages_no_elements,    # possibly skipped pages
    }

    faithfulness = faithfulness_metrics(all_claims or [])
    speed = speed_metrics(elapsed_seconds, docs, total_pages, claims_total,
                          **(speed_kwargs or {}))
    cost = cost_metrics(sonnet_calls=sonnet_calls, input_tokens=input_tokens,
                        output_tokens=output_tokens, documents=docs,
                        total_pages=total_pages, **(cost_kwargs or {}))

    return {"coverage": agg_coverage, "faithfulness": faithfulness,
            "speed": speed, "cost": cost}


def evaluation_table(ev: dict, label: str = "run") -> str:
    """Readable table of the evaluation."""
    lines = [f"=== EVALUATION — {label} ===", ""]
    cov = ev["coverage"]
    lines += ["COVERAGE (did we extract everything?)",
              f"  documents:            {cov['documents']}",
              f"  total pages:          {cov['total_pages']}",
              f"  pages with claims:    {cov['pages_with_claims']}",
              f"  PAGE COVERAGE:        {cov['page_coverage']*100:.1f}%",
              f"  pages w/ content but NO claims: {cov['pages_missing_claims_total']}  <-- gap",
              f"  pages ai_parse saw NOTHING on:  {cov['pages_no_elements_total']}  <-- possible skip",
              ""]
    f = ev["faithfulness"]
    lines += ["FAITHFULNESS (is it grounded?)",
              f"  claims:               {f['n_claims']}",
              f"  grounded/parsed:      {f['grounded_parsed_frac']*100:.1f}%",
              f"  llm_estimated:        {f['llm_estimated_frac']*100:.1f}%  (hallucination-risk set)",
              ""]
    s = ev["speed"]
    lines += ["SPEED",
              f"  elapsed:              {s['elapsed_seconds']}s",
              f"  docs/min:             {s['docs_per_min']}",
              f"  seconds/page:         {s['seconds_per_page']}",
              ""]
    c = ev["cost"]
    lines += ["COST",
              f"  sonnet calls:         {c['sonnet_calls']}",
              f"  total cost:           ${c['total_cost_usd']}",
              f"  cost/doc:             ${c['cost_per_doc_usd']}",
              f"  cost/page:            ${c['cost_per_page_usd']}",
              ""]
    return "\n".join(lines)

# %%
# How to use it in your notebook:

from preprocessing_etl.custom.evaluation import coverage_metrics, evaluate_run, evaluation_table
from preprocessing_etl.custom.hybrid_extraction import parse_ai_parse_response
import time

# per document, build coverage (needs parsed elements, claims, and page count)
per_doc = []
# (in practice, collect these as you process each doc, or re-derive from stored data)
els = parse_ai_parse_response(raw)
pages = len(d.get("pages", []))   # total pages from ai_parse
per_doc.append(coverage_metrics(els, claims, pages_in_doc=pages))

# aggregate evaluation (time the run, pass token usage if you track it)
ev = evaluate_run(per_doc, result=result, elapsed_seconds=elapsed,
                  sonnet_calls=n_calls, input_tokens=in_tok, output_tokens=out_tok,
                  all_claims=all_claims)
print(evaluation_table(ev, "v6 run"))

# few_shot_example
"""FIRM few-shot examples — filled in by teams, auto-loaded by the pipeline.

Teams add a firm's chart example(s) HERE. When the pipeline processes a document
it resolves the firm and looks it up; if present it uses that firm's example(s),
otherwise it moves on with the normal (working) path — so firms without an entry
are completely unaffected.

PURPOSE: teach ONE specific, hard-to-read SKILL (e.g. reading a MISALIGNED /
MISPLACED SCALE), NOT to make the model guess. Give a MINIMAL, TARGETED example:
'here is a chart whose scale is misplaced, and here is how to read it correctly'.

HOW TO ADD A FIRM
-----------------
1. Save the firm's example chart image to a Volume the cluster can read.
2. Add an entry keyed by the firm's entity_name or entity_id (matched
   case-insensitively, substring-tolerant, so 'IkeBank' matches 'Ike Bank Plc'):

   FIRM_EXAMPLES = {
       "IkeBank": [
           {
               "image_path": "/Volumes/<cat>/<schema>/<vol>/examples/ikebank_misaligned_scale.png",
               # 'note' teaches the SPECIFIC skill + guards against guessing:
               "note": ("In this chart the y-axis scale is MISALIGNED/offset — the "
                        "gridline labels do not sit level with the plotted values. "
                        "Read each bar/point against the TRUE scale position (interpolate "
                        "between the correctly-placed gridlines), NOT the nearest printed "
                        "label. IMPORTANT: only return values you can actually read; if a "
                        "value is genuinely unreadable, OMIT it — do not guess."),
               # 'output' = the CORRECT reading (demonstrates proper scale-reading,
               # and omits anything unreadable so it does not model guessing):
               "output": '{"claims": ['
                         '{"field_name": "<series> | <category>", "value": "<correct value>", "confidence": 0.9}'
                         ']}',
           },
       ],
   }

Rules:
- Keep it MINIMAL — one targeted example per skill. More examples cost more per
  read and can over-generalise.
- The 'note' should name the specific skill AND tell the model to OMIT what it
  cannot read (so a teaching example does not encourage guessing).
- The 'output' MUST be VERIFIED-CORRECT and should itself omit anything unreadable.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# TEAMS: add your firm entries here. Empty by default -> no firm is affected.
# ---------------------------------------------------------------------------
FIRM_EXAMPLES: dict = {
    # "IkeBank": [
    #     {
    #         "image_path": "/Volumes/.../examples/ikebank_misaligned_scale.png",
    #         "note": "In this chart the y-axis scale is MISALIGNED ... read against "
    #                 "the TRUE scale position; OMIT anything you cannot read, do not guess.",
    #         "output": '{"claims": [ ...verified-correct reading... ]}',
    #     },
    # ],
}


# =============================================================================
#  FIRST-DRAFT TEST: X-AXIS LABEL RESOLUTION (geometry + LLM)
#
#  Your charts: spacing is ALWAYS regular; the problem is a varying START month
#  and MISSING FIRST / LAST labels. Because spacing is regular, we can:
#    1. LLM reads: each bar's VALUE + x-position, and each printed x-LABEL + position
#    2. GEOMETRY: assign printed labels to nearest bars, derive the monthly stride
#       from two anchors, then EXTRAPOLATE every bar's month (fills missing
#       first/last labels, handles the varying start) — deterministic & safe
#       because the stride is regular.
#    3. OUTPUT: each VALUE attached to its correct resolved period.
#
#  Flags only when there are < 2 printed labels (stride cannot be derived).
#  Run on ONE chart to validate before wiring into the pipeline.
# =============================================================================
import base64, json, re
from datetime import datetime
from dateutil.relativedelta import relativedelta

CHART_PATH = "/Volumes/<catalog>/<schema>/<volume>/test_chart.png"   # <-- your chart
# client and run_async must be defined in your session

# ---- 1. LLM reads values + positions + printed labels + positions ----
READ_PROMPT = (
    "You are reading a bar/line chart. Return STRICT JSON (no prose):\n"
    '{"bars": [{"id": <int, left-to-right from 0>, "series": "<series>", '
    '"value": <printed number>, "x_center": <horizontal PIXEL position of this '
    'bar/point centre>}], '
    '"labels": [{"text": "<x-axis label exactly as printed, e.g. Jul 2024>", '
    '"x_center": <horizontal PIXEL position of this label centre>}]}\n'
    "Rules:\n"
    "- VALUE: the number PRINTED on/beside the bar/point, exactly. Do NOT estimate "
    "from the axis scale.\n"
    "- Give EVERY bar/point (even if it has no printed x-label) and EVERY printed "
    "x-axis label, each with its x_center pixel position.\n"
    "- Output ONLY the JSON object."
)

with open(CHART_PATH.replace("dbfs:", "/dbfs"), "rb") as f:
    img_b64 = base64.b64encode(f.read()).decode()

import asyncio
async def _call():
    resp = await client.extract("tier2", READ_PROMPT, "", {"name": "noop"},
                                image_base64=img_b64)
    return getattr(resp, "text", "") or json.dumps(getattr(resp, "tool_arguments", {}))

raw = run_async(_call())
try:
    data = json.loads(raw.strip().strip("`").replace("json", "", 1).strip())
except Exception:
    m = re.search(r"\{.*\}", raw, re.S)
    data = json.loads(m.group(0)) if m else {"bars": [], "labels": []}

bars_in = data.get("bars", [])
labels_in = data.get("labels", [])
print(f"LLM read {len(bars_in)} bars, {len(labels_in)} printed labels")
print(f"printed labels: {[(l.get('text'), l.get('x_center')) for l in labels_in]}\n")


# ---- 2. GEOMETRY: resolve every bar's period from anchor + regular stride ----
def resolve_axis_labels(detected_bars, detected_labels):
    bars = sorted(detected_bars, key=lambda b: b.get('x_center', 0))

    # assign each printed label to its nearest bar
    assignments = {}
    for lbl in detected_labels:
        if not bars:
            break
        best = min(bars, key=lambda b: abs(b.get('x_center', 0) - lbl.get('x_center', 0)))
        assignments[best['id']] = lbl.get('text', '')

    # parse labeled bars to (bar_index, date) — parse WITH year when present
    parsed = []
    for idx, bar in enumerate(bars):
        if bar['id'] in assignments:
            text = str(assignments[bar['id']]).strip()
            for fmt in ("%B %Y", "%b %Y", "%Y-%m", "%b-%y", "%B", "%b"):
                try:
                    parsed.append((idx, datetime.strptime(text, fmt)))
                    break
                except ValueError:
                    continue

    # need >= 2 anchors to DERIVE the stride; otherwise flag the unlabeled
    if len(parsed) < 2:
        return {b['id']: assignments.get(b['id'], "FLAG_REVIEW") for b in bars}, False

    # derive the monthly stride (spacing is regular, so two anchors suffice)
    (i1, d1), (i2, d2) = parsed[0], parsed[1]
    months_diff = (d2.year - d1.year) * 12 + (d2.month - d1.month)
    index_diff = (i2 - i1) or 1
    step_months = int(round(months_diff / index_diff)) or 1

    # compute EVERY bar's month from the anchor + stride (fills missing
    # first/last labels; handles the varying start — no start assumption needed)
    base_idx, base_date = parsed[0]
    out = {}
    for idx, bar in enumerate(bars):
        if bar['id'] in assignments:
            out[bar['id']] = assignments[bar['id']]                 # keep printed label
        else:
            inferred = base_date + relativedelta(months=(idx - base_idx) * step_months)
            out[bar['id']] = inferred.strftime("%B %Y")             # include year
    return out, True

resolved, ok = resolve_axis_labels(bars_in, labels_in)

# ---- 3. attach each VALUE to its resolved period ----
print("=== RESULT (value -> resolved period) ===")
bars_sorted = sorted(bars_in, key=lambda b: b.get('x_center', 0))
for bar in bars_sorted:
    period = resolved.get(bar['id'], "FLAG_REVIEW")
    printed = "printed" if any(
        min(bars_sorted, key=lambda b: abs(b.get('x_center',0)-l.get('x_center',0)))['id'] == bar['id']
        for l in labels_in) else "inferred"
    print(f"  {bar.get('series','?')} = {bar.get('value')}  ->  {period}  ({printed})")

if not ok:
    print("\n>>> FEWER THAN 2 PRINTED LABELS — stride could not be derived.")
    print(">>> These must be FLAGGED for review (cannot resolve safely).")
else:
    print("\n>>> CHECK: is each value attached to the CORRECT period?")
    print(">>> Printed labels kept as-is; missing first/last/middle inferred from")
    print(">>> the regular monthly stride. If correct, wire this into the pipeline.")
