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



