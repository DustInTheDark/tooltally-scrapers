# scripts/util_identifiers.py
"""
Utilities to extract EAN/GTIN and MPN from product pages.

Supports two modes:
- Scrapy mode:   extract_identifiers(response)          -> dict(ean_gtin, mpn)
- HTML-only mode:extract_identifiers_from_html(html)    -> dict(ean_gtin, mpn)
"""
from __future__ import annotations
import json, re
from typing import Dict

EAN_KEYS = {"gtin", "gtin13", "gtin12", "ean", "ean13", "upc"}
MPN_KEYS = {"mpn", "manufacturerpartnumber", "manufacturer_part_number", "model"}

LABEL_MAP = {
    "ean": "ean", "gtin": "ean", "gtin-13": "ean", "upc": "ean",
    "mpn": "mpn", "manufacturer part number": "mpn",
    "manufacturer model no": "mpn", "manufacturer model number": "mpn",
    "model": "mpn", "product code": "mpn", "sku": "mpn"
}

def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())

def _first_val(d: dict, keys: set[str]) -> str:
    for k in keys:
        if k in d and d[k]:
            v = d[k]
            if isinstance(v, (list, tuple)): v = v[0]
            return _clean(str(v))
    return ""

def _scan_jsonld_data(data, out: Dict[str, str]) -> None:
    if not isinstance(data, dict): return
    norm = {str(k).lower(): v for k, v in data.items()}
    out["ean_gtin"] = out.get("ean_gtin") or _first_val(norm, EAN_KEYS)
    out["mpn"]      = out.get("mpn")      or _first_val(norm, MPN_KEYS)

def extract_from_jsonld_html(html_text: str) -> dict:
    out = {"ean_gtin": "", "mpn": ""}
    if not html_text: return out
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html_text, re.I | re.S
    ):
        block = m.group(1)
        candidates = [block] + re.findall(r"\{.*?\}", block, re.S)
        for cand in candidates:
            try:
                data = json.loads(cand)
            except Exception:
                continue
            if isinstance(data, dict):
                _scan_jsonld_data(data, out)
                for v in list(data.values()):
                    if isinstance(v, dict): _scan_jsonld_data(v, out)
                    elif isinstance(v, list):
                        for o in v:
                            if isinstance(o, dict): _scan_jsonld_data(o, out)
            elif isinstance(data, list):
                for obj in data:
                    if isinstance(obj, dict): _scan_jsonld_data(obj, out)
    return out

def extract_from_labels_html(html_text: str) -> dict:
    out = {"ean_gtin": "", "mpn": ""}
    if not html_text: return out
    html = re.sub(r"\s+", " ", html_text)
    candidates = []
    for m in re.finditer(r"<tr[^>]*>\s*<th[^>]*>(.*?)</th>\s*<td[^>]*>(.*?)</td>.*?</tr>", html, re.I):
        candidates.append((m.group(1), m.group(2)))
    for m in re.finditer(r"<tr[^>]*>\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>.*?</tr>", html, re.I):
        candidates.append((m.group(1), m.group(2)))
    for m in re.finditer(r"<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>", html, re.I):
        candidates.append((m.group(1), m.group(2)))
    for m in re.finditer(r"<li[^>]*>\s*([^:<>{}]{2,30})\s*:\s*([^<>{}]{1,120})\s*</li>", html, re.I):
        candidates.append((m.group(1), m.group(2)))
    for raw_label, raw_val in candidates:
        l = _clean(re.sub("<.*?>", "", raw_label)).lower()
        v = _clean(re.sub("<.*?>", "", raw_val))
        for key, which in LABEL_MAP.items():
            if key in l and v:
                if which == "ean" and not out["ean_gtin"]: out["ean_gtin"] = v
                if which == "mpn" and not out["mpn"]:      out["mpn"]      = v
    return out

def extract_identifiers_from_html(html_text: str) -> dict:
    ids = extract_from_jsonld_html(html_text)
    if not ids.get("ean_gtin") or not ids.get("mpn"):
        fb = extract_from_labels_html(html_text)
        ids["ean_gtin"] = ids.get("ean_gtin") or fb.get("ean_gtin", "")
        ids["mpn"]      = ids.get("mpn")      or fb.get("mpn", "")
    return ids

def extract_identifiers(response) -> dict:
    ids = extract_from_jsonld_html(response.text or "")
    if ids.get("ean_gtin") and ids.get("mpn"): return ids
    ean, mpn = ids.get("ean_gtin") or "", ids.get("mpn") or ""
    candidates = []
    for tr in response.xpath("//tr"):
        th = " ".join(tr.xpath("./th//text()").getall()).strip()
        td1 = " ".join(tr.xpath("./td[1]//text()").getall()).strip()
        td2 = " ".join(tr.xpath("./td[2]//text()").getall()).strip()
        if th:
            val = " ".join(tr.xpath("./td//text()").getall()).strip()
            if val: candidates.append((th, val))
        elif td1 and td2: candidates.append((td1, td2))
    for dt in response.xpath("//dt"):
        label = " ".join(dt.xpath(".//text()").getall()).strip()
        value = " ".join(dt.xpath("following-sibling::dd[1]//text()").getall()).strip()
        if label and value: candidates.append((label, value))
    for li in response.xpath("//li[contains(.,':')]"):
        text = " ".join(li.xpath(".//text()").getall()).strip()
        if ":" in text:
            label, value = text.split(":", 1)
            candidates.append((label.strip(), value.strip()))
    for raw_label, raw_val in candidates:
        l = _clean(raw_label).lower()
        v = _clean(raw_val)
        for key, which in LABEL_MAP.items():
            if key in l and v:
                if which == "ean" and not ean: ean = v
                if which == "mpn" and not mpn: mpn = v
    return {"ean_gtin": ean, "mpn": mpn}
