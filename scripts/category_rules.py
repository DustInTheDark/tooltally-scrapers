# scripts/category_rules.py
"""
Category normalisation rules for ToolTally.

Goal: map vendor-specific/marketing categories to stable, overarching families
so search like "drill" returns all drills (cordless, combi, SDS) and so on.

Approach:
- We use both raw category string and the product title.
- We match specific families first (ordered), then fall back to broader power tool buckets,
  then hand tools, then Accessories/Other.
- Matching is simple/fast: lowercase, whole-word-ish contains, with a few multiword phrases.

Extend/adjust keywords over time as needed.
"""

import re

# Order matters: earlier families are tested first
FAMILY_RULES = [
    # --- Highly specific power tools first ---
    ("Impact Wrenches", [
        r"impact\s+wrench", r"\bwrenc?h\b",
    ]),
    ("Impact Drivers", [
        r"impact\s+driver", r"\bid\b", r"\btid\b", r"\bdtd\b",
    ]),
    ("Combi Drills", [
        r"combi\s+drill", r"hammer\s+drill", r"percussion\s+drill",
        r"\bdhp\b", r"\bdcd\b", r"\bgsb\b",
    ]),
    ("SDS Drills", [
        r"\bsds\b", r"sds-\w+", r"rotary\s+hammer",
        r"\bdhr\b", r"\bdch\b",
    ]),
    ("Drills", [
        r"\bdrill\b", r"drill\s+driver",
    ]),
    ("Angle Grinders", [
        r"angle\s+grinder", r"\bgrinder\b", r"\bdga\b", r"\bgws\b",
    ]),
    ("Circular Saws", [
        r"circular\s+saw", r"\bcs\b", r"\bdhs\b", r"\bdcs\b",
    ]),
    ("Jigsaws", [
        r"\bjig\s*saw\b", r"\bjigsaw\b", r"\bdtj\b", r"\bdcs?j\b",
    ]),
    ("Mitre Saws", [
        r"mitre\s+saw", r"miter\s+saw", r"\bms\b", r"\bdhs?m\b",
    ]),
    ("Recip Saws", [
        r"reciprocating\s+saw", r"\brecip\b", r"sabre\s+saw", r"\bdjr\b",
    ]),
    ("Multi Tools", [
        r"multi[-\s]?tool", r"\bot\b", r"\bmt\b",
    ]),
    ("Sanders", [
        r"sande?r", r"random\s+orbit", r"\bbo\d{3}\b", r"\bros\b",
    ]),
    ("Routers & Trimmers", [
        r"\brouter\b", r"trimmer\s*router", r"\brt\d+", r"\bgo\d+",
    ]),
    ("Planers", [
        r"\bplaner\b", r"\bkp\d+\b",
    ]),
    ("Heat Guns", [
        r"heat\s+gun", r"\bhg\d+\b",
    ]),
    ("Nailers", [
        r"\bnailer\b", r"\bfn\d+\b", r"\bdn\d+\b", r"finish\s*nailer", r"brad\s*nailer",
    ]),
    ("Staplers", [
        r"\bstapler\b",
    ]),
    ("Batteries & Chargers", [
        r"\bbattery\b", r"\bbatteries\b", r"\bcharger\b", r"\bpowerstack\b",
        r"\bmakpac\s*charger\b", r"\bdc\d{2,}\b",
    ]),
    ("Lighting & Torches", [
        r"\bwork\s*light\b", r"\btorch\b", r"\blamp\b",
    ]),
    ("Measuring & Lasers", [
        r"\blaser\b", r"\bdist(ance)?\s*measure", r"\bdw\d{3}\b", r"\bldm\b",
    ]),
    ("Dust Extractors & Vacuums", [
        r"dust\s*(extract(or)?|extraction)", r"\bvac(uum)?\b", r"\bwet\s*dry\b",
    ]),
    ("Storage & Cases", [
        r"\bcase\b", r"\bbox\b", r"\bbag\b", r"\bmakpac\b", r"\bt[- ]?stak\b", r"\bpackout\b",
    ]),
    ("Radios", [
        r"\bradio\b",
    ]),
    ("Pressure Washers", [
        r"pressure\s*washer",
    ]),
    ("Garden Tools", [
        r"lawn\s*mower", r"hedge\s*trimmer", r"grass\s*trimmer", r"strimmer", r"leaf\s*blower",
    ]),

    # --- Hand tools (generic buckets) ---
    ("Hand Saws", [
        r"\bhand\s*saw\b", r"\bhacksaw\b", r"\btenon\s*saw\b", r"\bjunior\s*hacksaw\b", r"\bcoping\s*saw\b",
    ]),
    ("Hammers", [
        r"\bhammer\b", r"claw\s*hammer", r"ball\s*pein", r"club\s*hammer", r"sledge\s*hammer", r"dead-?blow",
    ]),
    ("Screwdrivers", [
        r"\bscrewdriver\b", r"\bpozidriv\b", r"\bphillips\b",
    ]),
    ("Pliers & Cutters", [
        r"\bplier\b", r"\bcutter\b", r"\bside\s*cutter\b", r"\bwire\s*cutter\b", r"snip",
    ]),
    ("Wrenches & Sockets", [
        r"\bsocket\b", r"\bratchet\b", r"\bspanner\b", r"\bwrench\b",
    ]),
    ("Knives & Blades", [
        r"\bknife\b", r"\bblade\b", r"\bstanley\s*knife\b", r"\butility\s*knife\b",
    ]),
    ("Files & Rasps", [
        r"\bfile\b", r"\brasp\b",
    ]),
    ("Tapes & Measures", [
        r"\btape\s*measure\b", r"\bmeasuring\s*tape\b",
    ]),
    ("Levels", [
        r"\blevel\b", r"\bspirit\s*level\b",
    ]),

    # --- Accessories as catch-all before Other ---
    ("Accessories", [
        r"\bbit(s)?\b", r"\bblade(s)?\b", r"\bdisc(s)?\b", r"\bsand(ing)?\s*sheet(s)?\b",
        r"\bset\b", r"\bpack\b",
    ]),
]

# Precompile regexes
FAMILY_RULES_COMPILED = [(family, [re.compile(pat, re.I) for pat in pats]) for family, pats in FAMILY_RULES]

def normalise_category(raw_category: str | None, title: str | None) -> str:
    """
    Map (raw_category, title) to a normalised family name.
    Returns one of the family labels above, else a tidied Title-Case of the raw category, else "Other".
    """
    cat = (raw_category or "").strip().lower()
    text = f"{cat} {(title or '')}".strip().lower()

    # Try in priority order
    for family, regexes in FAMILY_RULES_COMPILED:
        for rx in regexes:
            if rx.search(text):
                return family

    # No family hit: fall back to tidied raw category if present
    if cat:
        return cat.title()

    return "Other"
