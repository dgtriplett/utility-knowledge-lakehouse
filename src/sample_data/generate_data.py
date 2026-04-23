"""Generate synthetic utility documents for the sample pipeline.

Produces three kinds of content:

  1. Substation one-line summary PDFs (~50)
  2. Protection coordination study PDFs (~30)
  3. SME debrief transcripts as .txt + (optionally) generated WAV placeholders

None of this data is derived from any real utility. Equipment IDs,
substation names, and engineer names are deterministically generated from a
seed so runs are reproducible.

Usage:
    python -m src.sample_data.generate_data --out ./sample_data
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import string
from dataclasses import asdict, dataclass
from pathlib import Path

from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from reportlab.lib import colors


SEED = 424242
VOLTAGE_CLASSES = [12.47, 34.5, 69.0, 115.0, 138.0, 230.0, 345.0, 500.0]
REGIONS = ["NE", "SE", "MW", "NW", "SW"]
SUBSTATION_PREFIXES = [
    "Oak Ridge", "Pine Hollow", "Beaver Creek", "Silver Falls", "Red Butte",
    "Mill Creek", "Cedar Grove", "Granite Pass", "Blackwater", "North Fork",
    "East Meadow", "Sunridge", "Ironwood", "Whitestone", "Copper Gulch",
]

# Hardcoded to avoid a faker dependency — any ~20 names will do.
LAST_NAMES = [
    "Acevedo", "Benson", "Chen", "Delgado", "Ellis", "Fitzgerald", "Gupta",
    "Hoffman", "Ibrahim", "Jensen", "Kowalski", "Lindqvist", "Mendoza",
    "Nakamura", "Okonkwo", "Petrov", "Quintero", "Ramirez", "Suzuki",
    "Thompson", "Umar", "Vargas", "Whitfield", "Yamamoto", "Zhao",
]


@dataclass
class Substation:
    name: str
    region: str
    voltage_class_kv: float
    num_breakers: int
    commissioning_year: int


@dataclass
class Breaker:
    equipment_id: str
    substation: str
    voltage_class_kv: float
    manufacturer: str
    install_year: int


def _rng(seed_extra: str = "") -> random.Random:
    return random.Random(f"{SEED}-{seed_extra}")


def _engineer_name(rng: random.Random) -> str:
    last = rng.choice(LAST_NAMES)
    first_initial = rng.choice(string.ascii_uppercase)
    return f"{first_initial}. {last}, P.E."


def _substations(n: int) -> list[Substation]:
    rng = _rng("substations")
    out = []
    for i in range(n):
        base = rng.choice(SUBSTATION_PREFIXES)
        suffix = rng.choice(["", " North", " South", " East", " West", f" {i}"])
        out.append(
            Substation(
                name=f"{base}{suffix}".strip(),
                region=rng.choice(REGIONS),
                voltage_class_kv=rng.choice(VOLTAGE_CLASSES),
                num_breakers=rng.randint(4, 18),
                commissioning_year=rng.randint(1962, 2019),
            )
        )
    return out


def _breakers_for(sub: Substation) -> list[Breaker]:
    rng = _rng(f"breakers-{sub.name}")
    manufacturers = ["ABB", "GE", "Siemens", "Eaton", "Mitsubishi", "Hitachi Energy"]
    out = []
    for i in range(sub.num_breakers):
        eq_id = f"{int(sub.voltage_class_kv)}{rng.choice('LBCT')}-{i + 1}"
        out.append(
            Breaker(
                equipment_id=eq_id,
                substation=sub.name,
                voltage_class_kv=sub.voltage_class_kv,
                manufacturer=rng.choice(manufacturers),
                install_year=rng.randint(sub.commissioning_year, 2024),
            )
        )
    return out


def _write_oneline(sub: Substation, breakers: list[Breaker], out_path: Path) -> None:
    rng = _rng(f"oneline-{sub.name}")
    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(str(out_path), pagesize=LETTER, title=f"{sub.name} One-Line Summary")
    flow = []

    flow.append(Paragraph(f"<b>{sub.name} Substation — One-Line Summary</b>", styles["Title"]))
    flow.append(Spacer(1, 12))
    flow.append(
        Paragraph(
            f"Region: {sub.region}  |  Nominal voltage: {sub.voltage_class_kv:.2f} kV  |  "
            f"Commissioned: {sub.commissioning_year}",
            styles["Normal"],
        )
    )
    flow.append(Spacer(1, 12))
    flow.append(
        Paragraph(
            "This document summarizes the as-built one-line configuration for the station. "
            "It supersedes prior revisions. Clearance coordination and protection settings "
            "are covered in the companion Protection Coordination Study.",
            styles["Normal"],
        )
    )
    flow.append(Spacer(1, 12))

    rows = [["Equipment ID", "Voltage Class (kV)", "Manufacturer", "Install Year"]]
    for br in breakers:
        rows.append([br.equipment_id, f"{br.voltage_class_kv:.2f}", br.manufacturer, str(br.install_year)])

    table = Table(rows, hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
            ]
        )
    )
    flow.append(table)
    flow.append(Spacer(1, 18))

    flow.append(Paragraph("<b>Notes</b>", styles["Heading3"]))
    notes = [
        f"Bus arrangement is {rng.choice(['ring', 'breaker-and-a-half', 'double-bus, single-breaker'])}.",
        f"SCADA RTU is {rng.choice(['SEL-3555', 'GE D400', 'Novatech OrionLX'])}; polling every "
        f"{rng.choice([2, 4, 5])} seconds.",
        f"Last full thermal inspection: {rng.randint(2019, 2025)}.",
    ]
    for n in notes:
        flow.append(Paragraph(f"• {n}", styles["Normal"]))

    flow.append(Spacer(1, 18))
    flow.append(
        Paragraph(
            f"Approved: {_engineer_name(rng)} &nbsp;&nbsp; Date: "
            f"{rng.randint(1, 28)}/{rng.randint(1, 12)}/{rng.randint(2019, 2025)}",
            styles["Normal"],
        )
    )
    doc.build(flow)


def _write_study(sub: Substation, breakers: list[Breaker], out_path: Path) -> None:
    rng = _rng(f"study-{sub.name}")
    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(str(out_path), pagesize=LETTER, title=f"{sub.name} Protection Study")
    flow = []

    flow.append(Paragraph(f"<b>Protection Coordination Study — {sub.name}</b>", styles["Title"]))
    flow.append(Spacer(1, 12))
    flow.append(
        Paragraph(
            f"Study date: {rng.randint(1, 28)}/{rng.randint(1, 12)}/{rng.randint(2015, 2025)}<br/>"
            f"Prepared by: {_engineer_name(rng)}<br/>"
            f"Scope: {sub.voltage_class_kv:.0f} kV protection settings for all feeder breakers, "
            f"including overcurrent, reclosing, and backup distance elements.",
            styles["Normal"],
        )
    )
    flow.append(Spacer(1, 12))

    flow.append(Paragraph("<b>Settings Table</b>", styles["Heading3"]))
    rows = [["Equipment", "51P Pickup (A)", "51P Time Dial", "50P Pickup (A)", "Reclose (s)"]]
    for br in breakers:
        rows.append(
            [
                br.equipment_id,
                str(rng.randint(200, 1200)),
                f"{rng.uniform(0.5, 4.0):.2f}",
                str(rng.randint(2000, 8000)),
                ", ".join(str(x) for x in rng.sample([2, 5, 15, 30, 60, 120], 3)),
            ]
        )
    table = Table(rows, hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
            ]
        )
    )
    flow.append(table)
    flow.append(Spacer(1, 14))

    flow.append(Paragraph("<b>Engineering Rationale</b>", styles["Heading3"]))
    chosen = rng.sample(breakers, min(3, len(breakers)))
    for br in chosen:
        rationale = rng.choice(
            [
                f"Kept {br.equipment_id} at the existing time dial to preserve coordination with the "
                "downstream recloser; previous attempt to tighten this resulted in nuisance trips during "
                "cold-load pickup in winter 2021.",
                f"Reduced {br.equipment_id} 50P pickup after fault study showed arc-flash incident energy "
                "exceeded 8 cal/cm² at the downstream cabinet.",
                f"Left {br.equipment_id} reclose sequence unchanged — customer complaints about momentary "
                "interruptions traced to downstream issues, not relay logic.",
                f"Flagged {br.equipment_id} for re-study in 3 years; feeder growth projections indicate "
                "likely re-rating needed before 2028.",
            ]
        )
        flow.append(Paragraph(f"• {rationale}", styles["Normal"]))
        flow.append(Spacer(1, 4))

    flow.append(Spacer(1, 18))
    flow.append(
        Paragraph(
            f"Approved: {_engineer_name(rng)} &nbsp;&nbsp; Date: "
            f"{rng.randint(1, 28)}/{rng.randint(1, 12)}/{rng.randint(2020, 2025)}",
            styles["Normal"],
        )
    )
    doc.build(flow)


DEBRIEF_TEMPLATES = [
    # Each debrief is a scenario template; we fill in equipment and substation from the corpus.
    (
        "relay_coordination",
        (
            "So, about the protection settings at {sub} — the thing people miss is that we tried "
            "tightening the 51P on {eq} back in maybe 2019, and we got burned in January when the "
            "load picked up after a cold snap. It miscoordinated with the downstream recloser twice "
            "in one week. Since then we've left the time dial where it is even though the study "
            "keeps flagging it. The reason isn't on paper anywhere — it's just that when you see a "
            "settings recommendation that tight on that feeder, you check with operations first. "
            "If I were leaving tomorrow, that's the thing I'd want the next person to know."
        ),
    ),
    (
        "transformer_oil",
        (
            "The {sub} transformer — the main bank — had a DGA spike in 2022 that everyone thought "
            "was the start of an incipient fault. We pulled samples, ran them three times, and it "
            "turned out to be a sampling issue. The valve itself was entraining air. We replaced the "
            "sampling valve and the numbers went back to normal within a month. But the replaced-valve "
            "story isn't in the maintenance record because it wasn't officially a fault. So if someone "
            "looks at the 2022 numbers without that context they'll think we missed something big. "
            "We didn't."
        ),
    ),
    (
        "switching_quirk",
        (
            "If you ever have to switch {eq} out of service, the interlock on the disconnect is "
            "wired backwards relative to the one-line. I don't know why. It's been that way since "
            "the 1998 rebuild. The procedure is right — people just need to follow it, not the "
            "drawing. We've tried to get that corrected three times and it keeps getting "
            "deprioritized. I've got a note taped to the switchgear panel but obviously that's not "
            "a real fix."
        ),
    ),
    (
        "relay_firmware",
        (
            "The {sub} relays running firmware rev 1.7 have a known quirk where the event record "
            "timestamp drifts about 200 ms per day if the GPS sync drops. It comes back when GPS "
            "reconnects but the event records from that period are unreliable for sequence-of-events "
            "analysis. We've been meaning to upgrade to 1.9 for three years. The workaround is to "
            "cross-check with the SCADA event log for anything that matters."
        ),
    ),
    (
        "customer_history",
        (
            "There's a manufacturing customer on the {sub} feeder — their process is sensitive to "
            "voltage sags below about 85%. We don't fault-ride-through it, we just have a running "
            "understanding that before any planned switching on that feeder we call their shift "
            "supervisor 24 hours ahead. It's not in the SOP. It's a handshake from 2014 when they "
            "threatened to take the load off-grid. Next person in this seat needs to know that "
            "relationship exists."
        ),
    ),
]


def _write_debrief(sub: Substation, eq: str, template_key: str, template: str, out_path: Path) -> str:
    rng = _rng(f"debrief-{sub.name}-{template_key}")
    text = template.format(sub=sub.name, eq=eq)
    header = (
        f"Retirement debrief transcript\n"
        f"Interviewee: {rng.choice(['Senior Engineer', 'Lead Protection Specialist', 'Operations Supervisor'])}\n"
        f"Date: 2026-0{rng.randint(1, 4)}-{rng.randint(10, 28)}\n"
        f"Topic: {template_key.replace('_', ' ').title()}\n"
        f"Interviewer: Knowledge Capture Team\n"
        f"---\n\n"
    )
    content = header + text + "\n"
    out_path.write_text(content, encoding="utf-8")
    return content


def generate(out_dir: str | Path, n_substations: int = 15, n_debriefs: int = 20) -> dict:
    """Generate the full synthetic corpus into out_dir.

    Importable from a notebook as well as callable from the CLI. Returns a
    summary dict with counts and the manifest path.
    """
    out = Path(out_dir).resolve()
    (out / "onelines").mkdir(parents=True, exist_ok=True)
    (out / "studies").mkdir(parents=True, exist_ok=True)
    (out / "debriefs").mkdir(parents=True, exist_ok=True)

    subs = _substations(n_substations)
    all_breakers: list[Breaker] = []
    manifest: list[dict] = []

    for sub in subs:
        breakers = _breakers_for(sub)
        all_breakers.extend(breakers)

        safe = sub.name.replace(" ", "_").lower()

        oneline_path = out / "onelines" / f"{safe}_oneline.pdf"
        _write_oneline(sub, breakers, oneline_path)
        manifest.append({"path": str(oneline_path), "type": "oneline", "substation": sub.name})

        study_path = out / "studies" / f"{safe}_protection_study.pdf"
        _write_study(sub, breakers, study_path)
        manifest.append({"path": str(study_path), "type": "protection_study", "substation": sub.name})

    rng = _rng("debriefs")
    for i in range(n_debriefs):
        sub = rng.choice(subs)
        eq = rng.choice([b.equipment_id for b in all_breakers if b.substation == sub.name])
        key, template = rng.choice(DEBRIEF_TEMPLATES)
        debrief_path = out / "debriefs" / f"debrief_{i:03d}_{key}.txt"
        _write_debrief(sub, eq, key, template, debrief_path)
        manifest.append(
            {"path": str(debrief_path), "type": "debrief", "substation": sub.name, "topic": key}
        )

    manifest_path = out / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))

    return {
        "n_substations": len(subs),
        "n_breakers": len(all_breakers),
        "n_onelines": len([m for m in manifest if m["type"] == "oneline"]),
        "n_studies": len([m for m in manifest if m["type"] == "protection_study"]),
        "n_debriefs": len([m for m in manifest if m["type"] == "debrief"]),
        "manifest_path": str(manifest_path),
    }


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic utility documents.")
    parser.add_argument("--out", dest="out_dir", default="./sample_data", help="Output directory")
    parser.add_argument("--n-substations", type=int, default=15)
    parser.add_argument("--n-debriefs", type=int, default=20)
    args = parser.parse_args()

    summary = generate(args.out_dir, args.n_substations, args.n_debriefs)
    print(
        f"Generated {summary['n_substations']} substations, {summary['n_breakers']} breakers."
    )
    print(
        f"Wrote {summary['n_onelines']} one-line PDFs, "
        f"{summary['n_studies']} protection studies, "
        f"{summary['n_debriefs']} debrief transcripts."
    )
    print(f"Manifest: {summary['manifest_path']}")


if __name__ == "__main__":
    _cli()
