#!/usr/bin/env python3
"""
Generate stress-test invoices (PDF), bank statements (PDF), and GL CSV for revenue testing.

Outputs (under test_documents/stress_test/):
  - 100 invoice PDFs, layouts A–L in round-robin (~8–9 of each type)
  - 8 remittance-style PDFs (type I) use filenames containing 'remittance'
  - 3 bank statement PDFs (40 lines each) with mixed receipt / fee transactions
  - stress_test_gl.csv (100 rows; 8 amounts > $500k ex GST; 10 dates near 30/06/2024)

Usage:  python create_stress_test_documents.py

Requires: reportlab (see requirements.txt)
"""

from __future__ import annotations

import csv
import random
import sys
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas
from reportlab.platypus import (
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CUSTOMERS = [
    "ABC Technology Pty Ltd",
    "XYZ Services Pty Ltd",
    "Global Manufacturing Co",
    "Health Solutions Ltd",
    "Fintech Partners Pty Ltd",
    "Metro Construction Pty Ltd",
    "Digital Innovations Co",
    "Pacific Retail Group",
    "Northern Biotech Ltd",
    "Community Services NFP",
    "Coastal Engineering Pty Ltd",
    "Alpine Software Co",
    "Meridian Health Group",
    "Summit Legal Services",
    "Harbour Consulting Pty Ltd",
]

LAYOUT_TYPES = list("ABCDEFGHIJKL")  # A–L

OUT_DIR = Path(__file__).resolve().parent / "test_documents" / "stress_test"

SEED = 42


def money_fmt(n: float) -> str:
    return f"${n:,.2f}"


def ex_from_inc(inc: float) -> float:
    return round(inc / 1.1, 2)


def build_styles():
    styles = getSampleStyleSheet()
    styles.add(
        ParagraphStyle(
            name="Small",
            parent=styles["Normal"],
            fontSize=8,
            leading=10,
        )
    )
    styles.add(
        ParagraphStyle(
            name="Right",
            parent=styles["Normal"],
            alignment=TA_RIGHT,
        )
    )
    styles.add(
        ParagraphStyle(
            name="TitleDoc",
            parent=styles["Heading1"],
            fontSize=16,
            spaceAfter=12,
        )
    )
    return styles


def invoice_number(i: int) -> str:
    return f"INV-2024-{i:03d}"


def truncate_desc(s: str, n: int = 18) -> str:
    s = s.replace(" Pty Ltd", "").replace(" Ltd", "").strip()
    return s[:n].upper() if len(s) > n else s.upper()


# ---------------------------------------------------------------------------
# Invoice PDF builders (return list of Flowables)
# ---------------------------------------------------------------------------


def _table_line_items(rows: list[list[str]], col_widths=None):
    t = Table(rows, colWidths=col_widths, repeatRows=1)
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0F2A52")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    return t


def build_type_a(
    styles,
    inv: str,
    customer: str,
    inv_date: date,
    inc: float,
    ex: float,
    gst: float,
) -> list:
    story = [
        Paragraph("<b>ACME SUPPLY CO PTY LTD</b><br/>ABN 12 345 678 901<br/>1 Industrial Way, Sydney NSW", styles["Normal"]),
        Spacer(1, 0.4 * cm),
        Paragraph(f"<b>Tax Invoice {inv}</b>", styles["TitleDoc"]),
        Paragraph(f"<para align=right>Date: {inv_date.strftime('%d/%m/%Y')}</para>", styles["Right"]),
        Spacer(1, 0.3 * cm),
        Paragraph(f"<b>Bill To:</b><br/>{customer}", styles["Normal"]),
        Spacer(1, 0.5 * cm),
    ]
    rows = [
        ["Description", "Qty", "Unit", "Amount"],
        ["Professional services — consulting", "1", money_fmt(ex * 0.6), money_fmt(ex * 0.6)],
        ["Software licence fee", "1", money_fmt(ex * 0.4), money_fmt(ex * 0.4)],
    ]
    story.append(_table_line_items(rows))
    story += [
        Spacer(1, 0.4 * cm),
        Paragraph(f"Subtotal (ex GST): {money_fmt(ex)}", styles["Right"]),
        Paragraph(f"GST: {money_fmt(gst)}", styles["Right"]),
        Paragraph(f"<b>TOTAL {money_fmt(inc)} inc GST</b>", styles["Right"]),
    ]
    return story


def build_type_b(styles, inv, customer, inv_date, inc, ex, gst) -> list:
    story = [
        Paragraph(f"<b><font size=14>Tax Invoice</font></b> — {inv}", styles["Normal"]),
        Paragraph(f"Customer: {customer}", styles["Normal"]),
        Spacer(1, 0.3 * cm),
        Paragraph(f"<b>Amount Due {money_fmt(inc)}</b> (including GST)", styles["Normal"]),
        Spacer(1, 0.2 * cm),
        Paragraph(f"Tax (10%): {money_fmt(gst)}", styles["Normal"]),
        Paragraph(f"Net (ex GST): {money_fmt(ex)}", styles["Normal"]),
        Spacer(1, 0.5 * cm),
        Paragraph(f"Issue date: {inv_date.strftime('%d/%m/%Y')}", styles["Small"]),
        Spacer(1, 1 * cm),
        Paragraph("<i>ACME SUPPLY CO PTY LTD — ABN 12 345 678 901</i>", styles["Small"]),
    ]
    return story


def build_type_c(styles, inv, customer, inv_date, inc, ex, gst) -> list:
    parts = ex / 6
    rows = [
        ["Part #", "Description", "Qty", "Unit $", "Line $"],
        ["P-1001", "Widget assembly A", "12", money_fmt(parts / 12), money_fmt(parts)],
        ["P-1002", "Bracket steel grade 304", "40", money_fmt(parts / 10), money_fmt(parts)],
        ["P-2099", "Hydraulic seal kit", "6", money_fmt(parts / 6), money_fmt(parts)],
        ["—", "Delivery ex works — FOT", "1", money_fmt(parts), money_fmt(parts)],
        ["—", "Packing / handling", "1", money_fmt(parts), money_fmt(parts)],
        ["—", "Engineering review", "4", money_fmt(parts / 4), money_fmt(parts)],
    ]
    story = [
        Paragraph("<b>GLOBAL PARTS MANUFACTURING</b>", styles["Normal"]),
        Paragraph(f"Invoice {inv} &nbsp; Date {inv_date.strftime('%d/%m/%Y')}", styles["Normal"]),
        Paragraph(f"Deliver To: {customer}", styles["Normal"]),
        Spacer(1, 0.3 * cm),
        _table_line_items(rows, [2 * cm, 5 * cm, 1.5 * cm, 2 * cm, 2 * cm]),
        Spacer(1, 0.3 * cm),
        Paragraph(f"Subtotal ex GST {money_fmt(ex)} | GST {money_fmt(gst)} | <b>Total {money_fmt(inc)}</b>", styles["Normal"]),
        Paragraph("Terms: Net 30. Delivery: Incoterms EXW.", styles["Small"]),
    ]
    return story


def build_type_d(styles, inv, customer, inv_date, inc, ex, gst) -> list:
    rows = [
        ["Resource", "Hours", "Rate", "Amount"],
        ["S. Lee — Partner", "12.5", money_fmt(ex * 0.35 / 12.5), money_fmt(ex * 0.35)],
        ["J. Smith — Senior", "22.0", money_fmt(ex * 0.30 / 22), money_fmt(ex * 0.30)],
        ["A. Kim — Analyst", "35.0", money_fmt(ex * 0.35 / 35), money_fmt(ex * 0.35)],
    ]
    story = [
        Paragraph("<b>SUMMIT LEGAL — Professional Fees</b>", styles["Normal"]),
        Paragraph(f"Matter: MT-{inv[-3:]}-2024 &nbsp; Billing period: {(inv_date.replace(day=1)).strftime('%b %Y')}", styles["Small"]),
        Paragraph(f"Client: {customer}", styles["Normal"]),
        Paragraph(f"Invoice {inv} &nbsp; {inv_date.strftime('%d %b %Y')}", styles["Normal"]),
        Spacer(1, 0.3 * cm),
        _table_line_items(rows),
        Spacer(1, 0.2 * cm),
        Paragraph(f"Fees ex GST {money_fmt(ex)} | GST {money_fmt(gst)} | Total {money_fmt(inc)}", styles["Normal"]),
    ]
    return story


def build_type_e(styles, inv, customer, inv_date, inc, ex, gst) -> list:
    period_end = inv_date
    period_start = period_end - timedelta(days=29)
    story = [
        Paragraph("<b>ALPINE SOFTWARE — Subscription Invoice</b>", styles["Normal"]),
        Paragraph(f"Plan: Enterprise Suite — Invoice {inv}", styles["Normal"]),
        Paragraph(f"Customer: {customer}", styles["Normal"]),
        Spacer(1, 0.2 * cm),
        Paragraph(
            f"Service period: {period_start.strftime('%d/%m/%Y')} – {period_end.strftime('%d/%m/%Y')}",
            styles["Normal"],
        ),
        Paragraph(f"Monthly fee (prorated where applicable) ex GST: {money_fmt(ex)}", styles["Normal"]),
        Paragraph(f"GST 10%: {money_fmt(gst)}", styles["Normal"]),
        Paragraph(f"<b>Amount payable {money_fmt(inc)}</b>", styles["Normal"]),
    ]
    return story


def build_type_f(styles, inv, customer, inv_date, inc, ex, gst) -> list:
    contract = ex * 8
    prev_claimed = contract * 0.35
    this_claim = ex
    story = [
        Paragraph("<b>PROGRESS CLAIM — Construction</b>", styles["Normal"]),
        Paragraph(f"Project: Riverside Plaza — Contract value {money_fmt(contract)} ex GST", styles["Normal"]),
        Paragraph(f"Claim #{inv[-3:]} &nbsp; Client: {customer}", styles["Normal"]),
        Spacer(1, 0.2 * cm),
        Paragraph(f"Milestone: Level 4 slab pour — % complete stated 62%", styles["Normal"]),
        Paragraph(f"Previously claimed (cumulative): {money_fmt(prev_claimed)} ex GST", styles["Normal"]),
        Paragraph(f"<b>This period claimed: {money_fmt(this_claim)} ex GST</b>", styles["Normal"]),
        Paragraph(f"GST {money_fmt(gst)} | Total due {money_fmt(inc)}", styles["Normal"]),
        Paragraph(f"Date: {inv_date.strftime('%d/%m/%Y')}", styles["Small"]),
    ]
    return story


def build_type_g(styles, inv, customer, inv_date, inc, ex, gst) -> list:
    story = [
        Paragraph("<b>invoice</b> &nbsp; " + inv, styles["Small"]),
        Spacer(1, 0.8 * cm),
        Paragraph(
            f"randomtext {customer[:10]}... amount buried here total was like {money_fmt(inc)} "
            f"including everything and gst portion approx {money_fmt(gst)} ok thanks",
            styles["Small"],
        ),
        Paragraph(f"ref {inv} date{inv_date.strftime('%d%m%y')}", styles["Small"]),
    ]
    return story


def build_type_h(styles, inv, customer, inv_date, inc, ex, gst) -> list:
    # MM/DD/YYYY display
    usd = inv_date.strftime("%m/%d/%Y")
    story = [
        Paragraph("<b>Invoice # " + inv + "</b>", styles["Normal"]),
        Paragraph(f"Invoice Date: {usd}", styles["Normal"]),
        Paragraph(f"Bill to: {customer}", styles["Normal"]),
        Paragraph(f"Tax ID: 98-7654321", styles["Small"]),
        Spacer(1, 0.3 * cm),
        Paragraph(f"Goods/services total ex tax: USD equivalent AUD {money_fmt(ex)}", styles["Normal"]),
        Paragraph(f"Tax: {money_fmt(gst)}", styles["Normal"]),
        Paragraph(f"Grand Total AUD {money_fmt(inc)}", styles["Normal"]),
    ]
    return story


def build_type_i(styles, inv, customer, inv_date, inc, gst, pool_refs: list[tuple[str, float]]) -> list:
    """Remittance listing multiple invoices."""
    story = [
        Paragraph("<b>Payment Advice / Remittance</b>", styles["Normal"]),
        Paragraph(f"From: {customer}", styles["Normal"]),
        Paragraph(f"Payment date: {inv_date.strftime('%d/%m/%Y')}", styles["Normal"]),
        Spacer(1, 0.2 * cm),
        Paragraph("<b>Invoices paid this run:</b>", styles["Normal"]),
    ]
    rows = [["Invoice ref", "Amount paid"]]
    for ref, amt in pool_refs:
        rows.append([ref, money_fmt(amt)])
    rows.append(["Total remittance", money_fmt(inc)])
    story.append(_table_line_items(rows, [6 * cm, 4 * cm]))
    story.append(Spacer(1, 0.2 * cm))
    story.append(Paragraph(f"Total includes GST components as applicable. Ref batch {inv}.", styles["Small"]))
    return story


def build_type_j(styles, inv, customer, inv_date, inc, ex, gst) -> list:
    orig = f"INV-2024-{max(1, int(inv.split('-')[-1]) - 5):03d}"
    story = [
        Paragraph("<b>CREDIT NOTE</b> CN-" + inv.split("-")[-1], styles["TitleDoc"]),
        Paragraph(f"Original invoice: {orig}", styles["Normal"]),
        Paragraph(f"Customer: {customer}", styles["Normal"]),
        Paragraph(f"Date: {inv_date.strftime('%d/%m/%Y')}", styles["Normal"]),
        Spacer(1, 0.3 * cm),
        Paragraph(f"Credit ex GST: ({money_fmt(abs(ex))})", styles["Normal"]),
        Paragraph(f"GST adjustment: ({money_fmt(abs(gst))})", styles["Normal"]),
        Paragraph(f"<b>Total credit {money_fmt(inc)}</b> (negative)", styles["Normal"]),
    ]
    return story


def build_type_k(styles, inv, customer, inv_date, inc, ex, gst) -> list:
    story = [
        Paragraph("<b>Milestone Invoice — Biotech R&D Agreement</b>", styles["Normal"]),
        Paragraph(f"Contract ref: CT-BIO-2023-014 clause 4.2 milestone payments", styles["Small"]),
        Paragraph(f"Milestone: Phase II interim data package — {customer}", styles["Normal"]),
        Paragraph(f"Achievement threshold: 70% enrolment target met (certified 14/{inv_date.strftime('%m')}/2024)", styles["Small"]),
        Paragraph(f"Payment trigger: milestone sign-off by steering committee", styles["Small"]),
        Paragraph(f"Fee ex GST {money_fmt(ex)} | GST {money_fmt(gst)} | <b>Pay {money_fmt(inc)}</b>", styles["Normal"]),
        Paragraph(f"Invoice {inv} dated {inv_date.strftime('%d/%m/%Y')}", styles["Small"]),
    ]
    return story


def build_type_l(styles, inv, customer, inv_date, inc, ex, gst) -> list:
    story = [
        Paragraph("<b>Grant Instalment Tax Invoice</b>", styles["Normal"]),
        Paragraph(f"Grant agreement: GA-NFP-2024-07 — Performance obligation: community programme delivery", styles["Small"]),
        Paragraph(f"Recipient: {customer}", styles["Normal"]),
        Paragraph(f"Instalment 3 of 4 — reporting period ended {inv_date.strftime('%d/%m/%Y')}", styles["Normal"]),
        Paragraph(f"Funding ex GST {money_fmt(ex)} | GST {money_fmt(gst)} | Total {money_fmt(inc)}", styles["Normal"]),
        Paragraph(f"Reference {inv}", styles["Small"]),
    ]
    return story


BUILDERS = {
    "A": build_type_a,
    "B": build_type_b,
    "C": build_type_c,
    "D": build_type_d,
    "E": build_type_e,
    "F": build_type_f,
    "G": build_type_g,
    "H": build_type_h,
    "I": build_type_i,
    "J": build_type_j,
    "K": build_type_k,
    "L": build_type_l,
}


def write_invoice_pdf(
    path: Path,
    layout: str,
    idx: int,
    customer: str,
    inv_date: date,
    inc: float,
    ex: float,
    gst: float,
    rng: random.Random,
    remittance_lines: list[tuple[str, float]] | None,
) -> None:
    styles = build_styles()
    inv = invoice_number(idx)
    story: list

    if layout == "I" and remittance_lines:
        story = build_type_i(styles, inv, customer, inv_date, inc, gst, remittance_lines)
    elif layout == "J":
        story = build_type_j(styles, inv, customer, inv_date, inc, ex, gst)
    else:
        fn = BUILDERS[layout]
        if layout == "I":
            # fallback if no lines
            story = build_type_i(
                styles,
                inv,
                customer,
                inv_date,
                inc,
                gst,
                [(inv, inc)],
            )
        else:
            story = fn(styles, inv, customer, inv_date, inc, ex, gst)

    doc = SimpleDocTemplate(str(path), pagesize=A4, rightMargin=2 * cm, leftMargin=2 * cm, topMargin=2 * cm, bottomMargin=2 * cm)
    doc.build(story)


# ---------------------------------------------------------------------------
# Bank statement PDF (canvas)
# ---------------------------------------------------------------------------


def write_bank_statement_pdf(path: Path, title: str, transactions: list[dict]) -> None:
    c = canvas.Canvas(str(path), pagesize=landscape(A4))
    width, height = landscape(A4)
    y = height - 2 * cm
    c.setFont("Helvetica-Bold", 14)
    c.drawString(2 * cm, y, title)
    y -= 1 * cm
    c.setFont("Helvetica", 9)
    headers = ["Date", "Description", "Debit", "Credit", "Balance"]
    col_x = [2 * cm, 4 * cm, 14 * cm, 17 * cm, 19.5 * cm]
    bal = 125000.0
    for i, h in enumerate(headers):
        c.drawString(col_x[i], y, h)
    y -= 0.5 * cm
    c.line(2 * cm, y, width - 2 * cm, y)
    y -= 0.4 * cm

    for tx in transactions:
        if y < 2 * cm:
            c.showPage()
            y = height - 2 * cm
            c.setFont("Helvetica", 9)
        d = tx["date"]
        d_str = d.strftime("%d/%m/%Y") if isinstance(d, date) else str(d)
        c.drawString(col_x[0], y, d_str)
        desc = tx["desc"][:70] + ("…" if len(tx["desc"]) > 70 else "")
        c.drawString(col_x[1], y, desc)
        dr = tx.get("debit")
        cr = tx.get("credit")
        if dr:
            c.drawRightString(col_x[2] + 2.5 * cm, y, money_fmt(dr))
            bal -= dr
        else:
            c.drawRightString(col_x[2] + 2.5 * cm, y, "")
        if cr:
            c.drawRightString(col_x[3] + 2.5 * cm, y, money_fmt(cr))
            bal += cr
        else:
            c.drawRightString(col_x[3] + 2.5 * cm, y, "")
        c.drawRightString(col_x[4] + 2.2 * cm, y, money_fmt(bal))
        y -= 0.38 * cm

    c.save()


# ---------------------------------------------------------------------------
# Main generation
# ---------------------------------------------------------------------------


def assign_dates_and_amounts(rng: random.Random) -> tuple[list[date], list[float], list[int]]:
    """Returns per-index (1..100) date, inc_gst, customer_index."""
    start = date(2024, 1, 1)
    end = date(2024, 6, 30)
    total_days = (end - start).days

    dates: list[date | None] = [None] * 101
    inc_list: list[float | None] = [None] * 101
    cust_idx: list[int | None] = [None] * 101

    # Spread base dates evenly + jitter
    for i in range(1, 101):
        frac = (i - 1) / 99.0 if 99 else 0
        base = start + timedelta(days=int(frac * total_days))
        jitter = rng.randint(-2, 2)
        d = base + timedelta(days=jitter)
        if d < start:
            d = start
        if d > end:
            d = end
        dates[i] = d

    # 10 cutoff cluster around 30 Jun 2024
    cutoff_indices = list(range(40, 50))  # INV-040 to 049
    for j in cutoff_indices:
        dates[j] = date(2024, 6, 30) - timedelta(days=rng.randint(0, 5))

    # 8 high-value (> $500k ex GST → inc > 550k)
    hv_idx = [7, 14, 21, 28, 35, 42, 49, 56]
    for i in hv_idx:
        inc_list[i] = round(rng.uniform(550_000.01, 2_000_000), 2)

    # Fill remaining amounts $5k – $2M inc GST (non-HV below 550k unless random hits)
    for i in range(1, 101):
        if inc_list[i] is not None:
            continue
        inc_list[i] = round(rng.uniform(5_000, 549_999.99), 2)

    # Customer rotation
    for i in range(1, 101):
        cust_idx[i] = (i - 1) % 15

    return dates, inc_list, cust_idx  # type: ignore


def payment_date(rng: random.Random, inv_d: date) -> date:
    return inv_d + timedelta(days=rng.randint(15, 45))


def main() -> int:
    rng = random.Random(SEED)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    errors: list[str] = []
    layout_counts: Counter[str] = Counter()

    dates, inc_list, cust_idx = assign_dates_and_amounts(rng)

    # Layout assignment: round-robin A–L
    layouts: dict[int, str] = {}
    for i in range(1, 101):
        layouts[i] = LAYOUT_TYPES[(i - 1) % 12]
        layout_counts[layouts[i]] += 1

    for i in range(1, 101):
        if layouts[i] == "J":
            inc_list[i] = -abs(float(inc_list[i]))

    # Generate 100 invoice PDFs
    for i in range(1, 101):
        layout = layouts[i]
        inv = invoice_number(i)
        customer = CUSTOMERS[cust_idx[i]]
        inv_date = dates[i]
        inc = float(inc_list[i])
        ex = ex_from_inc(abs(inc))
        gst = round(abs(inc) - ex, 2)
        if inc < 0:
            ex = -ex
            gst = -gst

        rem_lines = None
        if layout == "I":
            nlines = rng.randint(3, 5)
            refs: list[tuple[str, float]] = []
            seen: set[int] = set()
            for _ in range(nlines * 3):
                if len(refs) >= nlines:
                    break
                ref_i = rng.randint(1, 100)
                if ref_i == i or ref_i in seen:
                    continue
                seen.add(ref_i)
                share = abs(float(inc_list[ref_i])) / max(1, nlines)
                refs.append((invoice_number(ref_i), round(share, 2)))
            if len(refs) < 3:
                refs = [(inv, abs(inc))]
            tot = sum(a for _, a in refs) or 1.0
            scale = abs(inc) / tot
            rem_lines = [(r, round(a * scale, 2)) for r, a in refs]

        if layout == "I":
            fname = f"remittance_batch_{inv}.pdf"
        else:
            fname = f"{inv}_layout_{layout}.pdf"

        path = OUT_DIR / fname
        try:
            write_invoice_pdf(path, layout, i, customer, inv_date, inc, ex, gst, rng, rem_lines)
        except Exception as e:
            errors.append(f"{fname}: {e}")

    # --- GL CSV ---
    gl_path = OUT_DIR / "stress_test_gl.csv"
    try:
        with gl_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["invoice_number", "date", "customer", "amount"])
            for i in range(1, 101):
                inv = invoice_number(i)
                inv_date = dates[i]
                customer = CUSTOMERS[cust_idx[i]]
                inc = float(inc_list[i])
                ex = ex_from_inc(abs(inc))
                if inc < 0:
                    ex = -ex
                w.writerow([inv, inv_date.isoformat(), customer, f"{ex:.2f}"])
    except Exception as e:
        errors.append(f"stress_test_gl.csv: {e}")

    # --- Bank transactions from invoices (payment 15–45 days after invoice) ---
    payments: list[dict] = []
    for i in range(1, 101):
        if layouts[i] in ("I", "J"):
            continue
        inc = float(inc_list[i])
        inv_date = dates[i]
        pd = payment_date(rng, inv_date)
        customer = CUSTOMERS[cust_idx[i]]
        cust_short = truncate_desc(customer, 18)
        inv = invoice_number(i)
        styles_pick = rng.randint(0, 3)
        if styles_pick == 0:
            desc = f"RECEIPT - {cust_short}"
        elif styles_pick == 1:
            desc = f"EFT FROM {cust_short} REF {inv}"
        elif styles_pick == 2:
            desc = f"DIRECT CREDIT {customer[:35]}"
        else:
            desc = cust_short

        payments.append(
            {
                "date": pd,
                "desc": desc,
                "debit": None,
                "credit": round(abs(inc), 2),
                "inv_idx": i,
            }
        )

    for i in range(1, 101):
        if layouts[i] != "J":
            continue
        inc = float(inc_list[i])
        inv_date = dates[i]
        pd = payment_date(rng, inv_date)
        desc = f"REFUND CN {invoice_number(i)} {truncate_desc(CUSTOMERS[cust_idx[i]], 12)}"
        payments.append({"date": pd, "desc": desc, "debit": round(abs(inc), 2), "credit": None, "inv_idx": i})

    # Remittance-type files: add synthetic receipt for that invoice's total on invoice date + offset
    for i in range(1, 101):
        if layouts[i] != "I":
            continue
        inc = float(inc_list[i])
        inv_date = dates[i]
        pd = payment_date(rng, inv_date)
        customer = CUSTOMERS[cust_idx[i]]
        desc = f"REMITTANCE EFT {truncate_desc(customer, 14)} REF batch"
        payments.append({"date": pd, "desc": desc, "debit": None, "credit": round(inc, 2), "inv_idx": i})

    # Bucket payments by statement period (by calendar month of transaction date)
    def bucket(tx_date: date) -> str:
        if tx_date.year == 2024 and tx_date.month <= 2:
            return "jan_feb"
        if tx_date.year == 2024 and tx_date.month <= 4:
            return "mar_apr"
        if tx_date.year == 2024 and tx_date.month <= 6:
            return "may_jun"
        return "may_jun"

    buckets: dict[str, list[dict]] = defaultdict(list)
    for p in payments:
        buckets[bucket(p["date"])].append(p)

    # Pad each bucket to 40 rows: add 10 non-receipt + ensure ~30 receipts minimum from pool
    fee_descs = [
        "MONTHLY ACCOUNT FEE",
        "INTERNATIONAL TRANSFER FEE",
        "SWIFT CHARGE",
        "ATM ADMIN FEE",
        "OVERDRAWN INTEREST",
        "STAFF TRANSFER INTERNAL",
        "BPAY PAYMENT TO SUPPLIER",
        "PERIODIC PAYMENT TO PAYROLL",
        "CHEQUE CLEARING FEE",
        "CARD MERCHANT FEE",
    ]

    bank_specs = [
        ("bank_jan_feb_2024.pdf", "Operating Account — Statement Jan–Feb 2024", "jan_feb"),
        ("bank_mar_apr_2024.pdf", "Operating Account — Statement Mar–Apr 2024", "mar_apr"),
        ("bank_may_jun_2024.pdf", "Operating Account — Statement May–Jun 2024", "may_jun"),
    ]

    bank_row_notes: list[str] = []

    for fname, title, key in bank_specs:
        txs = buckets[key]
        txs.sort(key=lambda x: x["date"])
        # Non-receipt rows for this period's date range
        if key == "jan_feb":
            d0, d1 = date(2024, 1, 1), date(2024, 2, 29)
        elif key == "mar_apr":
            d0, d1 = date(2024, 3, 1), date(2024, 4, 30)
        else:
            d0, d1 = date(2024, 5, 1), date(2024, 6, 30)

        extra_fees = []
        for k in range(10):
            dd = d0 + timedelta(days=rng.randint(0, (d1 - d0).days))
            amt = round(rng.uniform(5, 120), 2)
            if rng.random() < 0.5:
                extra_fees.append({"date": dd, "desc": fee_descs[k], "debit": amt, "credit": None})
            else:
                extra_fees.append({"date": dd, "desc": fee_descs[k], "debit": None, "credit": round(rng.uniform(0.01, 5), 2)})

        merged = list(txs) + extra_fees
        merged.sort(key=lambda x: x["date"])

        # If fewer than 40, duplicate spacing with small filler credits
        while len(merged) < 40:
            dd = d0 + timedelta(days=rng.randint(0, max(1, (d1 - d0).days)))
            merged.append(
                {
                    "date": dd,
                    "desc": "MISC CREDIT ADJUSTMENT",
                    "debit": None,
                    "credit": round(rng.uniform(10, 50), 2),
                }
            )
            merged.sort(key=lambda x: x["date"])

        merged = merged[:40]
        n_cred = sum(1 for x in merged if x.get("credit"))
        n_deb = sum(1 for x in merged if x.get("debit"))
        bank_row_notes.append(
            f"  {fname}: {len(txs)} payment events in period + 10 fee/adjustment rows -> {len(merged)} rows "
            f"({n_cred} with credit, {n_deb} with debit)"
        )

        try:
            write_bank_statement_pdf(OUT_DIR / fname, title, merged)
        except Exception as e:
            errors.append(f"{fname}: {e}")

    # --- Summary ---
    print("=" * 72)
    print("Stress test document generation summary")
    print("=" * 72)
    print(f"Output directory: {OUT_DIR}")
    print(f"Invoice PDFs: 100")
    print("Layout distribution (A-L):")
    for lt in LAYOUT_TYPES:
        print(f"  Type {lt}: {layout_counts[lt]}")
    print(f"GL CSV: {gl_path.name} (100 rows)")
    print(f"Total synthetic bank payment lines (before per-statement trim): {len(payments)}")
    print("Bank statements (40 rows each; includes 10 non-receipt fee/adjustment lines):")
    for line in bank_row_notes:
        print(line)
    print(f"High-value GL rows (ex GST > $500k): 8 (invoice indices {sorted(set([7,14,21,28,35,42,49,56]))})")
    print(f"Cutoff-cluster invoice indices (around 30 Jun 2024): {sorted(set(range(40,50)))}")
    print(f"Remittance-style filenames (type I): {layout_counts['I']} PDFs")
    print(f"Credit note layouts (type J): {layout_counts['J']} PDFs")
    if errors:
        print(f"\nErrors ({len(errors)}):")
        for e in errors:
            print(f"  - {e}")
        return 1
    print("\nCompleted with no errors.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
