from pathlib import Path

import datetime as dt
import io
from uuid import uuid4

import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from starlette.responses import StreamingResponse

from .revenue_tests import cutoff_testing, mus_sample_size_parameters, mus_sampling, target_testing

app = FastAPI(title="Revenue Testing SaaS API", version="0.1.0")

FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"
OUTPUTS_DIR = Path(__file__).resolve().parent.parent / "outputs"

# Mount the frontend folder so any static assets can be served.
app.mount("/frontend", StaticFiles(directory=str(FRONTEND_DIR)), name="frontend")


@app.get("/")
def root() -> FileResponse:
    return FileResponse(str(FRONTEND_DIR / "index.html"))


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


def _coerce_bool(value: str | None) -> bool:
    if value is None:
        return False
    v = str(value).strip().lower()
    return v in {"1", "true", "t", "yes", "y", "on"}


def _to_snake(name: str) -> str:
    s = str(name).strip().lower()
    out: list[str] = []
    prev_us = False
    for ch in s:
        if ch.isalnum():
            out.append(ch)
            prev_us = False
        else:
            if not prev_us:
                out.append("_")
                prev_us = True
    snake = "".join(out).strip("_")
    while "__" in snake:
        snake = snake.replace("__", "_")
    return snake


def normalize_gl_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize GL column names to canonical schema:
    invoice_number, date, customer, amount

    Rules:
    - Strip whitespace from all column names
    - Convert to lowercase snake_case
    - Map common variations to canonical names
    - Raise a clear error if any required column can't be matched
    """
    if df is None or df.empty:
        raise ValueError("GL file is empty or could not be read into a table.")

    out = df.copy()

    # (1) + (2) base normalization
    out.rename(columns={c: _to_snake(c) for c in out.columns}, inplace=True)
    out.columns = [_to_snake(c) for c in out.columns]

    # (3) synonym mapping (all values already snake_case)
    synonym_map: dict[str, set[str]] = {
        "invoice_number": {
            "invoice_number",
            "invoice_no",
            "invoice_num",
            "invoice",
            "inv_number",
            "inv_no",
            "inv_num",
            "inv",
        },
        "date": {
            "date",
            "invoice_date",
            "posting_date",
            "transaction_date",
            "gl_date",
        },
        "customer": {
            "customer",
            "customer_name",
            "client",
            "client_name",
            "debtor",
        },
        "amount": {
            "amount",
            "value",
            "total",
            "net_amount",
            "revenue",
            "gl_amount",
        },
    }

    reverse: dict[str, str] = {}
    for canonical, alts in synonym_map.items():
        for alt in alts:
            reverse[_to_snake(alt)] = canonical

    canonical_hits: dict[str, str] = {}
    for col in list(out.columns):
        key = _to_snake(col)
        if key not in reverse:
            continue
        canonical = reverse[key]
        if canonical in canonical_hits and canonical_hits[canonical] != col:
            raise ValueError(
                f"Ambiguous columns for '{canonical}': '{canonical_hits[canonical]}' and '{col}'. "
                "Please keep only one."
            )
        canonical_hits[canonical] = col

    # (4) required columns
    required = ["invoice_number", "date", "customer", "amount"]
    missing = [c for c in required if c not in canonical_hits]
    if missing:
        missing_list = ", ".join(missing)
        raise ValueError(
            f"Missing required column(s): {missing_list}. "
            "Please include columns that map to invoice_number, date, customer, amount."
        )

    out.rename(columns={src: canonical for canonical, src in canonical_hits.items()}, inplace=True)
    return out


def _pick_date_column(df: pd.DataFrame) -> str:
    # After normalization we always expect a canonical 'date' column.
    if "date" not in df.columns:
        raise ValueError(
            "Missing required column: date. Please include an invoice/transaction date column."
        )
    return "date"


def _target_threshold(performance_materiality: float, inherent_risk: str) -> float:
    risk_pct_by_level: dict[str, float] = {
        "significant": 0.10,
        "higher": 0.25,
        "lower": 0.50,
    }
    level = str(inherent_risk).strip().lower()
    if level not in risk_pct_by_level:
        raise ValueError(
            "Invalid inherent_risk. Expected one of: 'significant', 'higher', 'lower'."
        )
    return float(performance_materiality) * risk_pct_by_level[level]


def _fmt_title_case(value: str) -> str:
    v = str(value).strip()
    return v[:1].upper() + v[1:] if v else v


def _apply_table_style(ws, header_row: int, min_col: int, max_col: int) -> None:
    navy = PatternFill("solid", fgColor="0F2A52")
    white_bold = Font(bold=True, color="FFFFFF")
    thin = Side(style="thin", color="D6DEEA")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    for c in range(min_col, max_col + 1):
        cell = ws.cell(row=header_row, column=c)
        cell.fill = navy
        cell.font = white_bold
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = border

    # Borders for all populated cells
    for row in ws.iter_rows(min_row=header_row + 1, max_row=ws.max_row, min_col=min_col, max_col=max_col):
        for cell in row:
            cell.border = border
            if cell.alignment is None or (cell.alignment.horizontal is None and cell.alignment.vertical is None):
                cell.alignment = Alignment(vertical="top", wrap_text=True)


def _auto_fit_columns(ws, min_col: int = 1, max_col: int | None = None) -> None:
    if max_col is None:
        max_col = ws.max_column
    for c in range(min_col, max_col + 1):
        max_len = 0
        for r in range(1, ws.max_row + 1):
            v = ws.cell(row=r, column=c).value
            if v is None:
                continue
            s = str(v)
            if len(s) > max_len:
                max_len = len(s)
        ws.column_dimensions[get_column_letter(c)].width = min(max(10, max_len + 2), 42)


def _write_header(ws, row: int, headers: list[str]) -> None:
    for i, h in enumerate(headers, start=1):
        ws.cell(row=row, column=i, value=h)


def _set_number_formats(ws, col_map: dict[str, int], start_row: int) -> None:
    amount_cols = {"Amount", "Total", "Total Value", "Threshold Used", "Interval", "Cumulative Value", "Target testing threshold"}
    date_cols = {"Date", "Cutoff Date", "Date/Time of run"}

    for name, idx in col_map.items():
        if name in amount_cols:
            for r in range(start_row, ws.max_row + 1):
                ws.cell(row=r, column=idx).number_format = "#,##0.00"
        if name in date_cols:
            for r in range(start_row, ws.max_row + 1):
                ws.cell(row=r, column=idx).number_format = "DD/MM/YYYY"


async def _read_gl_to_df(upload: UploadFile) -> pd.DataFrame:
    filename = (upload.filename or "").strip()
    ext = filename.split(".")[-1].lower() if "." in filename else ""
    data = await upload.read()
    if not data:
        raise ValueError("Uploaded file is empty.")

    bio = io.BytesIO(data)
    if ext == "csv":
        return pd.read_csv(bio)
    if ext in {"xls", "xlsx"}:
        return pd.read_excel(bio)

    # Fallback based on content type if extension is missing/misleading.
    ctype = (upload.content_type or "").lower()
    if "csv" in ctype:
        return pd.read_csv(bio)
    if "excel" in ctype or "spreadsheet" in ctype:
        return pd.read_excel(bio)

    raise ValueError("Unsupported file type. Please upload a CSV or Excel file.")


@app.post("/upload")
async def upload(
    gl_file: UploadFile = File(...),
    performance_materiality: float = Form(...),
    inherent_risk: str = Form(...),
    control_risk: str = Form(...),
    sap_level: str = Form(...),
    confidence_level: int = Form(...),
    cutoff_date: dt.date = Form(...),
    test_negatives: str | None = Form(None),
    enable_target_testing: str | None = Form(None),
):
    """
    Accept GL + parameters, run tests, and return an Excel workpaper.
    """
    try:
        gl_df = normalize_gl_columns(await _read_gl_to_df(gl_file))
        run_id = str(uuid4())
        run_dt = dt.datetime.now()
        enable_target = True if enable_target_testing is None else _coerce_bool(enable_target_testing)

        # Normalize population if negatives should be excluded.
        include_negatives = _coerce_bool(test_negatives)
        if not include_negatives:
            amounts = pd.to_numeric(gl_df["amount"], errors="coerce")
            gl_df = gl_df.loc[amounts.isna() | (amounts >= 0)].copy()

        # Normalize key columns for reporting
        date_column = _pick_date_column(gl_df)
        df = gl_df.copy()
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")

        invoice_col = "invoice_number"
        customer_col = "customer"

        if enable_target:
            target_df = target_testing(
                gl_transactions=df,
                performance_materiality=float(performance_materiality),
                risk_level=str(inherent_risk),
            )
            # Correct MUS methodology: remove target-tested items from population before MUS
            target_ids = (
                pd.Series(target_df.get(invoice_col, []))
                .dropna()
                .astype(str)
                .tolist()
            )
            target_id_set = set(target_ids)
            residual_df = df.loc[~df[invoice_col].astype(str).isin(target_id_set)].copy()
        else:
            target_df = df.iloc[0:0].copy()
            target_id_set = set()
            residual_df = df.copy()

        mus_df = mus_sampling(
            gl_transactions=residual_df,
            performance_materiality=float(performance_materiality),
            inherent_risk=str(inherent_risk),
            control_risk=str(control_risk),
            sap_level=str(sap_level),
            confidence_level=int(confidence_level),
            exclude_invoice_numbers=(target_id_set or None),
            invoice_col=invoice_col,
        )

        cutoff_df = cutoff_testing(
            gl_transactions=df,
            cutoff_date=cutoff_date,
            date_column=date_column,
        )

        # Compute parameter and population metrics
        pm = float(performance_materiality)
        pop_count = int(len(df))
        pop_value = float(df["amount"].sum(skipna=True))
        threshold_used = float(_target_threshold(pm, inherent_risk)) if enable_target else None
        target_tested_value = (
            float(pd.to_numeric(target_df.get("amount"), errors="coerce").sum(skipna=True))
            if enable_target
            else 0.0
        )
        residual_pop_value_for_mus = float(
            pd.to_numeric(residual_df.loc[residual_df["amount"] > 0, "amount"], errors="coerce").sum(skipna=True)
        )

        # MUS interval & cumulative values for sampled items
        population_mus = residual_df.loc[residual_df["amount"] > 0].copy()
        population_mus["__cume__"] = population_mus["amount"].cumsum()
        pop_value_pos = float(population_mus["amount"].sum(skipna=True))
        mus_params = mus_sample_size_parameters(
            population_value=pop_value_pos,
            performance_materiality=pm,
            inherent_risk=str(inherent_risk).strip().lower(),
            control_risk=str(control_risk).strip().lower(),
            sap_level=str(sap_level).strip().lower(),
        )
        ria_pct = float(mus_params["ria_pct"])
        confidence_factor = float(mus_params["confidence_factor"])
        sample_size = int(mus_params["sample_size"])
        interval = (pop_value_pos / sample_size) if sample_size > 0 else None

        # Build professional workbook with openpyxl
        wb = Workbook()
        wb.remove(wb.active)

        # -------- Sheet 1: Audit Parameters --------
        ws_params = wb.create_sheet("Audit Parameters")
        ws_params["A1"] = "Audit Parameters"
        ws_params["A1"].font = Font(bold=True, size=14, color="0F2A52")
        ws_params["A3"] = "Parameter"
        ws_params["B3"] = "Value"

        mus_formula_text = (
            "CEILING((Population Value × Confidence Factor) / Tolerable Misstatement)"
        )
        params_rows = [
            ("Performance Materiality", pm),
            ("Target Testing", "Enabled" if enable_target else "Disabled"),
            (
                "Target Testing Note",
                None
                if enable_target
                else "Target testing not performed - MUS applied to full population",
            ),
            ("Inherent Risk", _fmt_title_case(inherent_risk)),
            ("Control Risk", _fmt_title_case(control_risk)),
            ("SAP Level", _fmt_title_case(sap_level)),
            ("Confidence Level", int(confidence_level)),
            ("RIA (Risk of Incorrect Acceptance)", ria_pct / 100.0),
            ("Confidence Factor (Poisson, 0 expected misstatements)", confidence_factor),
            ("MUS Sample Size Formula", mus_formula_text),
            ("MUS Sample Size", sample_size),
            ("Cutoff Date", cutoff_date),
            ("Test Negatives", bool(include_negatives)),
            ("Run ID", run_id),
            ("Date/Time of run", run_dt),
            ("Total population count", pop_count),
            ("Total population value", pop_value),
            ("Less Target Tested Value", target_tested_value),
            ("Residual Population Value used for MUS (positive amounts)", residual_pop_value_for_mus),
            ("Target testing threshold", threshold_used),
        ]
        for i, (k, v) in enumerate(params_rows, start=4):
            ws_params.cell(row=i, column=1, value=k)
            ws_params.cell(row=i, column=2, value=v)

        _apply_table_style(ws_params, header_row=3, min_col=1, max_col=2)
        ws_params.freeze_panes = "A4"
        ws_params.column_dimensions["A"].width = 34
        ws_params.column_dimensions["B"].width = 34
        for r in range(4, 4 + len(params_rows)):
            k = ws_params.cell(row=r, column=1).value
            if k in {
                "Performance Materiality",
                "Total population value",
                "Less Target Tested Value",
                "Residual Population Value used for MUS (positive amounts)",
                "Target testing threshold",
            }:
                ws_params.cell(row=r, column=2).number_format = "#,##0.00"
            if k == "RIA (Risk of Incorrect Acceptance)":
                ws_params.cell(row=r, column=2).number_format = "0%"
            if k == "Cutoff Date":
                ws_params.cell(row=r, column=2).number_format = "DD/MM/YYYY"
            if k == "Date/Time of run":
                ws_params.cell(row=r, column=2).number_format = "DD/MM/YYYY HH:MM"

        # -------- Sheet 2: Target Testing --------
        ws_target = wb.create_sheet("Target Testing")
        target_headers = [
            "Invoice Number",
            "Date",
            "Customer",
            "Amount",
            "Threshold Used",
            "Reason Selected",
        ]
        if not enable_target:
            ws_target.merge_cells(start_row=1, start_column=1, end_row=1, end_column=len(target_headers))
            note_cell = ws_target.cell(
                row=1,
                column=1,
                value="Target testing not performed - MUS applied to full population",
            )
            note_cell.fill = PatternFill("solid", fgColor="0F2A52")
            note_cell.font = Font(bold=True, color="FFFFFF")
            note_cell.alignment = Alignment(horizontal="left", vertical="center")

            _write_header(ws_target, 2, target_headers)
            _apply_table_style(ws_target, header_row=2, min_col=1, max_col=len(target_headers))
            ws_target.freeze_panes = "A3"
            ws_target.auto_filter.ref = f"A2:{get_column_letter(len(target_headers))}2"
            _auto_fit_columns(ws_target)
        else:
            _write_header(ws_target, 1, target_headers)

            target_out = target_df.copy()
            target_out["__reason__"] = "Amount exceeds target testing threshold"
            for i, row in enumerate(target_out.itertuples(index=False), start=2):
                inv = getattr(row, invoice_col, None) if hasattr(row, invoice_col) else None
                dval = getattr(row, date_column, None) if hasattr(row, date_column) else None
                cust = getattr(row, customer_col, None) if hasattr(row, customer_col) else None
                amt = getattr(row, "amount", None)
                ws_target.cell(row=i, column=1, value=inv)
                ws_target.cell(row=i, column=2, value=(pd.to_datetime(dval).date() if pd.notna(dval) else None))
                ws_target.cell(row=i, column=3, value=cust)
                ws_target.cell(row=i, column=4, value=float(amt) if pd.notna(amt) else None)
                ws_target.cell(row=i, column=5, value=threshold_used)
                ws_target.cell(row=i, column=6, value="Amount exceeds threshold")

            # Summary row
            summary_row = ws_target.max_row + 2
            ws_target.cell(row=summary_row, column=1, value="Summary").font = Font(bold=True)
            ws_target.cell(row=summary_row, column=2, value="Count").font = Font(bold=True)
            ws_target.cell(row=summary_row, column=3, value=int(len(target_out))).font = Font(bold=True)
            ws_target.cell(row=summary_row, column=4, value="Total value").font = Font(bold=True)
            total_target_value = float(pd.to_numeric(target_out.get("amount"), errors="coerce").sum(skipna=True))
            ws_target.cell(row=summary_row, column=5, value=total_target_value).font = Font(bold=True)
            ws_target.cell(row=summary_row, column=5).number_format = "#,##0.00"

            _apply_table_style(ws_target, header_row=1, min_col=1, max_col=len(target_headers))
            ws_target.freeze_panes = "A2"
            ws_target.auto_filter.ref = f"A1:{get_column_letter(len(target_headers))}1"
            # Formats
            for r in range(2, ws_target.max_row + 1):
                ws_target.cell(row=r, column=2).number_format = "DD/MM/YYYY"
                ws_target.cell(row=r, column=4).number_format = "#,##0.00"
                ws_target.cell(row=r, column=5).number_format = "#,##0.00"
            _auto_fit_columns(ws_target)

        # -------- Sheet 3: MUS Sample --------
        ws_mus = wb.create_sheet("MUS Sample")
        ws_mus.merge_cells(start_row=1, start_column=1, end_row=1, end_column=8)
        mus_note = (
            "MUS sample drawn from residual population (total less target-tested items)."
            if enable_target
            else "MUS sample drawn from full population (target testing not performed)."
        )
        note = ws_mus.cell(row=1, column=1, value=mus_note)
        note.fill = PatternFill("solid", fgColor="0F2A52")
        note.font = Font(bold=True, color="FFFFFF")
        note.alignment = Alignment(horizontal="left", vertical="center")
        mus_headers = [
            "Sample Number",
            "Invoice Number",
            "Date",
            "Customer",
            "Amount",
            "Cumulative Value",
            "Interval",
            "Reason Selected",
        ]
        _write_header(ws_mus, 2, mus_headers)

        mus_out = mus_df.copy()
        # Determine cumulative value in MUS population order
        mus_join = population_mus[[invoice_col, "__cume__"]].copy()
        mus_join.columns = [invoice_col, "__cume__"]
        if invoice_col in mus_out.columns:
            mus_out = mus_out.merge(mus_join, on=invoice_col, how="left")
        else:
            mus_out["__cume__"] = None

        for i, row in enumerate(mus_out.itertuples(index=False), start=3):
            inv = getattr(row, invoice_col, None) if hasattr(row, invoice_col) else None
            dval = getattr(row, date_column, None) if hasattr(row, date_column) else None
            cust = getattr(row, customer_col, None) if hasattr(row, customer_col) else None
            amt = getattr(row, "amount", None)
            cume = getattr(row, "__cume__", None) if hasattr(row, "__cume__") else None

            ws_mus.cell(row=i, column=1, value=i - 2)
            ws_mus.cell(row=i, column=2, value=inv)
            ws_mus.cell(row=i, column=3, value=(pd.to_datetime(dval).date() if pd.notna(dval) else None))
            ws_mus.cell(row=i, column=4, value=cust)
            ws_mus.cell(row=i, column=5, value=float(amt) if pd.notna(amt) else None)
            ws_mus.cell(row=i, column=6, value=float(cume) if pd.notna(cume) else None)
            ws_mus.cell(row=i, column=7, value=float(interval) if interval is not None else None)
            ws_mus.cell(row=i, column=8, value="Residual population MUS: cumulative value crossed sampling interval point")

        # Summary row
        summary_row = ws_mus.max_row + 2
        ws_mus.cell(row=summary_row, column=1, value="Summary").font = Font(bold=True)
        ws_mus.cell(row=summary_row, column=2, value="Sample size").font = Font(bold=True)
        ws_mus.cell(row=summary_row, column=3, value=int(len(mus_out))).font = Font(bold=True)
        ws_mus.cell(row=summary_row, column=4, value="Total value").font = Font(bold=True)
        total_mus_value = float(pd.to_numeric(mus_out.get("amount"), errors="coerce").sum(skipna=True))
        ws_mus.cell(row=summary_row, column=5, value=total_mus_value).font = Font(bold=True)
        ws_mus.cell(row=summary_row, column=5).number_format = "#,##0.00"

        _apply_table_style(ws_mus, header_row=2, min_col=1, max_col=len(mus_headers))
        ws_mus.freeze_panes = "A3"
        ws_mus.auto_filter.ref = f"A2:{get_column_letter(len(mus_headers))}2"
        for r in range(2, ws_mus.max_row + 1):
            if r >= 3:
                ws_mus.cell(row=r, column=3).number_format = "DD/MM/YYYY"
            for c in [5, 6, 7]:
                if r >= 3:
                    ws_mus.cell(row=r, column=c).number_format = "#,##0.00"
        _auto_fit_columns(ws_mus)

        # -------- Sheet 4: Cutoff Testing --------
        ws_cutoff = wb.create_sheet("Cutoff Testing")
        cutoff_headers = [
            "Invoice Number",
            "Date",
            "Customer",
            "Amount",
            "Cutoff Position",
            "Days from Cutoff",
        ]

        # Split pre/post and add section headers
        cutoff_out = cutoff_df.copy()
        if date_column in cutoff_out.columns:
            cutoff_out[date_column] = pd.to_datetime(cutoff_out[date_column], errors="coerce")
        cutoff_out["__days__"] = None
        if date_column in cutoff_out.columns:
            cutoff_out["__days__"] = (cutoff_out[date_column].dt.date - cutoff_date).apply(
                lambda d: d.days if d is not None else None
            )

        navy = PatternFill("solid", fgColor="0F2A52")
        white_bold = Font(bold=True, color="FFFFFF")

        def section_title(row_idx: int, title: str) -> int:
            ws_cutoff.merge_cells(start_row=row_idx, start_column=1, end_row=row_idx, end_column=len(cutoff_headers))
            c = ws_cutoff.cell(row=row_idx, column=1, value=title)
            c.fill = navy
            c.font = white_bold
            c.alignment = Alignment(horizontal="left", vertical="center")
            return row_idx + 1

        r = 1
        r = section_title(r, f"Pre-cutoff transactions (before {cutoff_date.strftime('%d/%m/%Y')})")
        _write_header(ws_cutoff, r, cutoff_headers)
        _apply_table_style(ws_cutoff, header_row=r, min_col=1, max_col=len(cutoff_headers))
        r += 1

        pre_df = cutoff_out.loc[cutoff_out.get("cutoff_position") == "pre"].copy()
        for row in pre_df.itertuples(index=False):
            inv = getattr(row, invoice_col, None) if hasattr(row, invoice_col) else None
            dval = getattr(row, date_column, None) if hasattr(row, date_column) else None
            cust = getattr(row, customer_col, None) if hasattr(row, customer_col) else None
            amt = getattr(row, "amount", None)
            pos = getattr(row, "cutoff_position", None)
            days = getattr(row, "__days__", None)

            ws_cutoff.cell(row=r, column=1, value=inv)
            ws_cutoff.cell(row=r, column=2, value=(pd.to_datetime(dval).date() if pd.notna(dval) else None))
            ws_cutoff.cell(row=r, column=3, value=cust)
            ws_cutoff.cell(row=r, column=4, value=float(amt) if pd.notna(amt) else None)
            ws_cutoff.cell(row=r, column=5, value=pos)
            ws_cutoff.cell(row=r, column=6, value=int(days) if days is not None and pd.notna(days) else None)
            r += 1

        r += 1
        r = section_title(r, f"Post-cutoff transactions (after {cutoff_date.strftime('%d/%m/%Y')})")
        _write_header(ws_cutoff, r, cutoff_headers)
        _apply_table_style(ws_cutoff, header_row=r, min_col=1, max_col=len(cutoff_headers))
        r += 1

        post_df = cutoff_out.loc[cutoff_out.get("cutoff_position") == "post"].copy()
        for row in post_df.itertuples(index=False):
            inv = getattr(row, invoice_col, None) if hasattr(row, invoice_col) else None
            dval = getattr(row, date_column, None) if hasattr(row, date_column) else None
            cust = getattr(row, customer_col, None) if hasattr(row, customer_col) else None
            amt = getattr(row, "amount", None)
            pos = getattr(row, "cutoff_position", None)
            days = getattr(row, "__days__", None)

            ws_cutoff.cell(row=r, column=1, value=inv)
            ws_cutoff.cell(row=r, column=2, value=(pd.to_datetime(dval).date() if pd.notna(dval) else None))
            ws_cutoff.cell(row=r, column=3, value=cust)
            ws_cutoff.cell(row=r, column=4, value=float(amt) if pd.notna(amt) else None)
            ws_cutoff.cell(row=r, column=5, value=pos)
            ws_cutoff.cell(row=r, column=6, value=int(days) if days is not None and pd.notna(days) else None)
            r += 1

        ws_cutoff.freeze_panes = "A3"
        for rr in range(1, ws_cutoff.max_row + 1):
            ws_cutoff.cell(row=rr, column=2).number_format = "DD/MM/YYYY"
            ws_cutoff.cell(row=rr, column=4).number_format = "#,##0.00"
        _auto_fit_columns(ws_cutoff)

        # -------- Sheet 5: Population Summary --------
        ws_pop = wb.create_sheet("Population Summary")
        pop_headers = ["Invoice Number", "Date", "Customer", "Amount", "Selection"]
        _write_header(ws_pop, 1, pop_headers)

        # Keep normalized column names for processing; only use pretty headers in the worksheet.
        df_pop = df[[invoice_col, date_column, customer_col, "amount"]].copy()

        target_set = set(pd.Series(target_df.get(invoice_col, [])).dropna().astype(str).tolist())
        mus_set = set(pd.Series(mus_df.get(invoice_col, [])).dropna().astype(str).tolist())
        cutoff_set = set(pd.Series(cutoff_df.get(invoice_col, [])).dropna().astype(str).tolist())

        def selection_for(inv: str) -> str:
            tags = []
            if inv in target_set:
                tags.append("Target")
            if inv in mus_set:
                tags.append("MUS")
            if inv in cutoff_set:
                tags.append("Cutoff")
            return "/".join(tags) if tags else "Not Selected"

        for i, row in enumerate(df_pop.itertuples(index=False), start=2):
            inv = str(getattr(row, invoice_col))
            dval = getattr(row, date_column)
            cust = getattr(row, customer_col)
            amt = getattr(row, "amount")
            ws_pop.cell(row=i, column=1, value=inv)
            ws_pop.cell(row=i, column=2, value=(pd.to_datetime(dval).date() if pd.notna(dval) else None))
            ws_pop.cell(row=i, column=3, value=cust)
            ws_pop.cell(row=i, column=4, value=float(amt) if pd.notna(amt) else None)
            ws_pop.cell(row=i, column=5, value=selection_for(inv))

        _apply_table_style(ws_pop, header_row=1, min_col=1, max_col=len(pop_headers))
        ws_pop.freeze_panes = "A2"
        ws_pop.auto_filter.ref = f"A1:{get_column_letter(len(pop_headers))}1"
        for rr in range(2, ws_pop.max_row + 1):
            ws_pop.cell(row=rr, column=2).number_format = "DD/MM/YYYY"
            ws_pop.cell(row=rr, column=4).number_format = "#,##0.00"
        _auto_fit_columns(ws_pop)

        output = io.BytesIO()
        wb.save(output)

        # Persist a local copy so the workpaper exists even if browser download fails.
        OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
        disk_path = OUTPUTS_DIR / "Revenue_Test_Workpaper.xlsx"
        disk_path.write_bytes(output.getvalue())

        output.seek(0)
        out_name = "Revenue_Test_Workpaper.xlsx"
        headers = {"Content-Disposition": f'attachment; filename="{out_name}"'}
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@app.get("/revenue-tests/target")
def run_target_testing() -> dict:
    return {
        "status": "not_implemented",
        "message": "target_testing now requires a GL DataFrame, performance_materiality, and risk_level. Wire this to an upload/ingestion endpoint next.",
    }


@app.get("/revenue-tests/mus")
def run_mus_sampling() -> dict:
    return {
        "status": "not_implemented",
        "message": "mus_sampling now requires a GL DataFrame plus PM/risk inputs. Wire this to an upload/ingestion endpoint next.",
    }


@app.get("/revenue-tests/cutoff")
def run_cutoff_testing() -> dict:
    return {
        "status": "not_implemented",
        "message": "cutoff_testing now requires a GL DataFrame, cutoff_date, and a date column. Wire this to the /upload endpoint.",
    }

