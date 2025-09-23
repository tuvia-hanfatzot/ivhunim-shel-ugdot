import io
import re
import zipfile
from collections import defaultdict
from typing import Iterable, List, Optional, Tuple
import pandas as pd
import streamlit as st

# --- ID detection: 9 digits starting with 4, 8, or 9 ---
ID_PATTERN = re.compile(r'\b([489]\d{8})\b')


def extract_ids_from_text(text: str) -> List[str]:
    return list(dict.fromkeys(ID_PATTERN.findall(text or "")))  # preserve order & dedupe


# --- File readers ---
def read_docx_bytes(data: bytes) -> str:
    """Extract text from a .docx file (paragraphs + tables)."""
    try:
        from docx import Document  # python-docx
    except Exception as e:
        raise RuntimeError("python-docx is required to parse .docx files") from e

    file_like = io.BytesIO(data)
    doc = Document(file_like)

    parts = []
    for p in doc.paragraphs:
        parts.append(p.text)
    # Extract table text as well
    for tbl in doc.tables:
        for row in tbl.rows:
            for cell in row.cells:
                parts.append(cell.text)

    return "\n".join(parts)


def read_xlsx_bytes(data: bytes) -> str:
    """Extract all cell values from an .xlsx workbook using openpyxl."""
    try:
        from openpyxl import load_workbook
    except Exception as e:
        raise RuntimeError("openpyxl is required to parse .xlsx files") from e

    wb = load_workbook(io.BytesIO(data), data_only=True, read_only=True)
    parts = []
    for ws in wb.worksheets:
        for row in ws.iter_rows(values_only=True):
            for val in row:
                if val is None:
                    continue
                parts.append(str(val))
    return "\n".join(parts)


def read_xls_bytes(data: bytes) -> str:
    """Extract all cell values from an .xls workbook using xlrd (v2 supports .xls only)."""
    try:
        import xlrd  # Ensure version supports .xls
    except Exception as e:
        raise RuntimeError("xlrd is required (and must support .xls) to parse .xls files") from e

    book = xlrd.open_workbook(file_contents=data)
    parts = []
    for sheet in book.sheets():
        for r in range(sheet.nrows):
            for c in range(sheet.ncols):
                val = sheet.cell_value(r, c)
                if val in (None, ""):
                    continue
                parts.append(str(val))
    return "\n".join(parts)

def read_doc_bytes(data: bytes) -> str:
    """
    Best-effort text extraction for legacy .doc.
    Tries 'textract' if available; otherwise raises with a clear message.
    """
    try:
        import textract  # requires system dependencies
    except Exception as e:
        raise RuntimeError(
            "textract is required to parse legacy .doc files and may need system packages (antiword/catdoc)."
        ) from e

    txt = textract.process(io.BytesIO(data), extension="doc")  # type: ignore
    try:
        return txt.decode("utf-8", errors="replace")
    except Exception:
        return str(txt)

# --- Helpers for ZIP traversal ---
def nearest_folder_from_path(path: str) -> Optional[str]:
    """
    Return the nearest enclosing folder (last folder before the filename),
    or None if the file is at the ZIP root.
    """
    segments = [seg for seg in path.split("/") if seg and seg != "."]
    return segments[-2] if len(segments) >= 2 else None

def extract_folder_file_pairs(zf: zipfile.ZipFile) -> Iterable[Tuple[str, zipfile.ZipInfo]]:
    """Yield (folder_name, zipinfo) for supported files under their nearest enclosing folder (or ROOT)."""
    for info in zf.infolist():
        if info.is_dir():
            continue
        path = info.filename.replace("\\", "/")
        folder = nearest_folder_from_path(path) or "ROOT"
        ext = path.lower().rsplit(".", 1)[-1] if "." in path else ""
        if ext in {"docx", "doc", "xls", "xlsx"}:
            yield folder, info

def harvest_ids_from_zip(zf: zipfile.ZipFile) -> Tuple[pd.DataFrame, List[str]]:
    """
    Return (DataFrame with columns: Folder, ID, file_order, row_order, warnings).
    - Preserves file traversal order within each folder (file_order).
    - Preserves ID appearance order within each file (row_order).
    - De-duplicates per (Folder, ID) keeping the earliest by file_order then row_order.
    """
    rows = []
    warnings: List[str] = []

    # Track per-folder file order in the sequence we encounter files in the ZIP
    folder_file_order: dict[str, dict[str, int]] = {}

    for folder, info in extract_folder_file_pairs(zf):
        path = info.filename.replace("\\", "/")
        filename = path.split("/")[-1]
        ext = path.lower().rsplit(".", 1)[-1]

        try:
            data = zf.read(info)
        except Exception as e:
            warnings.append(f"Could not read '{path}': {e}")
            continue

        text = ""
        try:
            if ext == "docx":
                text = read_docx_bytes(data)
            elif ext == "doc":
                text = read_doc_bytes(data)  # may warn if textract missing
            elif ext == "xlsx":
                text = read_xlsx_bytes(data)
            elif ext == "xls":
                text = read_xls_bytes(data)
        except Exception as e:
            warnings.append(f"Could not parse '{path}': {e}")
            continue

        ids = extract_ids_from_text(text)
        if not ids:
            warnings.append(f"No IDs found in '{path}'.")
            continue

        # Assign/order this file within its folder
        folder_file_order.setdefault(folder, {})
        if filename not in folder_file_order[folder]:
            folder_file_order[folder][filename] = len(folder_file_order[folder])  # 0,1,2,...

        file_order = folder_file_order[folder][filename]

        # Record each ID with “row_order” = sequence within this file
        for row_order, pid in enumerate(ids):
            rows.append({
                "Folder": folder,
                "ID": pid,
                "file_order": file_order,
                "row_order": row_order,
                # keep filename only internally for tie-breaks if ever needed
                "_source": filename,
            })

    if not rows:
        df = pd.DataFrame(columns=["Folder", "ID", "file_order", "row_order"])
        return df, warnings

    df = pd.DataFrame(rows)

    # De-dupe per (Folder, ID) keeping earliest by file_order -> row_order -> first seen
    df = (
        df.sort_values(["Folder", "file_order", "row_order", "_source"])
          .drop_duplicates(subset=["Folder", "ID"], keep="first")
          .reset_index(drop=True)
    )

    # No need to expose source filename
    df = df.drop(columns=["_source"])
    return df, warnings

from openpyxl import load_workbook, Workbook
from openpyxl.styles import PatternFill, Font
from openpyxl.utils import get_column_letter

YELLOW_FILL = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
BLACK_FILL  = PatternFill(start_color="000000", end_color="000000", fill_type="solid")

def _sanitize_sheet_title(name: str) -> str:
    # Excel sheet title constraints
    bad = set('[]:*?/\\')
    clean = "".join(ch for ch in str(name) if ch not in bad).strip()
    return (clean or "Folder")[:31]

def _is_solid_fill(cell) -> bool:
    try:
        return cell.fill is not None and cell.fill.fill_type == "solid"
    except Exception:
        return False

def _is_separator_row(ws, row_idx: int) -> bool:
    # A separator row is the full black row we created across A:B
    a = ws[f"A{row_idx}"]; b = ws[f"B{row_idx}"]
    return _is_solid_fill(a) and _is_solid_fill(b)

def _parse_summary(ws_summary):
    """
    Parse the existing Summary sheet into ordered blocks:
    returns: blocks = [
      {"folder": str, "start": int, "end": int, "sep": int, "ids": [str, ...]}
    ]
    where rows [start..end] are ID rows (header is row 1), and 'sep' is the separator row index.
    """
    blocks = []
    max_row = ws_summary.max_row
    r = 2  # skip header
    while r <= max_row:
        # skip any empty/separator padding before a block
        while r <= max_row and _is_separator_row(ws_summary, r):
            r += 1
        while r <= max_row and (ws_summary[f"A{r}"].value in (None, "") and ws_summary[f"B{r}"].value in (None, "")):
            r += 1
        if r > max_row:
            break

        # start of a block
        folder = ws_summary[f"A{r}"].value
        # if the first ID row has blank folder (e.g., malformed), forward-fill from above
        if not folder and r > 2:
            # find last seen folder upwards
            rr = r - 1
            while rr >= 2 and ws_summary[f"A{rr}"].value in (None, ""):
                rr -= 1
            folder = ws_summary[f"A{rr}"].value

        start = r
        ids = []
        # collect until separator or end
        while r <= max_row and not _is_separator_row(ws_summary, r):
            id_val = ws_summary[f"B{r}"].value
            if id_val not in (None, ""):
                ids.append(str(id_val))
            r += 1
        end = r - 1
        sep = r if (r <= max_row and _is_separator_row(ws_summary, r)) else None
        if sep:
            r += 1  # move past separator
        blocks.append({"folder": str(folder) if folder is not None else "", "start": start, "end": end, "sep": sep, "ids": ids})
    return blocks

def _ensure_provenance_sheet(wb):
    # no-op here; kept for symmetry if you add provenance later
    return

def _append_ids_to_folder_sheet(wb, folder: str, new_ids: list[str]):
    title = _sanitize_sheet_title(folder)
    if title in wb.sheetnames:
        ws = wb[title]
        # append below last row
        for pid in new_ids:
            ws.append([pid])
    else:
        ws = wb.create_sheet(title=title)
        ws.append(["ID"])
        for pid in new_ids:
            ws.append([pid])
        ws.column_dimensions["A"].width = 20
        ws.freeze_panes = "A2"

def update_existing_workbook(existing_xlsx: bytes, update_df: pd.DataFrame) -> bytes:
    """
    existing_xlsx: the original Excel you previously generated.
    update_df: DataFrame with columns [Folder, ID, file_order, row_order] (from harvest_ids_from_zip on the update ZIP).

    Behaviour:
      • Only adds IDs that are not already present under the same Folder.
      • For existing Folders: insert new ID rows just before that folder's separator row.
      • For new Folders: append a new block at the bottom (Folder shown once, then IDs, then a black separator).
      • Newly added rows in Summary are highlighted in yellow.
      • Per-folder sheets are created/updated accordingly.
    """
    wb = load_workbook(io.BytesIO(existing_xlsx))
    if "Summary" not in wb.sheetnames:
        raise RuntimeError("The uploaded Excel has no 'Summary' sheet.")

    ws = wb["Summary"]
    blocks = _parse_summary(ws)

    # Build current index: folder -> set of IDs, plus quick locators
    folder_to_ids = {b["folder"]: set(b["ids"]) for b in blocks}
    folder_order   = [b["folder"] for b in blocks]  # preserve display order

    # Prepare additions: folder -> [new_ids in desired order]
    # Keep update_df order (file_order then row_order)
    upd_sorted = update_df.sort_values(["Folder", "file_order", "row_order"])
    additions: dict[str, list[str]] = {}
    for folder, sub in upd_sorted.groupby("Folder", sort=False):
        have = folder_to_ids.get(folder, set())
        to_add = [pid for pid in sub["ID"].astype(str).tolist() if pid not in have]
        if to_add:
            additions[folder] = to_add

    # Fast exit if nothing to add
    if not additions:
        out = io.BytesIO()
        wb.save(out)
        return out.getvalue()

    # 1) Update existing folders: insert new rows before their separator
    # We must adjust row indexes as we insert; so we process blocks bottom-up
    blocks_by_folder = {b["folder"]: b for b in blocks}
    for folder in reversed(folder_order):
        if folder not in additions:
            continue
        new_ids = additions[folder]
        b = blocks_by_folder[folder]
        insert_at = (b["sep"] if b["sep"] else b["end"] + 1)  # insert before separator, or at end if no sep
        # Insert N rows
        ws.insert_rows(insert_at, amount=len(new_ids))
        # Write the new ID rows (folder col blank except optionally the first line—keep consistent with your format)
        r = insert_at
        for i, pid in enumerate(new_ids, start=0):
            ws[f"A{r}"].value = ""  # keep folder label only on the first original row
            ws[f"B{r}"].value = pid
            ws[f"A{r}"].fill = YELLOW_FILL
            ws[f"B{r}"].fill = YELLOW_FILL
            r += 1
        # Shift recorded indices for all following blocks
        shift = len(new_ids)
        for bb in blocks:
            if bb["start"] >= insert_at:
                bb["start"] += shift
            if bb["end"] >= insert_at:
                bb["end"] += shift
            if bb["sep"] and bb["sep"] >= insert_at:
                bb["sep"] += shift
        # Update this folder's block ids set
        folder_to_ids[folder].update(new_ids)
        # Update folder sheet
        _append_ids_to_folder_sheet(wb, folder, new_ids)

    # 2) New folders: append at bottom (after the final content)
    new_folders = [f for f in additions.keys() if f not in folder_to_ids]
    if new_folders:
        # Find bottom index (after removing trailing empty area)
        last_row = ws.max_row
        # Ensure there is at least a blank line before adding (optional)
        # Append each new folder block
        for folder in new_folders:
            new_ids = additions[folder]
            # Write block: first row shows folder name, first ID; subsequent rows blank in col A
            # Row 1: header exists already
            # Start writing at last_row + 1
            row = last_row + 1
            if not new_ids:
                continue
            ws.cell(row=row, column=1, value=folder)
            ws.cell(row=row, column=2, value=new_ids[0])
            ws[f"A{row}"].fill = YELLOW_FILL
            ws[f"B{row}"].fill = YELLOW_FILL
            row += 1
            for pid in new_ids[1:]:
                ws.cell(row=row, column=1, value="")
                ws.cell(row=row, column=2, value=pid)
                ws[f"A{row}"].fill = YELLOW_FILL
                ws[f"B{row}"].fill = YELLOW_FILL
                row += 1
            # Separator row
            ws.cell(row=row, column=1, value="")
            ws.cell(row=row, column=2, value="")
            ws[f"A{row}"].fill = BLACK_FILL
            ws[f"B{row}"].fill = BLACK_FILL
            last_row = row
            # Create per-folder sheet
            _append_ids_to_folder_sheet(wb, folder, new_ids)

    # Keep basic cosmetics (header freeze remains)
    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()

def to_excel_bytes(df: pd.DataFrame) -> bytes:
    """
    Build an Excel workbook:
      - Summary sheet: two columns [Folder, ID].
        * For each folder, write the folder number only once next to the first ID.
        * Insert a full black separator row after each folder block.
      - One sheet per folder: single column [ID].
      - IDs are ordered by file traversal order, then by appearance within the file.
    """
    from openpyxl import Workbook
    from openpyxl.styles import PatternFill

    # Ensure ordering by (Folder -> file_order -> row_order)
    df_sorted = df.sort_values(["Folder", "file_order", "row_order"]).reset_index(drop=True)

    wb = Workbook()
    ws_summary = wb.active
    ws_summary.title = "Summary"

    # Headers
    ws_summary.append(["Folder", "ID"])

    # Write grouped blocks
    black_fill = PatternFill(start_color="000000", end_color="000000", fill_type="solid")

    for folder, sub in df_sorted.groupby("Folder", sort=True):
        sub = sub.sort_values(["file_order", "row_order"])
        first = True
        for _, row in sub.iterrows():
            ws_summary.append([folder if first else "", row["ID"]])
            first = False
        # Separator row (full black across used columns A:B)
        sep_row_idx = ws_summary.max_row + 1
        ws_summary.append(["", ""])
        for col in ("A", "B"):
            ws_summary[f"{col}{sep_row_idx}"].fill = black_fill

    # Auto-width (simple)
    for ws in [ws_summary]:
        for col_cells in ws.columns:
            length = max(len(str(c.value)) if c.value is not None else 0 for c in col_cells)
            ws.column_dimensions[col_cells[0].column_letter].width = min(max(length + 2, 10), 40)

    # Per-folder sheets: only IDs, ordered by file then within-file order
    for folder, sub in df_sorted.groupby("Folder", sort=True):
        ws = wb.create_sheet(title=_sanitize_sheet_title(folder))
        ws.append(["ID"])
        for _, row in sub.iterrows():
            ws.append([row["ID"]])
        # Basic width
        ws.column_dimensions["A"].width = 20

    # Remove the trailing all-black separator at end if you prefer not to have a final one:
    # (Uncomment to drop if desired)
    # last = ws_summary.max_row
    # if last >= 2 and all((ws_summary[f"A{last}"].fill == black_fill,
    #                       ws_summary[f"B{last}"].fill == black_fill)):
    #     ws_summary.delete_rows(last, 1)

    # Save to bytes
    output = io.BytesIO()
    wb.save(output)
    return output.getvalue()

# --- Streamlit UI ---
st.set_page_config(page_title="ID Harvester to Excel", page_icon="📄", layout="wide")
st.title("📄 ID Harvester → Excel")
st.caption(
    "Upload a ZIP of your folder. Each numbered subfolder may contain .docx, .doc, .xls, or .xlsx files. "
    "The app extracts 9-digit IDs beginning with 4/8/9 and groups them by folder."
)

st.subheader("Mode")
tab_create, tab_update = st.tabs(["Create from ZIP", "Update existing Excel with ZIP"])

with tab_create:
    base_zip = st.file_uploader("Upload a ZIP archive (create new Excel)", type=["zip"], key="create_zip")
    if base_zip is not None:
        try:
            with zipfile.ZipFile(base_zip) as zf:
                df, warnings = harvest_ids_from_zip(zf)

            if df.empty:
                st.warning("No IDs were found. Please check your ZIP structure and file contents.")
            else:
                st.subheader("Preview")
                st.dataframe(df, use_container_width=True)

                excel_bytes = to_excel_bytes(df)
                st.download_button(
                    label="⬇️ Download Excel",
                    data=excel_bytes,
                    file_name="ids_by_folder.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

            if warnings:
                st.divider()
                st.subheader("Warnings")
                for w in warnings:
                    st.write("• " + w)

        except zipfile.BadZipFile:
            st.error("The uploaded file is not a valid ZIP archive.")
        except Exception as e:
            st.exception(e)

with tab_update:
    existing_xlsx = st.file_uploader("Upload the existing Excel to update (.xlsx)", type=["xlsx"], key="existing_xlsx")
    update_zip    = st.file_uploader("Upload the update ZIP archive", type=["zip"], key="update_zip")

    if existing_xlsx is not None and update_zip is not None:
        try:
            with zipfile.ZipFile(update_zip) as zf:
                upd_df, upd_warnings = harvest_ids_from_zip(zf)

            if upd_df.empty:
                st.warning("No IDs were found in the update ZIP.")
            else:
                # Show what will be applied (per folder unique new IDs after comparing will be computed inside updater)
                st.subheader("Update ZIP Preview")
                st.dataframe(upd_df, use_container_width=True)

                updated_bytes = update_existing_workbook(existing_xlsx.read(), upd_df)
                st.download_button(
                    label="⬇️ Download Updated Excel",
                    data=updated_bytes,
                    file_name="ids_by_folder_updated.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

            if upd_warnings:
                st.divider()
                st.subheader("Warnings (update ZIP)")
                for w in upd_warnings:
                    st.write("• " + w)

        except zipfile.BadZipFile:
            st.error("The update file is not a valid ZIP archive.")
        except Exception as e:
            st.exception(e)
