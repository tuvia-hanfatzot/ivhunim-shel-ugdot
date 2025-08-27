# streamlit_app.py
# Streamlit app: Upload a ZIP containing folders (each folder name is a number),
# read .docx/.doc/.xls/.xlsx files inside, extract 9-digit IDs starting with 4/8/9,
# and generate an Excel workbook grouped by folder name.

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
def is_numbered_folder(name: str) -> bool:
    """Folder names are numbers as per requirement (e.g., '1234')."""
    return name.isdigit()

def first_numbered_folder_from_path(path: str) -> Optional[str]:
    """
    Given an archive path like '1234/sub/a.docx', return the first path segment
    that is a purely numeric folder name. If none found, return None.
    """
    segments = [seg for seg in path.split("/") if seg and seg != "."]
    for seg in segments[:-1]:  # exclude filename
        if is_numbered_folder(seg):
            return seg
    return None

def extract_folder_file_pairs(zf: zipfile.ZipFile) -> Iterable[Tuple[str, zipfile.ZipInfo]]:
    """Yield (folder_name, zipinfo) for supported files underneath numbered folders."""
    for info in zf.infolist():
        if info.is_dir():
            continue
        # Normalise path separators to forward slash
        path = info.filename.replace("\\", "/")
        folder = first_numbered_folder_from_path(path)
        if folder is None:
            continue
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
        ws = wb.create_sheet(title=str(folder)[:31] if folder else "Folder")
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

zip_file = st.file_uploader("Upload a ZIP archive", type=["zip"])

with st.expander("Notes & Requirements", expanded=False):
    st.markdown(
        "- The root of the ZIP should contain subfolders named with **numbers** (e.g., `12345/`, `987/`).\n"
        "- Files inside those folders may be **.docx, .doc, .xls, .xlsx**.\n"
        "- Legacy **.doc** parsing uses `textract` if available; otherwise such files will be skipped with a warning.\n"
        "- Detected ID pattern: **9 digits starting with 4, 8, or 9**."
    )

if zip_file is not None:
    try:
        with zipfile.ZipFile(zip_file) as zf:
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

# --- (Optional) Side-tools: quick tester for the regex ---
with st.sidebar:
    st.header("🔎 Test an ID String")
    sample_text = st.text_area("Paste any text to test ID detection", height=120)
    if st.button("Detect IDs"):
        found = extract_ids_from_text(sample_text)
        if found:
            st.success(f"Found {len(found)} ID(s): " + ", ".join(found))
        else:
            st.info("No matching IDs found.")
