import os
import sqlite3
from pathlib import Path
from typing import List, Tuple, Dict, Any

import pandas as pd
from flask import Flask, render_template, request, flash

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "flights.db"

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "safra-duplicates-secret")


def initialize_database() -> None:
    """Create the SQLite database with sample data when it does not exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS flights (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            airline TEXT NOT NULL,
            flight_number TEXT NOT NULL,
            origin TEXT NOT NULL,
            destination TEXT NOT NULL,
            departure_time TEXT NOT NULL,
            status TEXT NOT NULL
        )
        """
    )
    cursor.execute("SELECT COUNT(*) FROM flights")
    count = cursor.fetchone()[0]
    if count >= 100:
        conn.close()
        return

    cursor.execute("DELETE FROM flights")

    airlines = [
        "Safra Air",
        "Aurora Airways",
        "Atlântico Linhas",
        "Vento Azul",
        "NorteFly"
    ]
    origins = ["GRU", "CGH", "GIG", "BSB", "CNF", "SSA", "REC", "POA", "FLN", "BEL"]
    destinations = ["GRU", "CGH", "GIG", "BSB", "CNF", "SSA", "REC", "POA", "FLN", "BEL"]
    statuses = ["No Horário", "Atrasado", "Embarque", "Cancelado", "Concluído"]

    from datetime import datetime, timedelta
    import random

    random.seed(42)
    start_time = datetime(2024, 1, 1, 6, 0)

    rows = []
    for idx in range(100):
        airline = random.choice(airlines)
        flight_number = f"{random.randint(100, 999)}"
        origin = random.choice(origins)
        destination = random.choice([d for d in destinations if d != origin])
        departure = start_time + timedelta(minutes=30 * random.randint(0, 96))
        status = random.choice(statuses)

        rows.append((airline, flight_number, origin, destination, departure.isoformat(), status))

    # Introduce deterministic duplicates for demo purposes
    duplicates = [
        ("Safra Air", "202", "GRU", "GIG", start_time.isoformat(), "No Horário"),
        ("Safra Air", "202", "GRU", "GIG", start_time.isoformat(), "Atrasado"),
        ("Aurora Airways", "450", "BSB", "REC", (start_time + timedelta(hours=2)).isoformat(), "Embarque"),
        ("Aurora Airways", "450", "BSB", "REC", (start_time + timedelta(hours=2)).isoformat(), "Embarque"),
        ("Vento Azul", "301", "SSA", "CGH", (start_time + timedelta(hours=5)).isoformat(), "Cancelado"),
        ("Vento Azul", "301", "SSA", "CGH", (start_time + timedelta(hours=5)).isoformat(), "Cancelado"),
    ]

    rows[: len(duplicates)] = duplicates

    cursor.executemany(
        """
        INSERT INTO flights (airline, flight_number, origin, destination, departure_time, status)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )

    conn.commit()
    conn.close()


def run_query(query: str) -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()
    return df


def compute_duplicates(df: pd.DataFrame, keys: List[str]) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    if df.empty:
        return df, []

    duplicates_mask = df.duplicated(subset=keys, keep=False)
    duplicates_df = df.loc[duplicates_mask].copy()

    if duplicates_df.empty:
        return duplicates_df, []

    duplicates_df.sort_values(keys, inplace=True)
    duplicates_df["ordem"] = duplicates_df.groupby(keys).cumcount() + 1
    columns = ["ordem"] + [col for col in duplicates_df.columns if col != "ordem"]
    duplicates_df = duplicates_df[columns]

    difference_summary: List[Dict[str, Any]] = []
    non_key_columns = [col for col in df.columns if col not in keys]

    for group_keys, group in duplicates_df.groupby(keys):
        if not isinstance(group_keys, tuple):
            group_keys = (group_keys,)

        diff_columns = [col for col in non_key_columns if group[col].nunique() > 1]
        difference_summary.append(
            {
                "key": group_keys,
                "duplicate_count": len(group),
                "difference_columns": diff_columns,
            }
        )

    return duplicates_df, difference_summary


def build_query_from_input(source: str) -> str:
    cleaned = source.strip()
    if not cleaned:
        raise ValueError("Informe uma tabela ou uma consulta SQL válida.")
    lowered = cleaned.lower()
    if lowered.startswith("select") or lowered.startswith("with"):
        return cleaned
    return f"SELECT * FROM {cleaned}"


@app.route("/", methods=["GET", "POST"])
def index():
    initialize_database()

    query_input = ""
    selected_keys: List[str] = []
    duplicates_table = None
    difference_summary = []
    available_columns: List[str] = []
    selected_keys_label = ""

    if request.method == "POST":
        query_input = request.form.get("query", "")
        selected_keys = request.form.getlist("selected_keys")

        try:
            query = build_query_from_input(query_input)
        except ValueError as exc:
            flash(str(exc), "danger")
            return render_template(
                "index.html",
                query=query_input,
                selected_keys=selected_keys,
                duplicates_table=None,
                difference_summary=[],
                columns=[],
                available_columns=[],
                selected_keys_label="",
            )

        try:
            df = run_query(query)
        except Exception as exc:  # pragma: no cover - sqlite errors
            flash(f"Erro ao executar a consulta: {exc}", "danger")
            return render_template(
                "index.html",
                query=query_input,
                selected_keys=selected_keys,
                duplicates_table=None,
                difference_summary=[],
                columns=[],
                available_columns=[],
                selected_keys_label="",
            )

        available_columns = df.columns.tolist()

        if not selected_keys:
            flash(
                "Selecione ao menos uma coluna para identificar as duplicidades.",
                "info",
            )
            return render_template(
                "index.html",
                query=query_input,
                selected_keys=selected_keys,
                duplicates_table=None,
                difference_summary=[],
                columns=[],
                available_columns=available_columns,
                selected_keys_label="",
            )

        missing_keys = [key for key in selected_keys if key not in df.columns]
        if missing_keys:
            flash(
                "Colunas não encontradas no resultado: " + ", ".join(missing_keys),
                "danger",
            )
            return render_template(
                "index.html",
                query=query_input,
                selected_keys=selected_keys,
                duplicates_table=None,
                difference_summary=[],
                columns=[],
                available_columns=available_columns,
                selected_keys_label="",
            )

        selected_keys_label = ", ".join(selected_keys)

        duplicates_df, difference_summary = compute_duplicates(df, selected_keys)

        if duplicates_df.empty:
            flash("Nenhuma duplicidade encontrada para as colunas informadas.", "info")
        else:
            duplicates_table = duplicates_df.to_dict(orient="records")

    columns = []
    if duplicates_table:
        columns = list(duplicates_table[0].keys())

    return render_template(
        "index.html",
        query=query_input,
        selected_keys=selected_keys,
        duplicates_table=duplicates_table,
        difference_summary=difference_summary,
        columns=columns,
        available_columns=available_columns,
        selected_keys_label=selected_keys_label,
    )


@app.context_processor
def inject_helpers():
    def format_key(key_tuple: Tuple[Any, ...]) -> str:
        return " | ".join(str(value) for value in key_tuple)

    return {"format_key": format_key}


# Placeholder for future Spark integration
# def analyze_with_spark(spark_df, keys: List[str]):
#     """Stub to highlight where Spark-based duplicate analysis will be integrated."""
#     raise NotImplementedError


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
