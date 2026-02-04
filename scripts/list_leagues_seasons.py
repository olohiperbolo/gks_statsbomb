import json
import sqlite3
from pathlib import Path
from collections import defaultdict

PROJECT_DIR = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_DIR / "statsbomb_raw.sqlite"


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB not found: {DB_PATH}. Run ingest first.")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Pull minimal fields
    cur.execute("""
        SELECT competition_id, season_id, match_date, json
        FROM matches_raw
    """)

    agg = defaultdict(lambda: {
        "matches": 0,
        "min_date": None,
        "max_date": None,
        "competition_name": None,
        "season_name": None,
        "country_name": None,
    })

    for competition_id, season_id, match_date, js in cur.fetchall():
        key = (competition_id, season_id)
        row = agg[key]
        row["matches"] += 1

        # dates
        if match_date:
            if row["min_date"] is None or match_date < row["min_date"]:
                row["min_date"] = match_date
            if row["max_date"] is None or match_date > row["max_date"]:
                row["max_date"] = match_date

        # names from JSON
        if row["competition_name"] is None or row["season_name"] is None or row["country_name"] is None:
            try:
                m = json.loads(js)
            except Exception:
                continue

            comp = (m.get("competition") or {})
            season = (m.get("season") or {})
            country = (m.get("country") or {})

            row["competition_name"] = row["competition_name"] or comp.get("competition_name") or comp.get("name")
            row["season_name"] = row["season_name"] or season.get("season_name") or season.get("name")
            row["country_name"] = row["country_name"] or country.get("name")

    conn.close()

    # Print as a sorted table
    rows = []
    for (competition_id, season_id), v in agg.items():
        rows.append({
            "competition_id": competition_id,
            "season_id": season_id,
            "competition_name": v["competition_name"] or "",
            "season_name": v["season_name"] or "",
            "country": v["country_name"] or "",
            "matches": v["matches"],
            "min_date": v["min_date"] or "",
            "max_date": v["max_date"] or "",
        })

    rows.sort(key=lambda r: (r["competition_name"], r["season_name"], r["competition_id"], r["season_id"]))

    # Pretty print 
    header = ["competition_id", "season_id", "competition_name", "season_name", "country", "matches", "min_date", "max_date"]
    print("\t".join(header))
    for r in rows:
        print("\t".join(str(r[h]) for h in header))

    print(f"\nTotal league/season pairs: {len(rows)}")


if __name__ == "__main__":
    main()
