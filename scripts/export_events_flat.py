import argparse
import csv
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

PROJECT_DIR = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_DIR / "statsbomb_raw.sqlite"


# Output schema
COLUMNS = [
    # ids
    "match_id", "event_id", "index_in_file",
    # time
    "period", "timestamp", "minute", "second",
    # context
    "event_type", "possession", "play_pattern",
    # entities
    "team_id", "player_id",
    # locations
    "x", "y", "end_x", "end_y",
    # pass
    "pass_length", "pass_height", "pass_outcome", "pass_cross", "pass_switch",
    # shot
    "shot_outcome", "shot_body_part", "shot_type", "shot_xg",
    # carry
    "carry_length",
    # duel / foul
    "duel_type", "foul_committed", "foul_won",
]


def safe_get(d: Any, *keys: str) -> Any:
    """Safely get nested dict values; returns None if missing."""
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
        if cur is None:
            return None
    return cur


def get_location_xy(loc: Any) -> Tuple[Optional[float], Optional[float]]:
    if isinstance(loc, list) and len(loc) >= 2:
        return loc[0], loc[1]
    return None, None


def flatten_event(ev: Dict[str, Any], match_id: int, index_in_file: Optional[int]) -> Dict[str, Any]:
    # Common
    event_type = safe_get(ev, "type", "name")
    team_id = safe_get(ev, "team", "id")
    player_id = safe_get(ev, "player", "id")

    x, y = get_location_xy(ev.get("location"))

    row: Dict[str, Any] = {c: None for c in COLUMNS}
    row.update({
        "match_id": match_id,
        "event_id": ev.get("id"),
        "index_in_file": index_in_file,
        "period": ev.get("period"),
        "timestamp": ev.get("timestamp"),
        "minute": ev.get("minute"),
        "second": ev.get("second"),
        "event_type": event_type,
        "possession": ev.get("possession"),
        "play_pattern": safe_get(ev, "play_pattern", "name"),
        "team_id": team_id,
        "player_id": player_id,
        "x": x,
        "y": y,
    })

    # PASS
    if event_type == "Pass" and isinstance(ev.get("pass"), dict):
        p = ev["pass"]
        end_x, end_y = get_location_xy(p.get("end_location"))
        row["end_x"] = end_x
        row["end_y"] = end_y
        row["pass_length"] = p.get("length")
        row["pass_height"] = safe_get(p, "height", "name")
        row["pass_outcome"] = safe_get(p, "outcome", "name")
        row["pass_cross"] = p.get("cross")
        row["pass_switch"] = p.get("switch")

    # SHOT
    elif event_type == "Shot" and isinstance(ev.get("shot"), dict):
        s = ev["shot"]
        end_x, end_y = get_location_xy(s.get("end_location"))
        row["end_x"] = end_x
        row["end_y"] = end_y
        row["shot_outcome"] = safe_get(s, "outcome", "name")
        row["shot_body_part"] = safe_get(s, "body_part", "name")
        row["shot_type"] = safe_get(s, "type", "name")
        row["shot_xg"] = s.get("statsbomb_xg")

    # CARRY
    elif event_type == "Carry" and isinstance(ev.get("carry"), dict):
        c = ev["carry"]
        end_x, end_y = get_location_xy(c.get("end_location"))
        row["end_x"] = end_x
        row["end_y"] = end_y
        # simple carry length
        if x is not None and y is not None and end_x is not None and end_y is not None:
            dx = float(end_x) - float(x)
            dy = float(end_y) - float(y)
            row["carry_length"] = (dx * dx + dy * dy) ** 0.5

    # DUEL
    if event_type == "Duel" and isinstance(ev.get("duel"), dict):
        d = ev["duel"]
        row["duel_type"] = safe_get(d, "type", "name")

    # FOUL
    if event_type == "Foul Committed":
        row["foul_committed"] = True
    if event_type == "Foul Won":
        row["foul_won"] = True

    return row


def chunked(iterable: Iterable[Any], size: int) -> Iterable[List[Any]]:
    buf: List[Any] = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def fetch_match_ids(conn: sqlite3.Connection, competition_id: int, season_id: int) -> List[int]:
    cur = conn.cursor()
    cur.execute(
        "SELECT match_id FROM matches_raw WHERE competition_id=? AND season_id=?",
        (competition_id, season_id),
    )
    return [r[0] for r in cur.fetchall()]


def ensure_output_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_csv(rows: List[Dict[str, Any]], out_path: Path, write_header: bool) -> None:
    ensure_output_dir(out_path)
    mode = "w" if write_header else "a"
    with out_path.open(mode, newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        if write_header:
            w.writeheader()
        w.writerows(rows)


def parquet_available() -> bool:
    try:
        import pyarrow
        import pandas
        return True
    except Exception:
        return False


def write_parquet_batch(rows: List[Dict[str, Any]], out_path: Path, first_batch: bool) -> None:
    """
    Writes Parquet in batches. We use pandas+pyarrow.
    If file exists, we append by writing a new file per batch (simple & robust),
    then you can concatenate later. To keep MVP usable, we write:
      - events_flat_..._part000.parquet, part001.parquet, ...
    """
    import pandas as pd

    ensure_output_dir(out_path)
    df = pd.DataFrame(rows, columns=COLUMNS)
    df.to_parquet(out_path, index=False)


def export_events_flat(
    competition_id: int,
    season_id: int,
    out_dir: Path,
    limit_players: Optional[List[int]] = None,
    batch_size: int = 200_000,
) -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB not found: {DB_PATH}. Run ingest first.")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    match_ids = fetch_match_ids(conn, competition_id, season_id)
    if not match_ids:
        conn.close()
        raise ValueError(f"No matches found for competition_id={competition_id}, season_id={season_id}")

    print(f"Selected league/season: competition_id={competition_id}, season_id={season_id}")
    print(f"Matches found: {len(match_ids)}")

    # Create temp table for fast join rather than huge IN
    cur.execute("DROP TABLE IF EXISTS temp_selected_matches;")
    cur.execute("CREATE TEMP TABLE temp_selected_matches (match_id INTEGER PRIMARY KEY);")
    cur.executemany("INSERT INTO temp_selected_matches(match_id) VALUES (?);", [(mid,) for mid in match_ids])
    conn.commit()

    # Optional players filter via temp table
    if limit_players:
        cur.execute("DROP TABLE IF EXISTS temp_selected_players;")
        cur.execute("CREATE TEMP TABLE temp_selected_players (player_id INTEGER PRIMARY KEY);")
        cur.executemany("INSERT INTO temp_selected_players(player_id) VALUES (?);", [(pid,) for pid in limit_players])
        conn.commit()
        player_join = "JOIN temp_selected_players p ON e.player_id = p.player_id"
        print(f"Player filter enabled: {len(limit_players)} players")
    else:
        player_join = ""

    # Count events
    count_sql = f"""
        SELECT COUNT(*)
        FROM events_raw e
        JOIN temp_selected_matches m ON e.match_id = m.match_id
        {player_join}
    """
    cur.execute(count_sql)
    total_events = cur.fetchone()[0]
    print(f"Events to export: {total_events}")

    base_name = f"events_flat_league_{competition_id}_{season_id}"
    out_dir.mkdir(parents=True, exist_ok=True)

    use_parquet = parquet_available()
    if use_parquet:
        print("Parquet support detected (pandas+pyarrow). Will write partitioned Parquet files.")
    else:
        print("Parquet not available (missing pandas/pyarrow). Will write a single CSV file.")

    # Stream rows from SQLite
    select_sql = f"""
        SELECT e.match_id, e.event_id, e.index_in_file, e.json
        FROM events_raw e
        JOIN temp_selected_matches m ON e.match_id = m.match_id
        {player_join}
        ORDER BY e.match_id, e.index_in_file
    """
    cur.execute(select_sql)

    exported = 0
    part = 0
    header_written = False
    csv_path = out_dir / f"{base_name}.csv"

    while True:
        chunk = cur.fetchmany(50_000)
        if not chunk:
            break

        rows: List[Dict[str, Any]] = []
        for match_id, event_id, index_in_file, js in chunk:
            try:
                ev = json.loads(js)
            except Exception:
                continue
            row = flatten_event(ev, match_id=match_id, index_in_file=index_in_file)
            # redundancy ensure ids align
            row["event_id"] = event_id
            rows.append(row)

        # Write out in big batches
        for batch in chunked(rows, batch_size):
            if use_parquet:
                part_path = out_dir / f"{base_name}_part{part:03d}.parquet"
                write_parquet_batch(batch, part_path, first_batch=(part == 0))
                part += 1
            else:
                write_csv(batch, csv_path, write_header=(not header_written))
                header_written = True

            exported += len(batch)
            if total_events:
                pct = exported / total_events * 100
                print(f"Exported {exported}/{total_events} ({pct:.1f}%)")

    conn.close()

    if use_parquet:
        print(f"Done. Wrote {part} parquet part files to: {out_dir}")
        print(f"Pattern: {base_name}_partXXX.parquet")
    else:
        print(f"Done. Wrote CSV to: {csv_path}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export StatsBomb events to a flat, player-centric table.")
    p.add_argument("--competition-id", type=int, required=True)
    p.add_argument("--season-id", type=int, required=True)
    p.add_argument("--out-dir", type=str, default=str(PROJECT_DIR / "output"))
    p.add_argument("--players", type=str, default="", help="Comma-separated player_ids to filter (optional).")
    p.add_argument("--batch-size", type=int, default=200_000, help="Rows per output batch (parquet parts / csv chunks).")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    out_dir = Path(args.out_dir)

    players: Optional[List[int]] = None
    if args.players.strip():
        players = [int(x.strip()) for x in args.players.split(",") if x.strip()]

    export_events_flat(
        competition_id=args.competition_id,
        season_id=args.season_id,
        out_dir=out_dir,
        limit_players=players,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()
