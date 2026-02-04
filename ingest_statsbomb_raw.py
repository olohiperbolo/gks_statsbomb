import json
import sqlite3
from pathlib import Path
from datetime import datetime


PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data" / "statsbomb_open_data" / "data"

MATCHES_DIR = DATA_DIR / "matches"
EVENTS_DIR = DATA_DIR / "events"

DB_PATH = PROJECT_DIR / "statsbomb_raw.sqlite"


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def ensure_schema(conn: sqlite3.Connection):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS matches_raw (
          match_id       INTEGER PRIMARY KEY,
          competition_id INTEGER,
          season_id      INTEGER,
          match_date     TEXT,
          home_team_id   INTEGER,
          away_team_id   INTEGER,
          json           TEXT NOT NULL,
          source_file    TEXT,
          ingested_at    TEXT
        );

        CREATE TABLE IF NOT EXISTS events_raw (
          match_id       INTEGER NOT NULL,
          event_id       TEXT NOT NULL,
          index_in_file  INTEGER,
          period         INTEGER,
          timestamp      TEXT,
          minute         INTEGER,
          second         INTEGER,
          type_name      TEXT,
          possession     INTEGER,
          team_id        INTEGER,
          player_id      INTEGER,
          json           TEXT NOT NULL,
          source_file    TEXT,
          ingested_at    TEXT,
          PRIMARY KEY (match_id, event_id)
        );

        CREATE INDEX IF NOT EXISTS idx_events_match_id ON events_raw(match_id);
        CREATE INDEX IF NOT EXISTS idx_events_player_id ON events_raw(player_id);
        CREATE INDEX IF NOT EXISTS idx_events_type_name ON events_raw(type_name);
        """
    )


def upsert_match(conn, match, competition_id, season_id, source_file, ingested_at):
    match_id = match.get("match_id")
    match_date = match.get("match_date")
    home_team_id = (match.get("home_team") or {}).get("home_team_id")
    away_team_id = (match.get("away_team") or {}).get("away_team_id")

    conn.execute(
        """
        INSERT OR REPLACE INTO matches_raw
        (match_id, competition_id, season_id, match_date, home_team_id, away_team_id, json, source_file, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            match_id,
            competition_id,
            season_id,
            match_date,
            home_team_id,
            away_team_id,
            json.dumps(match, ensure_ascii=False),
            source_file,
            ingested_at,
        ),
    )


def insert_events(conn, match_id, events, source_file, ingested_at):
    rows = []
    for idx, ev in enumerate(events):
        event_id = ev.get("id")
        period = ev.get("period")
        timestamp = ev.get("timestamp")
        minute = ev.get("minute")
        second = ev.get("second")
        type_name = (ev.get("type") or {}).get("name")
        possession = ev.get("possession")
        team_id = (ev.get("team") or {}).get("id")
        player_id = (ev.get("player") or {}).get("id")

        rows.append(
            (
                match_id,
                event_id,
                idx,
                period,
                timestamp,
                minute,
                second,
                type_name,
                possession,
                team_id,
                player_id,
                json.dumps(ev, ensure_ascii=False),
                source_file,
                ingested_at,
            )
        )

    conn.executemany(
        """
        INSERT OR REPLACE INTO events_raw
        (match_id, event_id, index_in_file, period, timestamp, minute, second,
         type_name, possession, team_id, player_id, json, source_file, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def main():
    # --- Debug prints to avoid "I don't know where it's looking" ---
    print("PROJECT_DIR:", PROJECT_DIR)
    print("DATA_DIR:", DATA_DIR)
    print("MATCHES_DIR exists:", MATCHES_DIR.exists(), "| path:", MATCHES_DIR)
    print("EVENTS_DIR exists:", EVENTS_DIR.exists(), "| path:", EVENTS_DIR)
    print("DB_PATH:", DB_PATH)

    if not MATCHES_DIR.exists():
        raise FileNotFoundError(
            f"Matches dir not found: {MATCHES_DIR}\n"
            f"Expected StatsBomb submodule at: {PROJECT_DIR / 'data' / 'statsbomb_open_data'}"
        )

    ingested_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    conn = sqlite3.connect(DB_PATH)
    ensure_schema(conn)

    match_files = sorted(MATCHES_DIR.glob("*/*.json"))
    print(f"Found match files: {len(match_files)}")

    imported_match_files = 0
    imported_events_files = 0

    for mf in match_files:
        competition_id = int(mf.parent.name)
        season_id = int(mf.stem)

        matches = load_json(mf)

        # 1) Matches
        for m in matches:
            upsert_match(conn, m, competition_id, season_id, str(mf), ingested_at)

        # 2) Events for each match (if present)
        for m in matches:
            match_id = m["match_id"]
            ev_path = EVENTS_DIR / f"{match_id}.json"
            if not ev_path.exists():
                continue
            events = load_json(ev_path)
            insert_events(conn, match_id, events, str(ev_path), ingested_at)
            imported_events_files += 1

        conn.commit()
        imported_match_files += 1
        print(f"Imported match-file: {mf} (competition={competition_id}, season={season_id})")

    conn.close()
    print(f"Done. Imported match files: {imported_match_files}, event files: {imported_events_files}")
    print(f"SQLite DB saved as: {DB_PATH}")


if __name__ == "__main__":
    main()
