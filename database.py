import sqlite3
import os
from datetime import datetime
from pypinyin import lazy_pinyin, Style
from config import DB_PATH


def to_pinyin(name):
    """Convert Chinese name to pinyin string for fuzzy matching.
    Returns both full pinyin and initials, e.g. 'yaozhihan yzh'"""
    if not name:
        return ""
    full = "".join(lazy_pinyin(name))
    initials = "".join(lazy_pinyin(name, style=Style.FIRST_LETTER))
    return f"{full} {initials}"


def get_connection(db_path=None):
    """Get a SQLite connection with WAL mode and foreign keys enabled."""
    path = db_path or DB_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path=None):
    """Create all tables if they don't exist."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS competitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season TEXT,
            name TEXT,
            venue TEXT,
            date TEXT,
            organizer TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            competition_id INTEGER,
            discipline TEXT,
            gender TEXT,
            age_group TEXT,
            round_type TEXT,
            source_file TEXT,
            FOREIGN KEY (competition_id) REFERENCES competitions(id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id INTEGER,
            rank INTEGER,
            bib TEXT,
            name TEXT,
            team TEXT,
            run1_time TEXT,
            run2_time TEXT,
            total_time TEXT,
            run1_seconds REAL,
            run2_seconds REAL,
            total_seconds REAL,
            time_diff TEXT,
            status TEXT DEFAULT 'OK',
            name_pinyin TEXT,
            FOREIGN KEY (event_id) REFERENCES events(id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            s3_key TEXT UNIQUE,
            file_type TEXT,
            processed_at TEXT,
            status TEXT,
            error_message TEXT
        )
    """)

    # Indexes for common queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_results_name ON results(name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_results_name_pinyin ON results(name_pinyin)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_results_event_id ON results(event_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_competition_id ON events(competition_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_processed_files_s3_key ON processed_files(s3_key)")

    # Ensure name_pinyin column exists (for databases created before this feature)
    try:
        cursor.execute("ALTER TABLE results ADD COLUMN name_pinyin TEXT")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # Column already exists

    conn.commit()
    conn.close()


def insert_competition(season, name, venue=None, date=None, organizer=None, db_path=None):
    """Insert a competition record, returning its id. If a matching competition
    already exists (same season + name), return the existing id."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    # Check for existing competition
    cursor.execute(
        "SELECT id FROM competitions WHERE season = ? AND name = ?",
        (season, name)
    )
    row = cursor.fetchone()
    if row:
        comp_id = row["id"]
        conn.close()
        return comp_id

    cursor.execute(
        "INSERT INTO competitions (season, name, venue, date, organizer) VALUES (?, ?, ?, ?, ?)",
        (season, name, venue, date, organizer)
    )
    comp_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return comp_id


def insert_event(competition_id, discipline, gender, age_group, round_type=None, source_file=None, db_path=None):
    """Insert an event record, returning its id. If an event with the same
    source_file already exists, return the existing id."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    # Check for existing event by source_file to prevent duplicates
    if source_file:
        cursor.execute(
            "SELECT id FROM events WHERE source_file = ?",
            (source_file,)
        )
        row = cursor.fetchone()
        if row:
            event_id = row["id"]
            conn.close()
            return event_id

    cursor.execute(
        "INSERT INTO events (competition_id, discipline, gender, age_group, round_type, source_file) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (competition_id, discipline, gender, age_group, round_type, source_file)
    )
    event_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return event_id


def insert_results(event_id, results_list, db_path=None):
    """Insert a list of result dicts for a given event_id.

    Each dict should have keys matching the results table columns:
    rank, bib, name, team, run1_time, run2_time, total_time,
    run1_seconds, run2_seconds, total_seconds, time_diff, status
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()
    for r in results_list:
        name = r.get("name")
        cursor.execute(
            "INSERT INTO results "
            "(event_id, rank, bib, name, team, run1_time, run2_time, total_time, "
            "run1_seconds, run2_seconds, total_seconds, time_diff, status, name_pinyin) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                event_id,
                r.get("rank"),
                r.get("bib"),
                name,
                r.get("team"),
                r.get("run1_time"),
                r.get("run2_time"),
                r.get("total_time"),
                r.get("run1_seconds"),
                r.get("run2_seconds"),
                r.get("total_seconds"),
                r.get("time_diff"),
                r.get("status", "OK"),
                to_pinyin(name),
            )
        )
    conn.commit()
    conn.close()


def mark_file_processed(s3_key, file_type, status, error_message=None, db_path=None):
    """Record that a file has been processed (success/failed/skipped)."""
    conn = get_connection(db_path)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR REPLACE INTO processed_files (s3_key, file_type, processed_at, status, error_message) "
        "VALUES (?, ?, ?, ?, ?)",
        (s3_key, file_type, datetime.utcnow().isoformat(), status, error_message)
    )
    conn.commit()
    conn.close()


def is_file_processed(s3_key, db_path=None):
    """Check if a file has already been processed successfully."""
    conn = get_connection(db_path)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT status FROM processed_files WHERE s3_key = ?",
        (s3_key,)
    )
    row = cursor.fetchone()
    conn.close()
    if row and row["status"] == "success":
        return True
    return False


def search_results(filters=None, db_path=None):
    """Search results with optional filters.

    filters dict can include: season, competition, discipline, age_group, gender, name
    Returns a list of dicts with full result + event + competition info.
    """
    filters = filters or {}
    conn = get_connection(db_path)
    cursor = conn.cursor()

    query = """
        SELECT
            r.rank, r.bib, r.name, r.team,
            r.run1_time, r.run2_time, r.total_time, r.time_diff, r.status,
            r.run1_seconds, r.run2_seconds, r.total_seconds,
            e.discipline, e.gender, e.age_group, e.round_type,
            c.season, c.name AS competition, c.venue, c.date
        FROM results r
        JOIN events e ON r.event_id = e.id
        JOIN competitions c ON e.competition_id = c.id
        WHERE 1=1
    """
    params = []

    if filters.get("season"):
        query += " AND c.season = ?"
        params.append(filters["season"])
    if filters.get("competition"):
        query += " AND c.name = ?"
        params.append(filters["competition"])
    if filters.get("discipline"):
        query += " AND e.discipline = ?"
        params.append(filters["discipline"])
    if filters.get("age_group"):
        query += " AND e.age_group = ?"
        params.append(filters["age_group"])
    if filters.get("gender"):
        query += " AND e.gender = ?"
        params.append(filters["gender"])
    if filters.get("name"):
        keyword = filters["name"]
        # Check if input is ASCII (pinyin) or Chinese
        if all(ord(c) < 128 for c in keyword.replace(" ", "")):
            # Pinyin search: match against name_pinyin field
            query += " AND r.name_pinyin LIKE ?"
            params.append(f"%{keyword.lower()}%")
        else:
            # Chinese character search
            query += " AND r.name LIKE ?"
            params.append(f"%{keyword}%")

    query += " ORDER BY c.date DESC, r.rank ASC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    result = [dict(row) for row in rows]
    conn.close()
    return result


def get_athlete_history(name, db_path=None):
    """Get all results for a specific athlete (exact or partial match).
    Supports both Chinese characters and pinyin input."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    # Check if input is ASCII (pinyin) or Chinese
    if all(ord(c) < 128 for c in name.replace(" ", "")):
        where_clause = "r.name_pinyin LIKE ?"
        param = f"%{name.lower()}%"
    else:
        where_clause = "r.name LIKE ?"
        param = f"%{name}%"

    cursor.execute(f"""
        SELECT
            r.rank, r.bib, r.name, r.team,
            r.run1_time, r.run2_time, r.total_time, r.time_diff, r.status,
            r.total_seconds,
            e.discipline, e.gender, e.age_group, e.round_type,
            c.season, c.name AS competition, c.venue, c.date
        FROM results r
        JOIN events e ON r.event_id = e.id
        JOIN competitions c ON e.competition_id = c.id
        WHERE {where_clause}
        ORDER BY c.date DESC, e.discipline, r.rank
    """, (param,))
    rows = cursor.fetchall()
    result = [dict(row) for row in rows]
    conn.close()
    return result


def get_filter_options(season=None, competition=None, db_path=None):
    """Get unique values for filter dropdowns, with cascading filters.
    If season is set, competitions are filtered to that season.
    If competition is set, disciplines/age_groups/genders are filtered to that competition.
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()

    options = {}

    cursor.execute("SELECT DISTINCT season FROM competitions WHERE season IS NOT NULL AND season != '' ORDER BY season")
    options["seasons"] = [row["season"] for row in cursor.fetchall()]

    if season:
        cursor.execute("SELECT DISTINCT name FROM competitions WHERE name IS NOT NULL AND name != '' AND season = ? ORDER BY name", (season,))
    else:
        cursor.execute("SELECT DISTINCT name FROM competitions WHERE name IS NOT NULL AND name != '' ORDER BY name")
    options["competitions"] = [row["name"] for row in cursor.fetchall()]

    # Build event filter based on selected competition (and season)
    event_where = "WHERE e.discipline IS NOT NULL AND e.discipline != ''"
    event_params = []
    if competition:
        event_where += " AND c.name = ?"
        event_params.append(competition)
    elif season:
        event_where += " AND c.season = ?"
        event_params.append(season)

    event_join = "JOIN competitions c ON e.competition_id = c.id" if (season or competition) else ""

    cursor.execute(f"SELECT DISTINCT e.discipline FROM events e {event_join} {event_where} ORDER BY e.discipline", event_params)
    options["disciplines"] = [row["discipline"] for row in cursor.fetchall()]

    ag_where = event_where.replace("e.discipline IS NOT NULL AND e.discipline != ''", "e.age_group IS NOT NULL AND e.age_group != ''")
    cursor.execute(f"SELECT DISTINCT e.age_group FROM events e {event_join} {ag_where} ORDER BY e.age_group", event_params)
    options["age_groups"] = [row["age_group"] for row in cursor.fetchall()]

    g_where = event_where.replace("e.discipline IS NOT NULL AND e.discipline != ''", "e.gender IS NOT NULL AND e.gender != ''")
    cursor.execute(f"SELECT DISTINCT e.gender FROM events e {event_join} {g_where} ORDER BY e.gender", event_params)
    options["genders"] = [row["gender"] for row in cursor.fetchall()]

    conn.close()
    return options


def get_statistics(db_path=None):
    """Get summary counts for the database."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    stats = {}
    cursor.execute("SELECT COUNT(*) AS cnt FROM competitions")
    stats["competitions"] = cursor.fetchone()["cnt"]

    cursor.execute("SELECT COUNT(*) AS cnt FROM events")
    stats["events"] = cursor.fetchone()["cnt"]

    cursor.execute("SELECT COUNT(*) AS cnt FROM results")
    stats["results"] = cursor.fetchone()["cnt"]

    cursor.execute("SELECT COUNT(DISTINCT name) AS cnt FROM results")
    stats["athletes"] = cursor.fetchone()["cnt"]

    cursor.execute("SELECT COUNT(*) AS cnt FROM processed_files WHERE status = 'success'")
    stats["files_processed"] = cursor.fetchone()["cnt"]

    cursor.execute("SELECT COUNT(*) AS cnt FROM processed_files WHERE status = 'failed'")
    stats["files_failed"] = cursor.fetchone()["cnt"]

    conn.close()
    return stats
