import sqlite3

DB_PATH = "sites.db"


# Opens a connection to the SQLite database
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# Creates the site metadata tables if they do not already exist
def init_sites_db():
    conn = get_db()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            location_key TEXT NOT NULL,
            lat REAL NOT NULL,
            lng REAL NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS site_transformers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER NOT NULL,
            tf_id TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            display_order INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (site_id) REFERENCES sites(id),
            UNIQUE(site_id, tf_id)
        );
        """
    )
    conn.commit()
    conn.close()


# Returns the enabled site metadata used by the frontend
def list_site_meta():
    conn = get_db()
    rows = conn.execute(
        """
        SELECT id, name, location_key, lat, lng
        FROM sites
        WHERE enabled = 1
        ORDER BY name
        """
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# Returns the enabled transformer IDs for a selected site
def list_site_transformers(site_id: int):
    conn = get_db()
    rows = conn.execute(
        """
        SELECT tf_id
        FROM site_transformers
        WHERE site_id = ?
          AND enabled = 1
        ORDER BY display_order, tf_id
        """,
        (site_id,),
    ).fetchall()
    conn.close()
    return [r["tf_id"] for r in rows]


# Maps device IP addresses to site names for health display
def map_ip_to_site_name():
    conn = get_db()
    rows = conn.execute(
        """
        SELECT ip_address, name
        FROM sites
        WHERE enabled = 1 AND ip_address IS NOT NULL
        """
    ).fetchall()
    conn.close()
    return {r["ip_address"]: r["name"] for r in rows}
