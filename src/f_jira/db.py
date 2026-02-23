"""SQLite database layer, ADF text extraction, and HTML text extraction."""

from __future__ import annotations

import json
import re
import sqlite3
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

DEFAULT_DB_PATH = Path("jira_export.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS projects (
    id TEXT PRIMARY KEY,
    key TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    type TEXT,
    lead_name TEXT,
    description TEXT,
    exported_at TEXT
);

CREATE TABLE IF NOT EXISTS issues (
    id TEXT PRIMARY KEY,
    key TEXT UNIQUE NOT NULL,
    project_key TEXT NOT NULL,
    summary TEXT NOT NULL,
    description TEXT,
    description_raw TEXT,
    issue_type TEXT,
    status TEXT,
    status_category TEXT,
    priority TEXT,
    assignee_name TEXT,
    assignee_email TEXT,
    reporter_name TEXT,
    reporter_email TEXT,
    creator_name TEXT,
    creator_email TEXT,
    labels TEXT,
    components TEXT,
    fix_versions TEXT,
    affects_versions TEXT,
    resolution TEXT,
    resolution_date TEXT,
    created TEXT,
    updated TEXT,
    due_date TEXT,
    parent_key TEXT,
    time_original_estimate INTEGER,
    time_spent INTEGER,
    time_remaining INTEGER,
    story_points REAL,
    sprint TEXT,
    custom_fields TEXT,
    raw_json TEXT,
    FOREIGN KEY (project_key) REFERENCES projects(key)
);

CREATE TABLE IF NOT EXISTS comments (
    id TEXT PRIMARY KEY,
    issue_key TEXT NOT NULL,
    author_name TEXT,
    author_email TEXT,
    body TEXT,
    body_raw TEXT,
    created TEXT,
    updated TEXT,
    FOREIGN KEY (issue_key) REFERENCES issues(key)
);

CREATE TABLE IF NOT EXISTS issue_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_key TEXT NOT NULL,
    link_type TEXT,
    direction TEXT,
    linked_issue_key TEXT,
    FOREIGN KEY (issue_key) REFERENCES issues(key)
);

CREATE TABLE IF NOT EXISTS field_definitions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    custom BOOLEAN,
    schema_type TEXT
);

CREATE TABLE IF NOT EXISTS confluence_spaces (
    id TEXT PRIMARY KEY,
    key TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    type TEXT,
    status TEXT,
    exported_at TEXT
);

CREATE TABLE IF NOT EXISTS confluence_pages (
    id TEXT PRIMARY KEY,
    space_id TEXT NOT NULL,
    title TEXT NOT NULL,
    status TEXT,
    parent_id TEXT,
    author_id TEXT,
    body_plain TEXT,
    body_raw TEXT,
    labels TEXT,
    created TEXT,
    updated TEXT,
    version_number INTEGER,
    raw_json TEXT,
    FOREIGN KEY (space_id) REFERENCES confluence_spaces(id)
);

CREATE TABLE IF NOT EXISTS confluence_comments (
    id TEXT PRIMARY KEY,
    page_id TEXT NOT NULL,
    author_id TEXT,
    body_plain TEXT,
    body_raw TEXT,
    created TEXT,
    updated TEXT,
    FOREIGN KEY (page_id) REFERENCES confluence_pages(id)
);
"""


def extract_text_from_adf(adf: dict[str, Any] | None) -> str:
    """Recursively extract plain text from Atlassian Document Format JSON."""
    if not adf:
        return ""
    parts: list[str] = []
    _walk_adf(adf, parts)
    return "\n".join(parts).strip()


def _walk_adf(node: dict[str, Any], parts: list[str], depth: int = 0) -> None:
    """Walk ADF node tree and collect text."""
    node_type = node.get("type", "")

    if node_type == "text":
        parts.append(node.get("text", ""))
        return

    if node_type == "hardBreak":
        parts.append("\n")
        return

    if node_type == "heading":
        level = node.get("attrs", {}).get("level", 1)
        prefix = "#" * level + " "
        child_text = _collect_inline_text(node)
        parts.append(f"\n{prefix}{child_text}\n")
        return

    if node_type == "codeBlock":
        child_text = _collect_inline_text(node)
        parts.append(f"\n```\n{child_text}\n```\n")
        return

    if node_type == "blockquote":
        child_text = _collect_inline_text(node)
        for line in child_text.split("\n"):
            parts.append(f"> {line}")
        return

    if node_type in ("bulletList", "orderedList"):
        for i, item in enumerate(node.get("content", []), 1):
            marker = "- " if node_type == "bulletList" else f"{i}. "
            item_text = _collect_inline_text(item)
            parts.append(f"{marker}{item_text}")
        parts.append("")
        return

    if node_type == "table":
        for row in node.get("content", []):
            cells = []
            for cell in row.get("content", []):
                cells.append(_collect_inline_text(cell))
            parts.append(" | ".join(cells))
        parts.append("")
        return

    if node_type == "paragraph":
        child_text = _collect_inline_text(node)
        parts.append(child_text)
        return

    # Recurse into children for any other node type
    for child in node.get("content", []):
        _walk_adf(child, parts, depth + 1)


def _collect_inline_text(node: dict[str, Any]) -> str:
    """Collect inline text from a node's children."""
    texts: list[str] = []
    for child in node.get("content", []):
        if child.get("type") == "text":
            texts.append(child.get("text", ""))
        elif child.get("type") == "hardBreak":
            texts.append("\n")
        elif child.get("type") == "mention":
            texts.append(child.get("attrs", {}).get("text", ""))
        elif child.get("type") == "emoji":
            texts.append(child.get("attrs", {}).get("shortName", ""))
        elif child.get("type") == "inlineCard":
            texts.append(child.get("attrs", {}).get("url", ""))
        else:
            texts.append(_collect_inline_text(child))
    return "".join(texts)


class _HTMLTextExtractor(HTMLParser):
    """Simple HTML parser that extracts plain text from HTML/XHTML."""

    # Block-level elements that should produce whitespace boundaries
    _BLOCK_TAGS = frozenset({
        "p", "div", "br", "h1", "h2", "h3", "h4", "h5", "h6",
        "li", "ol", "ul", "table", "tr", "td", "th", "blockquote",
        "pre", "hr", "section", "article", "header", "footer",
    })

    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in self._BLOCK_TAGS:
            self._parts.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in self._BLOCK_TAGS:
            self._parts.append(" ")

    def handle_data(self, data: str) -> None:
        self._parts.append(data)

    def get_text(self) -> str:
        return re.sub(r"\s+", " ", "".join(self._parts)).strip()


def extract_text_from_storage(html_str: str | None) -> str:
    """Extract plain text from Confluence storage format (XHTML).

    Strips all HTML tags and normalizes whitespace.
    """
    if not html_str:
        return ""
    parser = _HTMLTextExtractor()
    parser.feed(html_str)
    return parser.get_text()


def _safe_json(value: Any) -> str | None:
    """Serialize a value to JSON string, or return None."""
    if value is None:
        return None
    return json.dumps(value)


def _safe_name(user: dict[str, Any] | None) -> str | None:
    """Extract display name from a user dict."""
    if not user:
        return None
    return user.get("displayName")


def _safe_email(user: dict[str, Any] | None) -> str | None:
    """Extract email from a user dict."""
    if not user:
        return None
    return user.get("emailAddress")


class Database:
    """SQLite database wrapper for JIRA export data."""

    def __init__(self, path: Path = DEFAULT_DB_PATH) -> None:
        self.path = path
        self.conn = sqlite3.connect(str(path))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._create_schema()

    def _create_schema(self) -> None:
        self.conn.executescript(SCHEMA)

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> Database:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    # -- Projects --

    def upsert_project(self, project: dict[str, Any]) -> None:
        """Insert or update a project from API response."""
        lead = project.get("lead", {})
        desc = project.get("description")
        if isinstance(desc, dict):
            desc = extract_text_from_adf(desc)

        self.conn.execute(
            """INSERT INTO projects (id, key, name, type, lead_name, description, exported_at)
            VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(id) DO UPDATE SET
                key=excluded.key, name=excluded.name, type=excluded.type,
                lead_name=excluded.lead_name, description=excluded.description,
                exported_at=excluded.exported_at""",
            (
                project["id"],
                project["key"],
                project["name"],
                project.get("projectTypeKey"),
                _safe_name(lead),
                desc,
            ),
        )
        self.conn.commit()

    # -- Issues --

    def upsert_issue(self, issue: dict[str, Any], field_map: dict[str, str] | None = None) -> None:
        """Insert or update an issue from API response."""
        fields = issue.get("fields", {})

        desc_raw = fields.get("description")
        desc_text = extract_text_from_adf(desc_raw) if isinstance(desc_raw, dict) else desc_raw

        # Extract custom fields
        custom_fields = {}
        if field_map:
            for field_id, field_name in field_map.items():
                if field_id.startswith("customfield_") and field_id in fields:
                    val = fields[field_id]
                    if val is not None:
                        custom_fields[field_name] = val

        # Extract sprint info from common sprint field locations
        sprint = fields.get("sprint")

        # Story points - try common field names
        story_points = fields.get("story_points") or fields.get("customfield_10016")

        parent = fields.get("parent", {})
        parent_key = parent.get("key") if parent else None

        self.conn.execute(
            """INSERT INTO issues (
                id, key, project_key, summary, description, description_raw,
                issue_type, status, status_category, priority,
                assignee_name, assignee_email, reporter_name, reporter_email,
                creator_name, creator_email,
                labels, components, fix_versions, affects_versions,
                resolution, resolution_date, created, updated, due_date,
                parent_key, time_original_estimate, time_spent, time_remaining,
                story_points, sprint, custom_fields, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                key=excluded.key, project_key=excluded.project_key,
                summary=excluded.summary, description=excluded.description,
                description_raw=excluded.description_raw,
                issue_type=excluded.issue_type, status=excluded.status,
                status_category=excluded.status_category, priority=excluded.priority,
                assignee_name=excluded.assignee_name, assignee_email=excluded.assignee_email,
                reporter_name=excluded.reporter_name, reporter_email=excluded.reporter_email,
                creator_name=excluded.creator_name, creator_email=excluded.creator_email,
                labels=excluded.labels, components=excluded.components,
                fix_versions=excluded.fix_versions, affects_versions=excluded.affects_versions,
                resolution=excluded.resolution, resolution_date=excluded.resolution_date,
                created=excluded.created, updated=excluded.updated,
                due_date=excluded.due_date, parent_key=excluded.parent_key,
                time_original_estimate=excluded.time_original_estimate,
                time_spent=excluded.time_spent, time_remaining=excluded.time_remaining,
                story_points=excluded.story_points, sprint=excluded.sprint,
                custom_fields=excluded.custom_fields, raw_json=excluded.raw_json""",
            (
                issue["id"],
                issue["key"],
                fields.get("project", {}).get("key", issue["key"].rsplit("-", 1)[0]),
                fields.get("summary", ""),
                desc_text,
                _safe_json(desc_raw),
                fields.get("issuetype", {}).get("name") if fields.get("issuetype") else None,
                fields.get("status", {}).get("name") if fields.get("status") else None,
                fields.get("status", {}).get("statusCategory", {}).get("name") if fields.get("status") else None,
                fields.get("priority", {}).get("name") if fields.get("priority") else None,
                _safe_name(fields.get("assignee")),
                _safe_email(fields.get("assignee")),
                _safe_name(fields.get("reporter")),
                _safe_email(fields.get("reporter")),
                _safe_name(fields.get("creator")),
                _safe_email(fields.get("creator")),
                _safe_json(fields.get("labels")),
                _safe_json([c.get("name") for c in fields.get("components", [])]),
                _safe_json([v.get("name") for v in fields.get("fixVersions", [])]),
                _safe_json([v.get("name") for v in fields.get("versions", [])]),
                fields.get("resolution", {}).get("name") if fields.get("resolution") else None,
                fields.get("resolutiondate"),
                fields.get("created"),
                fields.get("updated"),
                fields.get("duedate"),
                parent_key,
                fields.get("timeoriginalestimate"),
                fields.get("timespent"),
                fields.get("timeestimate"),
                story_points,
                _safe_json(sprint),
                _safe_json(custom_fields) if custom_fields else None,
                _safe_json(issue),
            ),
        )
        self.conn.commit()

    # -- Comments --

    def upsert_comment(self, issue_key: str, comment: dict[str, Any]) -> None:
        """Insert or update a comment from API response."""
        body_raw = comment.get("body")
        body_text = extract_text_from_adf(body_raw) if isinstance(body_raw, dict) else body_raw
        author = comment.get("author", {})

        self.conn.execute(
            """INSERT INTO comments (id, issue_key, author_name, author_email, body, body_raw, created, updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                issue_key=excluded.issue_key, author_name=excluded.author_name,
                author_email=excluded.author_email, body=excluded.body,
                body_raw=excluded.body_raw, created=excluded.created, updated=excluded.updated""",
            (
                comment["id"],
                issue_key,
                _safe_name(author),
                _safe_email(author),
                body_text,
                _safe_json(body_raw),
                comment.get("created"),
                comment.get("updated"),
            ),
        )
        self.conn.commit()

    # -- Issue Links --

    def insert_issue_links(self, issue_key: str, links: list[dict[str, Any]]) -> None:
        """Insert issue links from an issue's fields."""
        # Clear existing links for this issue first
        self.conn.execute("DELETE FROM issue_links WHERE issue_key = ?", (issue_key,))
        for link in links:
            link_type = link.get("type", {}).get("name", "")
            if "inwardIssue" in link:
                self.conn.execute(
                    "INSERT INTO issue_links (issue_key, link_type, direction, linked_issue_key) VALUES (?, ?, ?, ?)",
                    (issue_key, link_type, "inward", link["inwardIssue"]["key"]),
                )
            if "outwardIssue" in link:
                self.conn.execute(
                    "INSERT INTO issue_links (issue_key, link_type, direction, linked_issue_key) VALUES (?, ?, ?, ?)",
                    (issue_key, link_type, "outward", link["outwardIssue"]["key"]),
                )
        self.conn.commit()

    # -- Field Definitions --

    def upsert_field_definitions(self, fields: list[dict[str, Any]]) -> None:
        """Insert or update field definitions."""
        for field in fields:
            schema = field.get("schema", {})
            self.conn.execute(
                """INSERT INTO field_definitions (id, name, custom, schema_type)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name=excluded.name, custom=excluded.custom, schema_type=excluded.schema_type""",
                (
                    field["id"],
                    field["name"],
                    field.get("custom", False),
                    schema.get("type"),
                ),
            )
        self.conn.commit()

    def get_custom_field_map(self) -> dict[str, str]:
        """Return a mapping of custom field ID -> human-readable name."""
        rows = self.conn.execute(
            "SELECT id, name FROM field_definitions WHERE custom = 1"
        ).fetchall()
        return {row["id"]: row["name"] for row in rows}

    # -- Query helpers --

    def get_projects(self) -> list[dict[str, Any]]:
        """Return all exported projects."""
        rows = self.conn.execute("SELECT * FROM projects ORDER BY key").fetchall()
        return [dict(row) for row in rows]

    def get_issues(self, project_key: str | None = None) -> list[dict[str, Any]]:
        """Return issues, optionally filtered by project."""
        if project_key:
            rows = self.conn.execute(
                "SELECT * FROM issues WHERE project_key = ? ORDER BY key", (project_key,)
            ).fetchall()
        else:
            rows = self.conn.execute("SELECT * FROM issues ORDER BY key").fetchall()
        return [dict(row) for row in rows]

    def get_comments(self, issue_key: str | None = None) -> list[dict[str, Any]]:
        """Return comments, optionally filtered by issue."""
        if issue_key:
            rows = self.conn.execute(
                "SELECT * FROM comments WHERE issue_key = ? ORDER BY created", (issue_key,)
            ).fetchall()
        else:
            rows = self.conn.execute("SELECT * FROM comments ORDER BY issue_key, created").fetchall()
        return [dict(row) for row in rows]

    def get_issue_links(self, issue_key: str | None = None) -> list[dict[str, Any]]:
        """Return issue links, optionally filtered by issue."""
        if issue_key:
            rows = self.conn.execute(
                "SELECT * FROM issue_links WHERE issue_key = ? ORDER BY id", (issue_key,)
            ).fetchall()
        else:
            rows = self.conn.execute("SELECT * FROM issue_links ORDER BY issue_key, id").fetchall()
        return [dict(row) for row in rows]

    def get_stats(self) -> dict[str, int]:
        """Return summary statistics."""
        return {
            "projects": self.conn.execute("SELECT count(*) FROM projects").fetchone()[0],
            "issues": self.conn.execute("SELECT count(*) FROM issues").fetchone()[0],
            "comments": self.conn.execute("SELECT count(*) FROM comments").fetchone()[0],
            "links": self.conn.execute("SELECT count(*) FROM issue_links").fetchone()[0],
        }

    # -- Confluence: Spaces --

    def upsert_confluence_space(self, space: dict[str, Any]) -> None:
        """Insert or update a Confluence space from API response."""
        self.conn.execute(
            """INSERT INTO confluence_spaces (id, key, name, type, status, exported_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(id) DO UPDATE SET
                key=excluded.key, name=excluded.name, type=excluded.type,
                status=excluded.status, exported_at=excluded.exported_at""",
            (
                space["id"],
                space["key"],
                space["name"],
                space.get("type"),
                space.get("status"),
            ),
        )
        self.conn.commit()

    # -- Confluence: Pages --

    def upsert_confluence_page(
        self, page: dict[str, Any], labels: list[dict[str, Any]] | None = None
    ) -> None:
        """Insert or update a Confluence page from API response."""
        body_storage = page.get("body", {}).get("storage", {}).get("value", "")
        body_plain = extract_text_from_storage(body_storage)
        label_names = json.dumps([lb.get("name", lb.get("prefix", "")) for lb in (labels or [])])

        version = page.get("version", {})
        version_number = version.get("number") if isinstance(version, dict) else None

        self.conn.execute(
            """INSERT INTO confluence_pages
            (id, space_id, title, status, parent_id, author_id,
             body_plain, body_raw, labels, created, updated, version_number, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                space_id=excluded.space_id, title=excluded.title, status=excluded.status,
                parent_id=excluded.parent_id, author_id=excluded.author_id,
                body_plain=excluded.body_plain, body_raw=excluded.body_raw,
                labels=excluded.labels, created=excluded.created, updated=excluded.updated,
                version_number=excluded.version_number, raw_json=excluded.raw_json""",
            (
                page["id"],
                page.get("spaceId", ""),
                page.get("title", ""),
                page.get("status"),
                page.get("parentId"),
                page.get("authorId"),
                body_plain,
                body_storage,
                label_names,
                page.get("createdAt"),
                page.get("version", {}).get("createdAt") if isinstance(page.get("version"), dict) else None,
                version_number,
                _safe_json(page),
            ),
        )
        self.conn.commit()

    # -- Confluence: Comments --

    def upsert_confluence_comment(self, page_id: str, comment: dict[str, Any]) -> None:
        """Insert or update a Confluence comment from API response."""
        body_storage = comment.get("body", {}).get("storage", {}).get("value", "")
        body_plain = extract_text_from_storage(body_storage)

        self.conn.execute(
            """INSERT INTO confluence_comments
            (id, page_id, author_id, body_plain, body_raw, created, updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                page_id=excluded.page_id, author_id=excluded.author_id,
                body_plain=excluded.body_plain, body_raw=excluded.body_raw,
                created=excluded.created, updated=excluded.updated""",
            (
                comment["id"],
                page_id,
                comment.get("authorId"),
                body_plain,
                body_storage,
                comment.get("createdAt"),
                comment.get("version", {}).get("createdAt") if isinstance(comment.get("version"), dict) else None,
            ),
        )
        self.conn.commit()

    # -- Confluence: Query helpers --

    def get_confluence_spaces(self) -> list[dict[str, Any]]:
        """Return all exported Confluence spaces."""
        rows = self.conn.execute("SELECT * FROM confluence_spaces ORDER BY key").fetchall()
        return [dict(row) for row in rows]

    def get_confluence_pages(self, space_id: str | None = None) -> list[dict[str, Any]]:
        """Return Confluence pages, optionally filtered by space."""
        if space_id:
            rows = self.conn.execute(
                "SELECT * FROM confluence_pages WHERE space_id = ? ORDER BY title",
                (space_id,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM confluence_pages ORDER BY space_id, title"
            ).fetchall()
        return [dict(row) for row in rows]

    def get_confluence_comments(self, page_id: str | None = None) -> list[dict[str, Any]]:
        """Return Confluence comments, optionally filtered by page."""
        if page_id:
            rows = self.conn.execute(
                "SELECT * FROM confluence_comments WHERE page_id = ? ORDER BY created",
                (page_id,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM confluence_comments ORDER BY page_id, created"
            ).fetchall()
        return [dict(row) for row in rows]

    def get_confluence_stats(self) -> dict[str, int]:
        """Return Confluence summary statistics."""
        return {
            "spaces": self.conn.execute("SELECT count(*) FROM confluence_spaces").fetchone()[0],
            "pages": self.conn.execute("SELECT count(*) FROM confluence_pages").fetchone()[0],
            "comments": self.conn.execute("SELECT count(*) FROM confluence_comments").fetchone()[0],
        }
