"""Export JIRA data from SQLite to CSV, JIRA CSV, and JSON formats."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from f_jira.db import Database


# Core fields to include in standard CSV export
CORE_FIELDS = [
    "key", "project_key", "summary", "description", "issue_type", "status",
    "status_category", "priority", "assignee_name", "assignee_email",
    "reporter_name", "reporter_email", "labels", "components",
    "fix_versions", "affects_versions", "resolution", "resolution_date",
    "created", "updated", "due_date", "parent_key",
    "time_original_estimate", "time_spent", "time_remaining", "story_points",
]

# JIRA CSV import column name mapping
JIRA_FIELD_MAP = {
    "key": "Issue Key",
    "summary": "Summary",
    "issue_type": "Issue Type",
    "status": "Status",
    "priority": "Priority",
    "assignee_name": "Assignee",
    "reporter_name": "Reporter",
    "resolution": "Resolution",
    "created": "Created",
    "updated": "Updated",
    "due_date": "Due Date",
    "description": "Description",
    "parent_key": "Parent",
    "time_original_estimate": "Original Estimate",
    "time_spent": "Time Spent",
}


def _parse_json_field(value: str | None) -> list[str]:
    """Parse a JSON array field, returning empty list on failure."""
    if not value:
        return []
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return [str(v) for v in parsed]
    except (json.JSONDecodeError, TypeError):
        pass
    return []


def export_csv(db: Database, output_dir: Path, project_key: str | None = None) -> list[Path]:
    """Export issues and comments to standard CSV files.

    Returns list of created file paths.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    created_files: list[Path] = []

    # -- Issues CSV --
    issues = db.get_issues(project_key)
    if not issues:
        return created_files

    # Collect all custom field names across issues
    custom_field_names: set[str] = set()
    for issue in issues:
        if issue.get("custom_fields"):
            try:
                cf = json.loads(issue["custom_fields"])
                custom_field_names.update(cf.keys())
            except (json.JSONDecodeError, TypeError):
                pass

    sorted_custom = sorted(custom_field_names)
    all_headers = CORE_FIELDS + [f"custom:{name}" for name in sorted_custom]

    suffix = f"_{project_key}" if project_key else ""
    issues_path = output_dir / f"issues{suffix}.csv"
    with open(issues_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_headers, extrasaction="ignore")
        writer.writeheader()
        for issue in issues:
            row: dict[str, Any] = {}
            for field in CORE_FIELDS:
                val = issue.get(field)
                # Flatten JSON array fields for readability
                if field in ("labels", "components", "fix_versions", "affects_versions"):
                    val = ", ".join(_parse_json_field(val))
                row[field] = val or ""

            # Add custom fields
            if issue.get("custom_fields"):
                try:
                    cf = json.loads(issue["custom_fields"])
                    for name in sorted_custom:
                        val = cf.get(name)
                        if isinstance(val, (dict, list)):
                            val = json.dumps(val)
                        row[f"custom:{name}"] = val or ""
                except (json.JSONDecodeError, TypeError):
                    pass

            writer.writerow(row)
    created_files.append(issues_path)

    # -- Comments CSV --
    comments = db.get_comments()
    if project_key:
        issue_keys = {i["key"] for i in issues}
        comments = [c for c in comments if c["issue_key"] in issue_keys]

    if comments:
        comments_path = output_dir / f"comments{suffix}.csv"
        comment_headers = ["issue_key", "author_name", "author_email", "body", "created", "updated"]
        with open(comments_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=comment_headers, extrasaction="ignore")
            writer.writeheader()
            for comment in comments:
                writer.writerow({h: comment.get(h, "") for h in comment_headers})
        created_files.append(comments_path)

    return created_files


def export_jira_csv(db: Database, output_dir: Path, project_key: str | None = None) -> Path | None:
    """Export issues in JIRA-importable CSV format.

    Follows JIRA's CSV import format:
    - Uses JIRA-recognized column names
    - Multi-value fields (labels, components) use repeated column headers
    - Comments formatted as date;author;body
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    issues = db.get_issues(project_key)
    if not issues:
        return None

    # Find max counts for multi-value fields
    max_labels = 0
    max_components = 0
    max_fix_versions = 0
    max_comments = 0

    issue_comments: dict[str, list[dict[str, Any]]] = {}
    for issue in issues:
        labels = _parse_json_field(issue.get("labels"))
        components = _parse_json_field(issue.get("components"))
        fix_versions = _parse_json_field(issue.get("fix_versions"))
        max_labels = max(max_labels, len(labels))
        max_components = max(max_components, len(components))
        max_fix_versions = max(max_fix_versions, len(fix_versions))

        comments = db.get_comments(issue["key"])
        issue_comments[issue["key"]] = comments
        max_comments = max(max_comments, len(comments))

    # Build headers
    headers: list[str] = list(JIRA_FIELD_MAP.values())
    headers.extend(["Labels"] * max(max_labels, 1))
    headers.extend(["Component"] * max(max_components, 1))
    headers.extend(["Fix Version"] * max(max_fix_versions, 1))
    headers.extend(["Comment"] * max(max_comments, 1))

    suffix = f"_{project_key}" if project_key else ""
    jira_path = output_dir / f"jira_import{suffix}.csv"

    with open(jira_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for issue in issues:
            row: list[str] = []
            for db_field, _jira_name in JIRA_FIELD_MAP.items():
                val = issue.get(db_field)
                row.append(str(val) if val else "")

            # Multi-value: Labels
            labels = _parse_json_field(issue.get("labels"))
            for i in range(max(max_labels, 1)):
                row.append(labels[i] if i < len(labels) else "")

            # Multi-value: Components
            components = _parse_json_field(issue.get("components"))
            for i in range(max(max_components, 1)):
                row.append(components[i] if i < len(components) else "")

            # Multi-value: Fix Versions
            fix_versions = _parse_json_field(issue.get("fix_versions"))
            for i in range(max(max_fix_versions, 1)):
                row.append(fix_versions[i] if i < len(fix_versions) else "")

            # Comments: formatted as "date;author;body"
            comments = issue_comments.get(issue["key"], [])
            for i in range(max(max_comments, 1)):
                if i < len(comments):
                    c = comments[i]
                    comment_str = f"{c.get('created', '')};{c.get('author_name', '')};{c.get('body', '')}"
                    row.append(comment_str)
                else:
                    row.append("")

            writer.writerow(row)

    return jira_path


def export_json(db: Database, output_dir: Path, project_key: str | None = None) -> Path | None:
    """Export full issue data as JSON."""
    output_dir.mkdir(parents=True, exist_ok=True)
    issues = db.get_issues(project_key)
    if not issues:
        return None

    # Parse raw_json for each issue to get full API data
    full_issues: list[dict[str, Any]] = []
    for issue in issues:
        if issue.get("raw_json"):
            try:
                full_issues.append(json.loads(issue["raw_json"]))
            except (json.JSONDecodeError, TypeError):
                full_issues.append(dict(issue))
        else:
            full_issues.append(dict(issue))

    suffix = f"_{project_key}" if project_key else ""
    json_path = output_dir / f"issues{suffix}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(full_issues, f, indent=2, default=str)

    return json_path
