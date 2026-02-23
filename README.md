# f-jira

A terminal UI for exporting JIRA data into a local SQLite database, with re-export to CSV, JIRA-importable CSV, or JSON.

## Quick start

```bash
uv run f-jira
```

You'll be prompted for:

- **Atlassian domain** — e.g. `mycompany` (from `mycompany.atlassian.net`)
- **Email** — your Atlassian account email
- **API token** — generate one at https://id.atlassian.com/manage-profile/security/api-tokens

## What it does

1. Validates your credentials against the JIRA API
2. Lists all accessible projects with key, name, type, and lead
3. Exports selected projects — all issues, comments, links, and custom fields — into `jira_export.db`
4. Offers re-export as CSV, JIRA-importable CSV, or JSON into the `exports/` directory

## Export formats

| Format | Description |
|---|---|
| **CSV** | `issues.csv` + `comments.csv` with all core and custom fields |
| **JIRA CSV** | Follows JIRA's CSV import format with repeated headers for multi-value fields |
| **JSON** | Full raw API response per issue |

## Keyboard shortcuts

| Key | Action |
|---|---|
| `Ctrl+Q` | Quit |
| `A` | Select all projects |
| `N` | Deselect all projects |
| `Escape` | Back / Cancel |

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)

## Tech stack

- [Textual](https://textual.textualize.io/) — TUI framework
- [httpx](https://www.python-httpx.org/) — async HTTP client
- SQLite — local data storage (via stdlib `sqlite3`)
