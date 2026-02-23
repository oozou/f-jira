# f-jira

A terminal UI for exporting JIRA and Confluence data into a local SQLite database, with re-export
to CSV, JSON, or JIRA-importable CSV.

## Quick start

```bash
uv run f-jira
```

You'll be prompted for:

- **Atlassian domain** — e.g. `mycompany` (from `mycompany.atlassian.net`)
- **Email** — your Atlassian account email
- **API token** — generate one at https://id.atlassian.com/manage-profile/security/api-tokens

## What it does

1. Validates your credentials against the Atlassian API
2. Lets you choose **JIRA** or **Confluence** on the service selection screen
3. Lists all accessible projects (JIRA) or spaces (Confluence)
4. Exports selected items — issues/comments/links or pages/comments — into `jira_export.db`
5. Offers re-export in multiple formats into the `exports/` directory

You can go back and export from both services in the same session — everything accumulates in the
same database.

## Export formats

| Format | Service | Description |
|---|---|---|
| **CSV** | JIRA | `issues.csv` + `comments.csv` with all core and custom fields |
| **JIRA CSV** | JIRA | Follows JIRA's CSV import format with repeated headers for multi-value fields |
| **JSON** | JIRA | Full raw API response per issue |
| **Confluence CSV** | Confluence | `pages.csv` + `page_comments.csv` |
| **Confluence JSON** | Confluence | Full raw API response per page |

### Split export

A **split toggle** on the results screen lets you choose between:

- **Off** (default) — one combined file for all projects or spaces
- **On** — one file per project/space (e.g. `issues_PROJ1.csv`, `pages_12345.csv`)

## Keyboard shortcuts

| Key | Action |
|---|---|
| `Ctrl+Q` | Quit |
| `A` | Select all projects / spaces |
| `N` | Deselect all projects / spaces |
| `Escape` | Back / Cancel |

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)

## Tech stack

- [Textual](https://textual.textualize.io/) — TUI framework
- [httpx](https://www.python-httpx.org/) — async HTTP client
- SQLite — local data storage (via stdlib `sqlite3`)
