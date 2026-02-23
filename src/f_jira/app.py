"""Textual TUI for JIRA data export."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Center, Horizontal, Vertical, VerticalScroll
from textual.screen import Screen
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    Log,
    ProgressBar,
    Static,
)

from f_jira.api import JiraClient
from f_jira.db import Database
from f_jira.export import export_csv, export_jira_csv, export_json

log = logging.getLogger(__name__)

DB_PATH = Path("jira_export.db")
EXPORT_DIR = Path("exports")


class LoginScreen(Screen):
    """Screen for entering JIRA credentials."""

    def compose(self) -> ComposeResult:
        yield Header()
        with Center():
            with Vertical(id="login-form"):
                yield Label("JIRA Export Tool", id="title")
                yield Label("Enter your Atlassian credentials to get started.", id="subtitle")
                yield Label("")
                yield Label("Atlassian domain (e.g. mycompany):")
                yield Input(placeholder="mycompany", id="domain")
                yield Label("Email address:")
                yield Input(placeholder="you@example.com", id="email")
                yield Label("API token:")
                yield Input(placeholder="Your API token", password=True, id="token")
                yield Label("")
                yield Button("Connect", variant="primary", id="connect-btn")
                yield Label("", id="login-status")
        yield Footer()

    @on(Button.Pressed, "#connect-btn")
    def handle_connect(self) -> None:
        self._validate_credentials()

    @on(Input.Submitted)
    def handle_submit(self) -> None:
        self._validate_credentials()

    @work(exclusive=True)
    async def _validate_credentials(self) -> None:
        domain = self.query_one("#domain", Input).value.strip()
        email = self.query_one("#email", Input).value.strip()
        token = self.query_one("#token", Input).value.strip()
        status = self.query_one("#login-status", Label)

        if not all([domain, email, token]):
            status.update("[red]All fields are required.[/red]")
            return

        # Strip .atlassian.net if user included it
        domain = domain.replace(".atlassian.net", "").replace("https://", "").strip("/")

        status.update("[yellow]Connecting...[/yellow]")
        btn = self.query_one("#connect-btn", Button)
        btn.disabled = True

        try:
            client = JiraClient(domain, email, token)
            user_info = await client.get_myself()
            display_name = user_info.get("displayName", "Unknown")
            account_type = user_info.get("accountType", "")

            status.update(
                f"[green]Connected as {display_name} ({account_type})[/green]"
            )

            # Store client on the app for other screens
            app = self.app
            assert isinstance(app, JiraExportApp)
            app.jira_client = client
            app.user_info = user_info

            # Transition to project selection
            self.app.switch_screen(ProjectScreen())

        except Exception as exc:
            await client.close()
            status.update(f"[red]Connection failed: {exc}[/red]")
            btn.disabled = False


class ProjectScreen(Screen):
    """Screen for selecting projects to export."""

    BINDINGS = [
        Binding("escape", "go_back", "Back"),
        Binding("a", "select_all", "Select All"),
        Binding("n", "select_none", "Deselect All"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self._projects: list[dict[str, Any]] = []
        self._selected: set[str] = set()
        self._selected_col_key: Any = None

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical():
            yield Label("Select projects to export", id="proj-title")
            yield DataTable(id="project-table")
            with Center():
                with Horizontal(id="proj-buttons"):
                    yield Button("Export Selected", variant="primary", id="export-btn")
                    yield Button("Back", id="back-btn")
            yield Label("", id="proj-status")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#project-table", DataTable)
        table.cursor_type = "row"
        col_keys = table.add_columns("Selected", "Key", "Name", "Type", "Lead")
        self._selected_col_key = col_keys[0]
        self._load_projects()

    @work(exclusive=True)
    async def _load_projects(self) -> None:
        status = self.query_one("#proj-status", Label)
        status.update("[yellow]Loading projects...[/yellow]")

        app = self.app
        assert isinstance(app, JiraExportApp)
        try:
            self._projects = await app.jira_client.get_projects()
            table = self.query_one("#project-table", DataTable)
            table.clear()
            for proj in self._projects:
                key = proj["key"]
                lead = proj.get("lead", {})
                lead_name = lead.get("displayName", "") if lead else ""
                table.add_row(
                    " ",
                    key,
                    proj.get("name", ""),
                    proj.get("projectTypeKey", ""),
                    lead_name,
                    key=key,
                )
            status.update(f"[green]{len(self._projects)} projects found. Click rows to select, then Export.[/green]")
        except Exception as exc:
            status.update(f"[red]Failed to load projects: {exc}[/red]")

    @on(DataTable.RowSelected, "#project-table")
    def handle_row_selected(self, event: DataTable.RowSelected) -> None:
        table = self.query_one("#project-table", DataTable)
        row_key = event.row_key
        proj_key = row_key.value
        if proj_key in self._selected:
            self._selected.discard(proj_key)
            table.update_cell(row_key, self._selected_col_key, " ")
        else:
            self._selected.add(proj_key)
            table.update_cell(row_key, self._selected_col_key, "[green]✓[/green]")

    def action_select_all(self) -> None:
        table = self.query_one("#project-table", DataTable)
        for proj in self._projects:
            key = proj["key"]
            self._selected.add(key)
            table.update_cell(key, self._selected_col_key, "[green]✓[/green]")

    def action_select_none(self) -> None:
        table = self.query_one("#project-table", DataTable)
        for proj in self._projects:
            key = proj["key"]
            self._selected.discard(key)
            table.update_cell(key, self._selected_col_key, " ")

    @on(Button.Pressed, "#export-btn")
    def handle_export(self) -> None:
        if not self._selected:
            self.query_one("#proj-status", Label).update(
                "[red]Select at least one project to export.[/red]"
            )
            return
        selected_projects = [
            p for p in self._projects if p["key"] in self._selected
        ]
        self.app.switch_screen(ExportScreen(selected_projects))

    @on(Button.Pressed, "#back-btn")
    def handle_back(self) -> None:
        self.action_go_back()

    def action_go_back(self) -> None:
        self.app.switch_screen(LoginScreen())


class ExportScreen(Screen):
    """Screen showing export progress."""

    BINDINGS = [Binding("escape", "cancel", "Cancel")]

    def __init__(self, projects: list[dict[str, Any]]) -> None:
        super().__init__()
        self._projects = projects
        self._cancelled = False

    def compose(self) -> ComposeResult:
        yield Header()
        with VerticalScroll(id="export-container"):
            yield Label("Exporting JIRA data...", id="export-title")
            yield Label("", id="current-task")
            yield ProgressBar(id="overall-progress", total=100, show_eta=False)
            yield Label("", id="progress-label")
            yield Log(id="export-log", auto_scroll=True)
        yield Footer()

    def on_mount(self) -> None:
        self._run_export()

    def action_cancel(self) -> None:
        self._cancelled = True
        log_widget = self.query_one("#export-log", Log)
        log_widget.write_line("[Cancelled by user]")

    @work(exclusive=True)
    async def _run_export(self) -> None:
        app = self.app
        assert isinstance(app, JiraExportApp)
        client = app.jira_client
        db = Database(DB_PATH)

        log_widget = self.query_one("#export-log", Log)
        progress = self.query_one("#overall-progress", ProgressBar)
        progress_label = self.query_one("#progress-label", Label)
        task_label = self.query_one("#current-task", Label)

        total_projects = len(self._projects)
        total_issues_exported = 0
        total_comments_exported = 0

        try:
            # Fetch field definitions first
            task_label.update("Fetching field definitions...")
            log_widget.write_line("Fetching field definitions...")
            try:
                fields = await client.get_fields()
                db.upsert_field_definitions(fields)
                log_widget.write_line(f"  Loaded {len(fields)} field definitions")
            except Exception as exc:
                log_widget.write_line(f"  Warning: Could not fetch fields: {exc}")

            field_map = db.get_custom_field_map()

            for proj_idx, project in enumerate(self._projects):
                if self._cancelled:
                    break

                proj_key = project["key"]
                proj_name = project.get("name", proj_key)

                # Save project to DB
                db.upsert_project(project)

                task_label.update(f"Exporting {proj_key}: {proj_name} ({proj_idx + 1}/{total_projects})")
                log_widget.write_line(f"\n--- {proj_key}: {proj_name} ---")

                # Search issues
                log_widget.write_line(f"  Searching issues...")
                try:
                    total_count, issues = await client.search_issues(proj_key)
                except Exception as exc:
                    log_widget.write_line(f"  ERROR fetching issues: {exc}")
                    continue

                log_widget.write_line(f"  Found {total_count} issues, fetched {len(issues)}")

                # Store issues and fetch comments
                for i, issue in enumerate(issues):
                    if self._cancelled:
                        break

                    issue_key = issue.get("key", "?")
                    try:
                        db.upsert_issue(issue, field_map)
                        total_issues_exported += 1

                        # Insert issue links
                        links = issue.get("fields", {}).get("issuelinks", [])
                        if links:
                            db.insert_issue_links(issue_key, links)

                        # Fetch comments for this issue
                        comments_data = issue.get("fields", {}).get("comment", {})
                        comments = comments_data.get("comments", []) if isinstance(comments_data, dict) else []

                        # If comments weren't included in the issue response, fetch them
                        if not comments:
                            try:
                                comments = await client.get_issue_comments(issue_key)
                            except Exception:
                                pass

                        for comment in comments:
                            db.upsert_comment(issue_key, comment)
                            total_comments_exported += 1

                    except Exception as exc:
                        log_widget.write_line(f"  WARNING: Failed to process {issue_key}: {exc}")

                    # Update progress within this project
                    if len(issues) > 0:
                        issue_pct = (i + 1) / len(issues)
                        overall_pct = (proj_idx + issue_pct) / total_projects * 100
                        progress.update(progress=overall_pct)
                        progress_label.update(
                            f"  {proj_key}: {i + 1}/{len(issues)} issues"
                        )

                log_widget.write_line(f"  Done: {len(issues)} issues, comments stored")

                # Update overall progress
                overall_pct = (proj_idx + 1) / total_projects * 100
                progress.update(progress=overall_pct)

        except Exception as exc:
            log_widget.write_line(f"\nERROR: {exc}")
        finally:
            db.close()

        if self._cancelled:
            log_widget.write_line("\nExport cancelled.")
            task_label.update("[yellow]Export cancelled[/yellow]")
        else:
            progress.update(progress=100)
            log_widget.write_line(
                f"\nExport complete! {total_issues_exported} issues, "
                f"{total_comments_exported} comments across {total_projects} projects."
            )
            task_label.update("[green]Export complete![/green]")

        # Transition to results screen after a brief pause
        self.app.switch_screen(ResultsScreen())


class ResultsScreen(Screen):
    """Screen showing export results and re-export options."""

    BINDINGS = [
        Binding("escape", "quit", "Quit"),
        Binding("b", "go_back", "Back to Projects"),
    ]

    def compose(self) -> ComposeResult:
        yield Header()
        with VerticalScroll(id="results-container"):
            yield Label("Export Results", id="results-title")
            yield Static(id="stats-display")
            yield Label("")
            yield Label("Export formats:", id="export-label")
            with Center():
                with Horizontal(id="export-buttons"):
                    yield Button("CSV", variant="primary", id="csv-btn")
                    yield Button("JIRA CSV", variant="warning", id="jira-csv-btn")
                    yield Button("JSON", variant="success", id="json-btn")
            yield Label("", id="export-status")
            yield Label("")
            with Center():
                with Horizontal(id="nav-buttons"):
                    yield Button("Export More Projects", id="more-btn")
                    yield Button("Quit", variant="error", id="quit-btn")
        yield Footer()

    def on_mount(self) -> None:
        self._show_stats()

    def _show_stats(self) -> None:
        try:
            db = Database(DB_PATH)
            stats = db.get_stats()
            db.close()
            display = self.query_one("#stats-display", Static)
            display.update(
                f"[bold]Database:[/bold] {DB_PATH}\n\n"
                f"  Projects: [cyan]{stats['projects']}[/cyan]\n"
                f"  Issues:   [cyan]{stats['issues']}[/cyan]\n"
                f"  Comments: [cyan]{stats['comments']}[/cyan]\n"
                f"  Links:    [cyan]{stats['links']}[/cyan]"
            )
        except Exception as exc:
            self.query_one("#stats-display", Static).update(f"[red]Error reading database: {exc}[/red]")

    @on(Button.Pressed, "#csv-btn")
    def handle_csv(self) -> None:
        self._do_export("csv")

    @on(Button.Pressed, "#jira-csv-btn")
    def handle_jira_csv(self) -> None:
        self._do_export("jira_csv")

    @on(Button.Pressed, "#json-btn")
    def handle_json(self) -> None:
        self._do_export("json")

    def _do_export(self, fmt: str) -> None:
        status = self.query_one("#export-status", Label)
        try:
            db = Database(DB_PATH)
            if fmt == "csv":
                files = export_csv(db, EXPORT_DIR)
                db.close()
                if files:
                    paths = ", ".join(str(f) for f in files)
                    status.update(f"[green]CSV exported: {paths}[/green]")
                else:
                    status.update("[yellow]No issues to export.[/yellow]")
            elif fmt == "jira_csv":
                path = export_jira_csv(db, EXPORT_DIR)
                db.close()
                if path:
                    status.update(f"[green]JIRA CSV exported: {path}[/green]")
                else:
                    status.update("[yellow]No issues to export.[/yellow]")
            elif fmt == "json":
                path = export_json(db, EXPORT_DIR)
                db.close()
                if path:
                    status.update(f"[green]JSON exported: {path}[/green]")
                else:
                    status.update("[yellow]No issues to export.[/yellow]")
        except Exception as exc:
            status.update(f"[red]Export failed: {exc}[/red]")

    @on(Button.Pressed, "#more-btn")
    def handle_more(self) -> None:
        self.action_go_back()

    @on(Button.Pressed, "#quit-btn")
    def handle_quit(self) -> None:
        self.app.exit()

    def action_go_back(self) -> None:
        self.app.switch_screen(ProjectScreen())

    def action_quit(self) -> None:
        self.app.exit()


class JiraExportApp(App):
    """Main Textual application for JIRA data export."""

    TITLE = "f-jira"
    SUB_TITLE = "JIRA Data Export Tool"

    CSS = """
    #login-form {
        width: 60;
        height: auto;
        padding: 2 4;
        margin: 2 0;
        border: solid $accent;
        background: $surface;
    }

    #title {
        text-align: center;
        text-style: bold;
        width: 100%;
        margin-bottom: 1;
    }

    #subtitle {
        text-align: center;
        color: $text-muted;
        width: 100%;
        margin-bottom: 1;
    }

    #login-form Input {
        margin-bottom: 1;
    }

    #connect-btn {
        width: 100%;
        margin-top: 1;
    }

    #login-status {
        text-align: center;
        margin-top: 1;
    }

    #proj-title {
        text-style: bold;
        padding: 1 2;
    }

    #project-table {
        height: 1fr;
        margin: 0 2;
    }

    #proj-buttons {
        height: 3;
        margin: 1 0;
    }

    #proj-buttons Button {
        margin: 0 1;
    }

    #proj-status {
        text-align: center;
        padding: 1 2;
    }

    #export-container {
        padding: 1 2;
    }

    #export-title {
        text-style: bold;
        margin-bottom: 1;
    }

    #export-log {
        height: 1fr;
        min-height: 10;
        margin: 1 0;
        border: solid $accent;
    }

    #results-container {
        padding: 2 4;
    }

    #results-title {
        text-style: bold;
        text-align: center;
        width: 100%;
        margin-bottom: 1;
    }

    #stats-display {
        margin: 1 2;
        padding: 1 2;
        border: solid $accent;
        background: $surface;
    }

    #export-buttons {
        height: 3;
        margin: 1 0;
    }

    #export-buttons Button {
        margin: 0 1;
    }

    #nav-buttons {
        height: 3;
        margin: 1 0;
    }

    #nav-buttons Button {
        margin: 0 1;
    }

    #export-status {
        text-align: center;
        margin: 1 0;
    }
    """

    BINDINGS = [
        Binding("ctrl+q", "quit", "Quit", show=True),
    ]

    def __init__(self) -> None:
        super().__init__()
        self.jira_client: JiraClient | None = None
        self.user_info: dict[str, Any] = {}

    def on_mount(self) -> None:
        self.push_screen(LoginScreen())

    async def action_quit(self) -> None:
        if self.jira_client:
            await self.jira_client.close()
        self.exit()
