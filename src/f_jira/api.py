"""JIRA REST API v3 async client."""

from __future__ import annotations

import asyncio
import base64
import logging
from typing import Any

import httpx

log = logging.getLogger(__name__)


class JiraClient:
    """Async JIRA REST API v3 client with rate limiting and pagination."""

    BASE_PATH = "/rest/api/3"
    PAGE_SIZE = 100

    def __init__(self, domain: str, email: str, api_token: str) -> None:
        self.base_url = f"https://{domain}.atlassian.net{self.BASE_PATH}"
        credentials = base64.b64encode(f"{email}:{api_token}".encode()).decode()
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers={
                "Authorization": f"Basic {credentials}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )
        self._max_retries = 5

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> JiraClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Make a request with retry logic for rate limiting."""
        for attempt in range(self._max_retries):
            response = await self._client.request(method, path, **kwargs)
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 2 ** attempt))
                log.warning("Rate limited, retrying after %ds", retry_after)
                await asyncio.sleep(retry_after)
                continue
            response.raise_for_status()
            return response
        raise httpx.HTTPStatusError(
            "Max retries exceeded due to rate limiting",
            request=response.request,
            response=response,
        )

    async def get_myself(self) -> dict[str, Any]:
        """Validate credentials and return current user info."""
        resp = await self._request("GET", "/myself")
        return resp.json()

    async def get_projects(self) -> list[dict[str, Any]]:
        """Fetch all accessible projects with pagination."""
        projects: list[dict[str, Any]] = []
        start_at = 0
        while True:
            resp = await self._request(
                "GET",
                "/project/search",
                params={"startAt": start_at, "maxResults": self.PAGE_SIZE},
            )
            data = resp.json()
            projects.extend(data.get("values", []))
            if data.get("isLast", True):
                break
            start_at += self.PAGE_SIZE
        return projects

    async def search_issues(
        self,
        project_key: str,
        *,
        fields: str = "*all",
        expand: str = "renderedFields,names",
    ) -> tuple[int, list[dict[str, Any]]]:
        """Search all issues for a project using JQL with pagination.

        Returns (total_count, issues_list).
        Uses the newer /search/jql endpoint with nextPageToken pagination.
        Falls back to classic /search if the newer endpoint is unavailable.
        """
        jql = f"project = {project_key} ORDER BY key ASC"
        all_issues: list[dict[str, Any]] = []
        total = 0

        # Try the newer /search/jql endpoint first
        try:
            next_page_token: str | None = None
            first_page = True
            while first_page or next_page_token:
                first_page = False
                params: dict[str, Any] = {
                    "jql": jql,
                    "fields": fields,
                    "maxResults": self.PAGE_SIZE,
                }
                if next_page_token:
                    params["nextPageToken"] = next_page_token
                resp = await self._request("GET", "/search/jql", params=params)
                data = resp.json()
                if total == 0:
                    total = data.get("total", 0)
                all_issues.extend(data.get("issues", []))
                next_page_token = data.get("nextPageToken")
            return total, all_issues
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
                raise
            log.info("Falling back to classic /search endpoint")

        # Fallback to classic /search endpoint
        start_at = 0
        while True:
            resp = await self._request(
                "GET",
                "/search",
                params={
                    "jql": jql,
                    "fields": fields,
                    "expand": expand,
                    "startAt": start_at,
                    "maxResults": self.PAGE_SIZE,
                },
            )
            data = resp.json()
            if total == 0:
                total = data.get("total", 0)
            issues = data.get("issues", [])
            if not issues:
                break
            all_issues.extend(issues)
            start_at += len(issues)
            if start_at >= total:
                break
        return total, all_issues

    async def get_issue_comments(self, issue_key: str) -> list[dict[str, Any]]:
        """Fetch all comments for an issue."""
        comments: list[dict[str, Any]] = []
        start_at = 0
        while True:
            resp = await self._request(
                "GET",
                f"/issue/{issue_key}/comment",
                params={"startAt": start_at, "maxResults": self.PAGE_SIZE},
            )
            data = resp.json()
            comments.extend(data.get("comments", []))
            if start_at + self.PAGE_SIZE >= data.get("total", 0):
                break
            start_at += self.PAGE_SIZE
        return comments

    async def get_fields(self) -> list[dict[str, Any]]:
        """Fetch field definitions for custom field mapping."""
        resp = await self._request("GET", "/field")
        return resp.json()
