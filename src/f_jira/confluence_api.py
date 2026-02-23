"""Confluence REST API v2 async client."""

from __future__ import annotations

import asyncio
import base64
import logging
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx

log = logging.getLogger(__name__)


class ConfluenceClient:
    """Async Confluence REST API v2 client with rate limiting and cursor pagination."""

    V2_PATH = "/wiki/api/v2"
    V1_PATH = "/wiki/rest/api"
    PAGE_SIZE = 250

    def __init__(self, domain: str, email: str, api_token: str) -> None:
        self.base_url = f"https://{domain}.atlassian.net"
        credentials = base64.b64encode(f"{email}:{api_token}".encode()).decode()
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers={
                "Authorization": f"Basic {credentials}",
                "Accept": "application/json",
            },
            timeout=30.0,
        )
        self._max_retries = 5

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> ConfluenceClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Make a request with retry logic for rate limiting."""
        response: httpx.Response | None = None
        for attempt in range(self._max_retries):
            response = await self._client.request(method, path, **kwargs)
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 2**attempt))
                log.warning("Rate limited, retrying after %ds", retry_after)
                await asyncio.sleep(retry_after)
                continue
            response.raise_for_status()
            return response
        assert response is not None
        raise httpx.HTTPStatusError(
            "Max retries exceeded due to rate limiting",
            request=response.request,
            response=response,
        )

    async def _paginate(
        self, path: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Fetch all pages of a cursor-paginated v2 endpoint.

        Confluence v2 returns ``_links.next`` as a relative URL containing
        the cursor parameter for the next page.
        """
        results: list[dict[str, Any]] = []
        request_params = {"limit": self.PAGE_SIZE, **(params or {})}

        while True:
            resp = await self._request("GET", path, params=request_params)
            data = resp.json()
            results.extend(data.get("results", []))

            next_link = data.get("_links", {}).get("next")
            if not next_link:
                break

            # Extract cursor from the next link URL
            parsed = urlparse(next_link)
            qs = parse_qs(parsed.query)
            cursor = qs.get("cursor", [None])[0]
            if not cursor:
                break
            request_params["cursor"] = cursor

        return results

    # -- API methods --

    async def get_myself(self) -> dict[str, Any]:
        """Validate credentials and return current user info (v1 endpoint)."""
        resp = await self._request("GET", f"{self.V1_PATH}/user/current")
        return resp.json()

    async def get_spaces(self) -> list[dict[str, Any]]:
        """Fetch all accessible spaces."""
        return await self._paginate(f"{self.V2_PATH}/spaces")

    async def get_pages(self, space_id: str) -> list[dict[str, Any]]:
        """Fetch all pages in a space with body content."""
        return await self._paginate(
            f"{self.V2_PATH}/spaces/{space_id}/pages",
            params={"body-format": "storage"},
        )

    async def get_page(self, page_id: str) -> dict[str, Any]:
        """Fetch a single page with body content."""
        resp = await self._request(
            "GET",
            f"{self.V2_PATH}/pages/{page_id}",
            params={"body-format": "storage"},
        )
        return resp.json()

    async def get_footer_comments(self, page_id: str) -> list[dict[str, Any]]:
        """Fetch all footer comments for a page."""
        return await self._paginate(
            f"{self.V2_PATH}/pages/{page_id}/footer-comments",
            params={"body-format": "storage"},
        )

    async def get_labels(self, page_id: str) -> list[dict[str, Any]]:
        """Fetch all labels for a page."""
        return await self._paginate(f"{self.V2_PATH}/pages/{page_id}/labels")
