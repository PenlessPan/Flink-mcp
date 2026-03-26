import logging

import httpx

from .app import mcp

logger = logging.getLogger("flink-mcp-server")

MAX_OUTPUT_CHARS = 50000

FLINK_CONNECTION = {
    "url": None,
    "initialized": False,
}


def get_settings():
    if FLINK_CONNECTION["initialized"]:
        return FLINK_CONNECTION
    raise RuntimeError(
        "Flink connection not initialized. Call initialize_flink_connection first."
    )


@mcp.tool()
async def initialize_flink_connection(flink_url: str) -> str:
    """
    Initialize connection to Apache Flink REST API.
    Must be called before using any other Flink tools.

    Args:
        flink_url: Base URL of Flink REST API (e.g., http://localhost:8081)
    """
    flink_url = flink_url.rstrip('/')
    try:
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(f"{flink_url}/overview", timeout=5.0)
            response.raise_for_status()

        FLINK_CONNECTION["url"] = flink_url
        FLINK_CONNECTION["initialized"] = True
        logger.info(f"Successfully connected to Flink at: {flink_url}")
        return f"✓ Successfully connected to Flink cluster at {flink_url}"
    except Exception as e:
        logger.error(f"Failed to connect to Flink: {e}")
        return f"✗ Failed to connect to Flink at {flink_url}: {str(e)}"


@mcp.tool()
async def get_connection_status() -> str:
    """Check if connection to Flink is initialized and return the current URL."""
    if FLINK_CONNECTION["initialized"]:
        return f"✓ Connected to: {FLINK_CONNECTION['url']}"
    return "✗ Not connected. Call initialize_flink_connection first."
