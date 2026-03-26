from fastmcp import FastMCP
from typing import Any

mcp = FastMCP("Apache Flink MCP Server")

def _sanitize_schema(schema: Any) -> Any:
    if isinstance(schema, dict):
        if "type" in schema and isinstance(schema["type"], list):
            non_null = [t for t in schema["type"] if t != "null"]
            schema["type"] = non_null[0] if non_null else schema["type"][0]
        return {k: _sanitize_schema(v) for k, v in schema.items()}
    elif isinstance(schema, list):
        return [_sanitize_schema(i) for i in schema]
    return schema

_original_list_tools = mcp.__class__._list_tools

async def _patched_list_tools(self):
    tools = await _original_list_tools(self)
    for tool in tools:
        if tool.inputSchema:
            tool.inputSchema = _sanitize_schema(tool.inputSchema)
    return tools

mcp.__class__._list_tools = _patched_list_tools