FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mcp_server.py .
COPY code/ code/

ENV MCP_HOST=0.0.0.0
ENV MCP_PORT=9090

EXPOSE 9090

CMD ["python", "mcp_server.py"]
