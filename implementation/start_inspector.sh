#!/bin/bash
# Start the MCP Inspector
mkdir -p .npm-cache
NPM_CONFIG_CACHE="$PWD/.npm-cache" npx -y @modelcontextprotocol/inspector python ./mcp_server.py
