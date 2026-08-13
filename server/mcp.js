'use strict';

const { spawn } = require('child_process');

// ─── MCP client — spawns @confluentinc/mcp-confluent as a child process ───────
// Communication is over stdio using the MCP JSON-RPC 2.0 protocol.
// All Confluent connection credentials are passed via the inherited process.env
// (already loaded from .env by server.js before this module is required).

let _child    = null;
let _msgId    = 0;
const _pending = new Map(); // id → { resolve, reject, timer }
let   _buffer  = '';
let   _ready   = false; // true once initialize handshake completes

// ─── Route a natural-language prompt to an MCP tool call ──────────────────────
// Returns { tool, args } or null (falls back to search-product-docs).
function routePrompt(prompt) {
  const p = prompt.toLowerCase();
  if (/topic|stream|message|consumer|produce|offset|lag/.test(p))
    return { tool: 'list-topics', args: {} };
  if (/flink|statement|sql|aggregat|window/.test(p))
    return { tool: 'list-flink-statements', args: {} };
  if (/connector/.test(p))
    return { tool: 'list-connectors', args: {} };
  if (/schema|avro|registry/.test(p))
    return { tool: 'list-schemas', args: {} };
  if (/metric|throughput|bytes|request/.test(p))
    return { tool: 'list-available-metrics', args: {} };
  if (/cluster|environment|org/.test(p))
    return { tool: 'list-clusters', args: {} };
  // default: search Confluent docs
  return { tool: 'search-product-docs', args: { query: prompt } };
}

// ─── Extract a plain-text string from an MCP tools/call result ────────────────
function extractText(result) {
  if (!result) return '(no response)';
  // Standard MCP CallToolResult: { content: [{ type, text }], isError? }
  if (Array.isArray(result.content)) {
    return result.content
      .filter(c => c.type === 'text')
      .map(c => c.text)
      .join('\n') || '(empty response)';
  }
  // Fallback: stringify whatever came back
  return typeof result === 'string' ? result : JSON.stringify(result, null, 2);
}

// ─── Send a single JSON-RPC message (no reply expected — notifications) ────────
function _send(child, msg) {
  child.stdin.write(JSON.stringify(msg) + '\n');
}

// ─── Send a JSON-RPC request and wait for the matching response ────────────────
function _request(child, method, params, timeoutMs = 30_000) {
  return new Promise((resolve, reject) => {
    const id = ++_msgId;
    const timer = setTimeout(() => {
      _pending.delete(id);
      reject(new Error(`MCP request timed out: ${method}`));
    }, timeoutMs);
    _pending.set(id, { resolve, reject, timer });
    child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n');
  });
}

// ─── Spawn the child and perform the MCP initialize handshake ─────────────────
async function _spawnAndInit() {
  // @confluentinc/mcp-confluent expects different env var names than this server.
  const mcpEnv = {
    ...process.env,
    // Schema Registry: our SCHEMA_REGISTRY_URL → MCP's SCHEMA_REGISTRY_ENDPOINT
    SCHEMA_REGISTRY_ENDPOINT: process.env.SCHEMA_REGISTRY_URL,
    // Kafka credentials: our CONSUMER_API_KEY/SECRET → MCP's KAFKA_API_KEY/SECRET
    KAFKA_API_KEY:    process.env.CONSUMER_API_KEY,
    KAFKA_API_SECRET: process.env.CONSUMER_API_SECRET,
  };

  const child = spawn('npx', ['@confluentinc/mcp-confluent'], {
    env:   mcpEnv,
    stdio: ['pipe', 'pipe', 'inherit'], // stdin writable, stdout readable, stderr to console
  });

  child.stdout.setEncoding('utf8');
  child.stdout.on('data', (chunk) => {
    _buffer += chunk;
    const lines = _buffer.split('\n');
    _buffer = lines.pop(); // keep incomplete last line

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      try {
        const msg = JSON.parse(trimmed);
        // Only dispatch messages that have an id (responses to our requests)
        if (msg.id == null) continue;
        const handler = _pending.get(msg.id);
        if (!handler) continue;
        clearTimeout(handler.timer);
        _pending.delete(msg.id);
        if (msg.error) {
          handler.reject(new Error(msg.error.message || 'MCP error'));
        } else {
          handler.resolve(msg.result);
        }
      } catch (_) { /* non-JSON output from the child — ignore */ }
    }
  });

  child.on('exit', (code) => {
    console.warn(`[mcp] Child process exited with code ${code}. Will respawn on next request.`);
    _child = null;
    _ready = false;
    // Reject any in-flight requests
    for (const [id, handler] of _pending) {
      clearTimeout(handler.timer);
      handler.reject(new Error('MCP process exited unexpectedly'));
      _pending.delete(id);
    }
  });

  // MCP handshake: initialize → wait for server response → send initialized notification
  await _request(child, 'initialize', {
    protocolVersion: '2024-11-05',
    capabilities: {},
    clientInfo: { name: 'celltower-dashboard', version: '1.0.0' },
  });
  _send(child, { jsonrpc: '2.0', method: 'notifications/initialized' });

  return child;
}

async function getChild() {
  if (_child && !_child.killed && _ready) return _child;
  _child = await _spawnAndInit();
  _ready = true;
  return _child;
}

/**
 * Route a natural-language prompt to the appropriate MCP tool, call it,
 * and return the result as a plain text string.
 * @param {string} prompt
 * @returns {Promise<string>}
 */
async function queryMcp(prompt) {
  const child = await getChild();
  const { tool, args } = routePrompt(prompt);
  const result = await _request(child, 'tools/call', { name: tool, arguments: args });
  return extractText(result);
}

module.exports = { queryMcp };
