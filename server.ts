import { Server } from '@modelcontextprotocol/sdk/server';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  // Tools
  ListToolsRequestSchema,
  CallToolRequestSchema,
  ListToolsRequest,
  CallToolRequest,
  // Resources
  ListResourcesRequestSchema,
  ReadResourceRequestSchema,
  ListResourcesRequest,
  ReadResourceRequest
} from '@modelcontextprotocol/sdk/types.js';

// ---------------------------
// Backend plumbing
// ---------------------------

const ANALYST_BASE = process.env.ANALYST_BASE ?? 'http://127.0.0.1:8080';

// Generic HTTP helpers (fix TS2322 by returning typed values)
async function httpPost<T>(path: string, body: any): Promise<T> {
  const res = await fetch(`${ANALYST_BASE}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`${path} ${res.status}: ${await res.text()}`);
  return (await res.json()) as T;
}

async function httpGet<T>(path: string): Promise<T> {
  const res = await fetch(`${ANALYST_BASE}${path}`);
  if (!res.ok) throw new Error(`${path} ${res.status}: ${await res.text()}`);
  return (await res.json()) as T;
}

// ---------------------------
// Types shared with backend
// ---------------------------

export type BackendResourceMeta = {
  uri: string;             // e.g. "resource://glossary"
  name?: string;
  description?: string;
  mimeType?: string;       // e.g. "text/markdown" | "application/json"
  etag?: string;
  tags?: string[];
};

export type BackendResource = BackendResourceMeta & {
  text?: string;           // if textual content
  json?: any;              // if JSON content
};

// ---------------------------
// Tool registry
// ---------------------------

type ToolDef = {
  name: string;
  description?: string;
  inputSchema: any;   // JSON Schema
  outputSchema?: any; // JSON Schema
  handler: (args: any) => Promise<any>;
};

const tools = new Map<string, ToolDef>();
function registerTool(def: ToolDef) {
  tools.set(def.name, def);
}

// ---------------------------
// Tool registrations
// ---------------------------

registerTool({
  name: 'GetBigQueryDataSets',
  description: 'Return all datasets in this Google BigQuery project.',
  inputSchema: { type: 'object', properties: {} },
  outputSchema: { type: 'array', items: { type: 'string' } },
  handler: async () => httpGet<string[]>('/datasets'),
});

registerTool({
  name: 'GetTableGraph',
  description: 'Returns the tables, their schemas and how those tables are connected.',
  inputSchema: {
    type: 'object',
    properties: { datasets: { type: 'array', items: { type: 'string' } } },
  },
  outputSchema: { type: 'object' },
  handler: async ({ datasets }) => httpPost<any>('/graph', { datasets: datasets ?? [] }),
});

registerTool({
  name: 'GetRelevantQuestions',
  description: 'Given a question, return relevant questions ranked by similarity.',
  inputSchema: {
    type: 'object',
    required: ['question'],
    properties: {
      datasets: { type: 'array', items: { type: 'string' } },
      question: { type: 'string' },
      topK: { type: 'number' },
    },
  },
  outputSchema: { type: 'array', items: { type: 'object' } },
  handler: async ({ datasets, question, topK }) =>
      httpPost<any[]>('/relevant-questions', { datasets: datasets ?? [], question, topK }),
});

registerTool({
  name: 'GetUptoNDistinctStringValues',
  description: 'Return up to limit distinct string values for a non-numeric column.',
  inputSchema: {
    type: 'object',
    required: ['dataset', 'table', 'column'],
    properties: {
      dataset: { type: 'string' },
      table: { type: 'string' },
      column: { type: 'string' },
      limit: { type: 'number' },
    },
  },
  outputSchema: { type: 'array', items: { type: 'string' } },
  handler: async ({ dataset, table, column, limit }) => {
    const u = new URL(`${ANALYST_BASE}/distinct-values`);
    u.searchParams.set('dataset', dataset);
    u.searchParams.set('table', table);
    u.searchParams.set('column', column);
    if (limit != null) u.searchParams.set('limit', String(limit));
    const res = await fetch(u.toString());
    if (!res.ok) throw new Error(await res.text());
    return (await res.json()) as string[];
  },
});

registerTool({
  name: 'ValidateQuestionSQL',
  description:
      'Dry-run/EXPLAIN the SQL, then ask LLM if the generated SQL can correctly answer the question.',
  inputSchema: {
    type: 'object',
    required: ['question', 'sql', 'dataset'],
    properties: { question: { type: 'string' }, sql: { type: 'string' }, dataset: { type: 'string' } },
  },
  outputSchema: { type: 'object' },
  handler: async ({ question, sql, dataset }) =>
      httpPost<any>('/validate-sql', { question, sql, dataset }),
});

registerTool({
  name: 'ExecuteSQL',
  description:
      'Execute a validated SELECT/WITH query against a dataset and return rows + evidence that the query was indeed correct.',
  inputSchema: {
    type: 'object',
    required: ['dataset', 'sql'],
    properties: { dataset: { type: 'string' }, sql: { type: 'string' } },
  },
  outputSchema: { type: 'object' },
  handler: async ({ dataset, sql }) => httpPost<any>('/execute-sql', { dataset, sql }),
});

// --- Resource management tools (persisted on backend) ---

registerTool({
  name: 'UpsertResource',
  description:
      'Create or update a resource (e.g., glossary/taxonomy) so it persists across sessions and is visible to the agent and backend.',
  inputSchema: {
    type: 'object',
    required: ['uri'],
    properties: {
      uri: { type: 'string' },
      name: { type: 'string' },
      description: { type: 'string' },
      mimeType: { type: 'string' },
      tags: { type: 'array', items: { type: 'string' } },
      text: { type: 'string' },
      json: { type: 'object' },
    },
    oneOf: [
      { required: ['text'] },
      { required: ['json'] },
    ],
  },
  outputSchema: { type: 'object' },
  handler: async (args) =>
      httpPost<BackendResourceMeta>('/resources/upsert', args),
});

registerTool({
  name: 'DeleteResource',
  description: 'Delete a resource by URI from the persistent backend.',
  inputSchema: {
    type: 'object',
    required: ['uri'],
    properties: { uri: { type: 'string' } },
  },
  outputSchema: { type: 'object' },
  handler: async ({ uri }) =>
      httpPost<{ ok: boolean }>('/resources/delete', { uri }),
});

// ---------------------------
// MCP server (tools + resources)
// ---------------------------

const server = new Server(
    { name: 'cardinal-bq-analyst', version: '0.1.0' },
    {
      capabilities: {
        tools: {},
        resources: {}, // advertise resources capability
      },
    }
);

// --- Advertise tools ---
server.setRequestHandler(ListToolsRequestSchema, async (_request: ListToolsRequest) => ({
  tools: Array.from(tools.values()).map((t) => ({
    name: t.name,
    description: t.description ?? '',
    inputSchema: t.inputSchema,
  })),
}));

// --- Handle tool calls ---
server.setRequestHandler(CallToolRequestSchema, async (request: CallToolRequest) => {
  const { name, arguments: args } = request.params;
  if (!name || !tools.has(name)) throw new Error(`Unknown tool: ${name}`);
  const def = tools.get(name)!;
  const result = await def.handler(args ?? {});
  return { content: [{ type: 'text', text: JSON.stringify(result) }] };
});

// ---------------------------
// MCP Resources (list/read)
// ---------------------------

// List known resources (proxied from backend)
server.setRequestHandler(ListResourcesRequestSchema, async (_req: ListResourcesRequest) => {
  const list = await httpGet<BackendResourceMeta[]>('/resources/list');
  return {
    resources: list.map((r) => ({
      uri: r.uri,
      name: r.name ?? r.uri,
      description: r.description ?? '',
      mimeType: r.mimeType ?? 'text/plain',
    })),
  };
});

// Read a single resource content by URI
server.setRequestHandler(ReadResourceRequestSchema, async (req: ReadResourceRequest) => {
  const { uri } = req.params;
  const r = await httpGet<BackendResource>(`/resources/get?uri=${encodeURIComponent(uri)}`);

  // Prefer JSON if present
  if (r.mimeType === 'application/json' && r.json !== undefined) {
    return {
      contents: [
        {
          uri: r.uri,
          mimeType: 'application/json',
          text: JSON.stringify(r.json, null, 2),
        },
      ],
    };
  }

  // Fallback to text (or empty string)
  return {
    contents: [
      {
        uri: r.uri,
        mimeType: r.mimeType ?? 'text/plain',
        text: r.text ?? '',
      },
    ],
  };
});

// ---------------------------
// Boot
// ---------------------------

const transport = new StdioServerTransport();
await server.connect(transport);