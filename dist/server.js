import { Server } from '@modelcontextprotocol/sdk/server';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { 
// Tools
ListToolsRequestSchema, CallToolRequestSchema, 
// Resources
ListResourcesRequestSchema, ReadResourceRequestSchema } from '@modelcontextprotocol/sdk/types.js';
// ---------------------------
// Backend plumbing
// ---------------------------
const ANALYST_BASE = process.env.ANALYST_BASE ?? 'http://127.0.0.1:8080';
// Generic HTTP helpers (fix TS2322 by returning typed values)
async function httpPost(path, body) {
    const res = await fetch(`${ANALYST_BASE}${path}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
    });
    if (!res.ok)
        throw new Error(`${path} ${res.status}: ${await res.text()}`);
    return (await res.json());
}
async function httpGet(path) {
    const res = await fetch(`${ANALYST_BASE}${path}`);
    if (!res.ok)
        throw new Error(`${path} ${res.status}: ${await res.text()}`);
    return (await res.json());
}
// Helper to read credentials from environment
function getCredentialsFromEnv() {
    // First check if credentials are provided directly as JSON
    const credentialsJson = process.env.GOOGLE_CREDENTIALS_JSON;
    if (credentialsJson) {
        return credentialsJson;
    }
    // Otherwise, try to read from the credentials file path
    const credentialsPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
    if (credentialsPath) {
        try {
            const fs = require('fs');
            const credentials = fs.readFileSync(credentialsPath, 'utf8');
            return credentials;
        }
        catch (error) {
            throw new Error(`Failed to read credentials from ${credentialsPath}: ${error}`);
        }
    }
    throw new Error('No credentials found. Set either GOOGLE_CREDENTIALS_JSON or GOOGLE_APPLICATION_CREDENTIALS environment variable');
}
// Helper to extract and validate credentials from environment
function extractCredentials(args) {
    if (!args.profileId) {
        throw new Error('profileId parameter is required for BigQuery operations');
    }
    const credentials = getCredentialsFromEnv();
    return {
        profileId: args.profileId,
        credentials
    };
}
const tools = new Map();
function registerTool(def) {
    tools.set(def.name, def);
}
registerTool({
    name: 'SuggestVisualization',
    description: 'Return a Vega-Lite chart spec that best explains the answer. ' +
        'Call this whenever a metric, trend, comparison, breakdown, or distribution would be clearer with a chart. ' +
        'Prefer bar/line/area for trends and comparisons; scatter for relationships; ' +
        'fallback to a table when data is sparse. Use the provided rows (preferred) or SQL if rows are large.',
    inputSchema: {
        type: 'object',
        properties: {
            profileId: { type: 'string', description: 'Profile ID (optional for legacy mode)' },
            // Use EITHER rows (preferred) OR dataset+sql (backend will sample rows)
            rows: {
                type: 'array',
                description: 'Result rows to visualize (ideally ≤ 2k).',
                items: { type: 'object', additionalProperties: true }
            },
            dataset: { type: 'string', description: 'Dataset for SQL execution if rows omitted' },
            sql: { type: 'string', description: 'Query used to produce the answer if rows omitted' },
            // Optional hints
            question: { type: 'string', description: 'Original user question to guide chart choice' },
            prefer: { type: 'array', items: { type: 'string', enum: ['bar', 'line', 'area', 'scatter', 'table', 'map', 'pie', 'hist'] } },
            maxSeries: { type: 'number', description: 'Cap number of series/categories (default 12)' },
            maxRows: { type: 'number', description: 'Server-side sample limit when using SQL (default 2000)' }
        },
        additionalProperties: false
    },
    outputSchema: {
        type: 'object',
        required: ['format', 'spec'],
        properties: {
            format: { type: 'string', enum: ['vega-lite'] },
            spec: { type: 'object' },
            title: { type: 'string' },
            rationale: { type: 'string' },
            fields: {
                type: 'object',
                properties: {
                    measures: { type: 'array', items: { type: 'string' } },
                    dimensions: { type: 'array', items: { type: 'string' } },
                    time: { type: 'string' },
                    aggregation: { type: 'string' }
                }
            }
        }
    },
    handler: async ({ profileId, rows, dataset, sql, question, prefer, maxSeries, maxRows }) => {
        // Post to your backend viz suggester. The backend can:
        //  - infer field types, choose encodings, and build a Vega-Lite spec
        //  - inline data or reference a data URL (spec.data.values or spec.data.url)
        //  - return rationale + detected fields
        const payload = { profileId, rows, dataset, sql, question, prefer, maxSeries, maxRows };
        return httpPost('/suggest-viz', payload);
    }
});
registerTool({
    name: 'GetTableGraph',
    description: 'Returns the pre-built graph for this profile with tables, schemas, and connections based on attached datasets/tables. When a question is provided, the graph is filtered to show only tables relevant to answering that question. Always provide a question to get a focused, relevant graph.',
    inputSchema: {
        type: 'object',
        required: ['profileId'],
        properties: {
            profileId: { type: 'string', description: 'Profile ID' },
            question: { type: 'string', description: 'Optional question to filter the graph to only relevant tables. Highly recommended to provide this for better results.' }
        },
    },
    outputSchema: { type: 'object' },
    handler: async (args) => {
        const { profileId, credentials } = extractCredentials(args);
        const { question } = args;
        return httpPost('/graph', { profileId, credentials, question });
    },
});
registerTool({
    name: 'CheckHealth',
    description: 'Check health status of the server. When profileId is provided, returns whether the profile graph is ready (ready: true/false).',
    inputSchema: {
        type: 'object',
        properties: {
            profileId: { type: 'string', description: 'Optional profile ID to check if graph is ready' }
        },
    },
    outputSchema: { type: 'object' },
    handler: async (args) => {
        const { profileId } = args;
        const url = profileId ? `/health?profileId=${encodeURIComponent(profileId)}` : '/health';
        return httpGet(url);
    },
});
registerTool({
    name: 'GetUptoNDistinctStringValues',
    description: 'Return up to limit distinct string values for a non-numeric column.',
    inputSchema: {
        type: 'object',
        required: ['profileId', 'dataset', 'table', 'column'],
        properties: {
            profileId: { type: 'string', description: 'Profile ID' },
            dataset: { type: 'string' },
            table: { type: 'string' },
            column: { type: 'string' },
            limit: { type: 'number' },
        },
    },
    outputSchema: { type: 'array', items: { type: 'string' } },
    handler: async (args) => {
        const { profileId, credentials } = extractCredentials(args);
        const { dataset, table, column, limit } = args;
        // For GET requests with credentials, we need to send them as a POST body instead
        return httpPost('/distinct-values', {
            profileId,
            credentials,
            dataset,
            table,
            column,
            limit
        });
    },
});
registerTool({
    name: 'SearchQuestionBank',
    description: 'Search the question bank for similar questions and their SQL queries. ALWAYS call this FIRST before writing any SQL query or calling ExecuteSQL. Use topK=1 or topK=3 to find the most similar question. If a matching question is found with high similarity (>0.8), you should reuse its SQL query instead of writing a new one. This saves time and ensures consistency.',
    inputSchema: {
        type: 'object',
        required: ['profileId', 'question'],
        properties: {
            profileId: { type: 'string', description: 'Profile ID' },
            question: { type: 'string', description: 'Question to search for in the question bank' },
            topK: { type: 'number', description: 'Number of top similar questions to return (default: 5)' }
        },
    },
    outputSchema: {
        type: 'array',
        items: {
            type: 'object',
            required: ['entry', 'similarity'],
            properties: {
                entry: {
                    type: 'object',
                    required: ['question', 'sql'],
                    properties: {
                        question: { type: 'string' },
                        sql: { type: 'string' },
                        sqlFlowDiagram: { type: 'string' }
                    }
                },
                similarity: { type: 'number' }
            }
        }
    },
    handler: async (args) => {
        const { profileId, question, topK } = args;
        const k = topK ?? 5;
        const encodedQuestion = encodeURIComponent(question);
        return httpGet(`/question-bank/${profileId}?question=${encodedQuestion}&k=${k}`);
    },
});
registerTool({
    name: 'ExecuteSQL',
    description: 'Execute a SELECT/WITH query against a dataset with LLM validation and diagram generation. The question parameter is required and triggers validation before execution. Returns rows + validation evidence + diagram. IMPORTANT: Before calling this tool, you should ALWAYS call SearchQuestionBank first to check if a similar question already exists with a working SQL query that you can reuse.',
    inputSchema: {
        type: 'object',
        required: ['profileId', 'dataset', 'sql', 'question'],
        properties: {
            profileId: { type: 'string', description: 'Profile ID' },
            dataset: { type: 'string' },
            sql: { type: 'string' },
            question: { type: 'string', description: 'Original question - required for LLM validation and diagram generation' }
        },
    },
    outputSchema: { type: 'object' },
    handler: async (args) => {
        const { profileId, credentials } = extractCredentials(args);
        const { dataset, sql, question } = args;
        return httpPost('/execute-sql', { profileId, credentials, dataset, sql, question });
    },
});
// --- Resource management tools (persisted on backend) ---
registerTool({
    name: 'ListResources',
    description: 'PRIMARY TOOL for understanding domain-specific terminology, glossary terms, business definitions, and customer-specific knowledge. ' +
        'ALWAYS call this tool FIRST when encountering unfamiliar terms, business concepts, or domain-specific vocabulary in user questions. ' +
        'Examples: When user asks about "customers", "revenue", "conversions", or any business term - search this FIRST to understand the specific definition in this domain. ' +
        'Uses semantic similarity search (powered by embeddings) to find the most relevant resources. ' +
        'Returns: glossary definitions, taxonomies, business rules, metrics definitions, and other domain knowledge. ' +
        'RECOMMENDED WORKFLOW: 1) Search ListResources for key terms in question → 2) Use definitions to guide SQL/analysis. ' +
        'Without a query, returns all available resources.',
    inputSchema: {
        type: 'object',
        properties: {
            query: {
                type: 'string',
                description: 'Natural language query to search for relevant resources using semantic similarity. Example: "What is a customer?" or "revenue calculation rules"'
            },
            topK: {
                type: 'number',
                description: 'Number of top similar resources to return (default: 10). Only used when query is provided.'
            }
        },
    },
    outputSchema: {
        type: 'array',
        items: {
            type: 'object',
            properties: {
                resource: {
                    type: 'object',
                    properties: {
                        id: { type: 'string' },
                        title: { type: 'string' },
                        type: { type: 'string' },
                        text: { type: 'string' }
                    }
                },
                similarity: { type: 'number', description: 'Similarity score (0-1) when using semantic search' }
            }
        }
    },
    handler: async (args) => {
        const { query, topK } = args;
        if (query) {
            // Semantic search mode
            const k = topK ?? 10;
            const encodedQuery = encodeURIComponent(query);
            return httpGet(`/resources?query=${encodedQuery}&k=${k}`);
        }
        else {
            // List all resources
            return httpGet('/resources');
        }
    },
});
registerTool({
    name: 'UpsertResource',
    description: 'Create or update a resource (e.g., glossary/taxonomy) that persists across sessions. ' +
        'The resource text is automatically embedded using OpenAI embeddings, enabling semantic search via ListResources. ' +
        'Use this to store domain knowledge, business rules, definitions, or any context that should be searchable later.',
    inputSchema: {
        type: 'object',
        required: ['id', 'title', 'type', 'text'],
        properties: {
            id: { type: 'string', description: 'Unique resource ID/URI (e.g., "glossary-customer", "rule-revenue-calc")' },
            title: { type: 'string', description: 'Human-readable title' },
            type: { type: 'string', description: 'Resource type: glossary, taxonomy, facts, reasoning, gen' },
            text: { type: 'string', description: 'Resource content - will be embedded for semantic search' },
        },
        additionalProperties: false,
    },
    outputSchema: { type: 'object' },
    handler: async (args) => httpPost('/resources', args),
});
registerTool({
    name: 'DeleteResource',
    description: 'Delete a resource by ID from the persistent backend.',
    inputSchema: {
        type: 'object',
        required: ['id'],
        properties: { id: { type: 'string' } },
    },
    outputSchema: { type: 'object' },
    handler: async ({ id }) => {
        const res = await fetch(`${ANALYST_BASE}/resources/${encodeURIComponent(id)}`, {
            method: 'DELETE',
        });
        if (!res.ok)
            throw new Error(`/resources/${id} ${res.status}: ${await res.text()}`);
        return (await res.json());
    },
});
registerTool({
    name: 'DeleteQuestion',
    description: 'Delete a specific question from a profile\'s question bank. Use this to remove outdated or incorrect questions.',
    inputSchema: {
        type: 'object',
        required: ['profileId', 'question'],
        properties: {
            profileId: { type: 'string', description: 'Profile ID' },
            question: { type: 'string', description: 'The exact question text to delete' }
        },
    },
    outputSchema: { type: 'object' },
    handler: async ({ profileId, question }) => {
        const encodedQuestion = encodeURIComponent(question);
        const res = await fetch(`${ANALYST_BASE}/question-bank/${encodeURIComponent(profileId)}?question=${encodedQuestion}`, {
            method: 'DELETE',
        });
        if (!res.ok)
            throw new Error(`/question-bank/${profileId} ${res.status}: ${await res.text()}`);
        return (await res.json());
    },
});
// ---------------------------
// MCP server (tools + resources)
// ---------------------------
const server = new Server({ name: 'cardinal-bq-analyst', version: '0.1.0' }, {
    capabilities: {
        tools: {},
        resources: {}, // advertise resources capability
    },
});
// --- Advertise tools ---
server.setRequestHandler(ListToolsRequestSchema, async (_request) => ({
    tools: Array.from(tools.values()).map((t) => ({
        name: t.name,
        description: t.description ?? '',
        inputSchema: t.inputSchema,
    })),
}));
// --- Handle tool calls ---
server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;
    if (!name || !tools.has(name))
        throw new Error(`Unknown tool: ${name}`);
    const def = tools.get(name);
    const result = await def.handler(args ?? {});
    // Special handling for ExecuteSQL which returns a Mermaid diagram
    // The diagram is in result.evidence.sql_flow_diagram
    if (name === 'ExecuteSQL' && result && typeof result === 'object' &&
        result.evidence && result.evidence.sql_flow_diagram) {
        const content = [];
        // Add the main result as text
        content.push({ type: 'text', text: JSON.stringify(result) });
        // Add the Mermaid diagram as a resource
        if (typeof result.evidence.sql_flow_diagram === 'string') {
            content.push({
                type: 'resource',
                resource: {
                    uri: `mermaid://sql-execution-diagram/${Date.now()}`,
                    mimeType: 'text/vnd.mermaid',
                    text: result.evidence.sql_flow_diagram
                }
            });
        }
        return { content };
    }
    return { content: [{ type: 'text', text: JSON.stringify(result) }] };
});
// ---------------------------
// MCP Resources (list/read)
// ---------------------------
// List known resources (proxied from backend)
server.setRequestHandler(ListResourcesRequestSchema, async (_req) => {
    const list = await httpGet('/resources');
    return {
        resources: list.map((r) => ({
            uri: r.id,
            name: r.title ?? r.id,
            description: r.type ? `Type: ${r.type}` : '',
            mimeType: 'text/plain',
        })),
    };
});
// Read a single resource content by ID
server.setRequestHandler(ReadResourceRequestSchema, async (req) => {
    const { uri } = req.params;
    // Get all resources and find the matching one
    const list = await httpGet('/resources');
    const r = list.find(resource => resource.id === uri);
    if (!r) {
        throw new Error(`Resource not found: ${uri}`);
    }
    return {
        contents: [
            {
                uri: r.id,
                mimeType: 'text/plain',
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
