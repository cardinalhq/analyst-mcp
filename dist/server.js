import { Server } from '@modelcontextprotocol/sdk/server';
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { ListToolsRequestSchema, CallToolRequestSchema } from '@modelcontextprotocol/sdk/types.js';
const ANALYST_BASE = process.env.ANALYST_BASE ?? "http://127.0.0.1:8080";
async function httpPost(path, body) {
    const res = await fetch(`${ANALYST_BASE}${path}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
    });
    if (!res.ok)
        throw new Error(`${path} ${res.status}: ${await res.text()}`);
    return res.json();
}
async function httpGet(path) {
    const res = await fetch(`${ANALYST_BASE}${path}`);
    if (!res.ok)
        throw new Error(`${path} ${res.status}: ${await res.text()}`);
    return res.json();
}
const tools = new Map();
function registerTool(def) {
    tools.set(def.name, def);
}
// ---- tool registrations ----
registerTool({
    name: "GetBigQueryDataSets",
    description: "Return all datasets in this Google BigQuery project.",
    inputSchema: { type: "object", properties: {} },
    outputSchema: { type: "array", items: { type: "string" } },
    handler: async () => httpGet("/datasets"),
});
registerTool({
    name: "GetTableGraph",
    description: "Returns the tables, their schemas and how those tables are connected.",
    inputSchema: {
        type: "object",
        properties: { datasets: { type: "array", items: { type: "string" } } }
    },
    outputSchema: { type: "object" },
    handler: async ({ datasets }) => httpPost("/graph", { datasets: datasets ?? [] }),
});
registerTool({
    name: "GetRelevantQuestions",
    description: "Given a question, return relevant questions ranked by similarity.",
    inputSchema: {
        type: "object",
        required: ["question"],
        properties: {
            datasets: { type: "array", items: { type: "string" } },
            question: { type: "string" },
            topK: { type: "number" }
        }
    },
    outputSchema: { type: "array", items: { type: "object" } },
    handler: async ({ datasets, question, topK }) => httpPost("/relevant-questions", { datasets: datasets ?? [], question, topK }),
});
registerTool({
    name: "GetUptoNDistinctStringValues",
    description: "Return up to limit distinct string values for a non-numeric column.",
    inputSchema: {
        type: "object",
        required: ["dataset", "table", "column"],
        properties: {
            dataset: { type: "string" },
            table: { type: "string" },
            column: { type: "string" },
            limit: { type: "number" }
        }
    },
    outputSchema: { type: "array", items: { type: "string" } },
    handler: async ({ dataset, table, column, limit }) => {
        const u = new URL(`${ANALYST_BASE}/distinct-values`);
        u.searchParams.set("dataset", dataset);
        u.searchParams.set("table", table);
        u.searchParams.set("column", column);
        if (limit != null)
            u.searchParams.set("limit", String(limit));
        const res = await fetch(u.toString());
        if (!res.ok)
            throw new Error(await res.text());
        return res.json();
    },
});
registerTool({
    name: "ValidateQuestionSQL",
    description: "Dry-run/EXPLAIN the SQL, then ask LLM if it can answer the question.",
    inputSchema: {
        type: "object",
        required: ["question", "sql", "dataset"],
        properties: { question: { type: "string" }, sql: { type: "string" }, dataset: { type: "string" } }
    },
    outputSchema: { type: "object" },
    handler: async ({ question, sql, dataset }) => httpPost("/validate-sql", { question, sql, dataset }),
});
registerTool({
    name: "ExecuteSQL",
    description: "Execute a validated SELECT/WITH query against a dataset and return rows + evidence that the query was indeed correct.",
    inputSchema: {
        type: "object",
        required: ["dataset", "sql"],
        properties: { dataset: { type: "string" }, sql: { type: "string" } }
    },
    outputSchema: { type: "object" },
    handler: async ({ dataset, sql }) => httpPost("/execute-sql", { dataset, sql }),
});
// ---- server + stdio transport ----
const server = new Server({ name: "cardinal-bq-analyst", version: "0.1.0" }, { capabilities: { tools: {} } });
// Advertise tools
server.setRequestHandler(ListToolsRequestSchema, async (_request) => ({
    tools: Array.from(tools.values()).map(t => ({
        name: t.name,
        description: t.description ?? "",
        inputSchema: t.inputSchema
    }))
}));
// Handle tool calls
server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;
    if (!name || !tools.has(name)) {
        throw new Error(`Unknown tool: ${name}`);
    }
    const def = tools.get(name);
    const result = await def.handler(args ?? {});
    return { content: [{ type: "text", text: JSON.stringify(result) }] };
});
const transport = new StdioServerTransport();
await server.connect(transport);
