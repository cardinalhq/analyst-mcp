This project is a tool-calling agent that uses the Model Context Protocol SDK to interact with a BigQuery Analyst service. The agent exposes a set of tools that allow an LLM (Large Language Model) to perform tasks related to data analysis in Google BigQuery.

# Getting Started
To use this tool-calling agent, you'll need to have the underlying BigQuery Analyst service running. This agent acts as a bridge, exposing the analyst's capabilities as a set of callable tools.

## Available Tools
The agent exposes several tools for querying and analyzing BigQuery data. Each tool is designed to perform a specific task, which an LLM can leverage to answer user questions or explore data.

### `GetBigQueryDataSets`: 

- Returns a list of all datasets available in the BigQuery project. 
- `Description`: "Return all datasets in this Google BigQuery project."

### GetTableGraph: 
- Retrieves the schema and relationships for tables within specified datasets.
- `Description`: "Returns the tables, their schemas and how those tables are connected."

### GetRelevantQuestions:

- Finds and ranks similar questions based on a given query, which is useful for retrieving past insights or examples.
- `Description`: "Given a question, return relevant questions ranked by similarity."

### GetUptoNDistinctStringValues:
- Fetches a limited number of distinct string values from a specified column.
- `Description`: "Return up to limit distinct string values for a non-numeric column."

### ValidateQuestionSQL: 
- Performs a dry-run of a SQL query and validates its correctness against a given question.
- `Description`: "Dry-run/EXPLAIN the SQL, then ask LLM if the generated SQL can correctly answer the question."

### ExecuteSQL:
- Executes a validated SQL query and returns the results, along with "evidence" that explains how the results answer the original question.
- `Description`: "Execute a validated SELECT/WITH query against a dataset and return rows + evidence that the query was indeed correct. Evidence explains how the returned rows answer the question."

## How It Works
The agent uses the @modelcontextprotocol/sdk to set up a server that communicates via standard I/O (StdioServerTransport). It defines handlers for two key requests from an LLM:
- `ListToolsRequest`: When an LLM asks what tools are available, the agent responds with a list of the registered tools and their schemas. 
- `CallToolRequest`: When an LLM wants to use a specific tool, the agent looks up the corresponding handler function and executes it. This handler typically makes an HTTP request to the backend BigQuery Analyst service.

The agent's logic is encapsulated in the registerTool function, which maps a tool name to its description, input/output schemas, and an asynchronous handler. The ANALYST_BASE environment variable determines the location of the backend service.

## Project Structure
`ANALYST_BASE`: Environment variable for the backend service URL (defaults to http://127.0.0.1:8080).

- `httpPost`, `httpGet`: Helper functions for making HTTP requests to the backend. 
- `registerTool`: A utility function to simplify the process of adding new tools. 
- `tools`: A Map that stores all the registered tool definitions. 
- `Server`: An instance of the Model Context Protocol SDK server that handles communication. 
- `StdioServerTransport`: Configures the server to use standard input/output for communication, making it easy to integrate with various platforms.

## Deployment
To deploy this agent, you'll need to run it as a process that can communicate with an LLM via standard I/O. For example, if you're using a platform like Google Cloud Vertex AI, you can configure it to run this script as a subprocess, with the LLM providing input and receiving output through the standard I/O streams.