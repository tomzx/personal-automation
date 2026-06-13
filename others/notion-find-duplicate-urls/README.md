# notion-find-duplicate-urls (Notion Worker)

A [Notion Worker](https://developers.notion.com/workers/reference/sdk) that finds
and optionally removes duplicate URLs in a Notion data source (database).

This replaces the previous standalone `notion-find-duplicate-urls.py` CLI
script. Instead of running locally from a terminal, the logic now runs on
Notion's hosted runtime and is exposed as tools a Notion custom agent can call.

## Tools

- **Find Duplicate URLs** (`findDuplicateUrls`) — scans a data source and reports
  entries that share the same URL. Read-only; modifies nothing.
- **Deduplicate URLs** (`deduplicateUrls`) — keeps the oldest entry per URL,
  copies over the most recent read time and rating, and archives the remaining
  duplicates. Supports `dryRun` to preview without making changes.

### Inputs

| Input              | Tool(s)            | Default       | Description                                            |
| ------------------ | ------------------ | ------------- | ------------------------------------------------------ |
| `dataSourceId`     | both               | —             | ID of the data source (database) to process.           |
| `urlProperty`      | both               | `URL`         | Name of the URL property (case-insensitive).           |
| `readTimeProperty` | `deduplicateUrls`  | `Read time`   | Date property preserved when deduplicating.            |
| `ratingProperty`   | `deduplicateUrls`  | `Rating`      | Select property preserved when deduplicating.          |
| `includeEmpty`     | both               | `false`       | Group entries that have an empty URL together too.     |
| `dryRun`           | `deduplicateUrls`  | `false`       | Preview changes without modifying or archiving.        |

## Development

```shell
# Install the Notion CLI (if you don't have it)
curl -fsSL https://ntn.dev | bash

npm install
npm run check      # type-check
ntn workers deploy # deploy to Notion's runtime
```

The Notion API client is provided to each tool's `execute` function as
`context.notion`, so no `NOTION_TOKEN` needs to be configured manually.
