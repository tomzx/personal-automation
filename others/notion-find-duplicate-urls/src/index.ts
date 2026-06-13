import { Worker } from "@notionhq/workers";
import { j } from "@notionhq/workers/schema-builder";

const worker = new Worker();
export default worker;

/**
 * Find (and optionally remove) duplicate URLs in a Notion data source.
 *
 * This is a Notion Worker port of the former `notion-find-duplicate-urls.py`
 * CLI script. Instead of being run from a terminal, it exposes two tools that a
 * Notion custom agent can call:
 *
 *   - findDuplicateUrls: scan a data source and report entries sharing a URL.
 *   - deduplicateUrls:   keep the oldest entry per URL, copy over the most
 *                        recent read time / rating, and archive the duplicates.
 *
 * The Notion API client is provided to each tool's `execute` as `context.notion`.
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface PageData {
	id: string;
	title: string;
	url: string;
	createdTime: string | null;
	readTime?: string | null;
	rating?: string | null;
}

type Duplicates = Map<string, PageData[]>;

interface ResolvedProperties {
	urlProperty: string;
	readTimeProperty: string | null;
	ratingProperty: string | null;
}

// ---------------------------------------------------------------------------
// Property helpers (mirror the original Python helpers)
// ---------------------------------------------------------------------------

/** Find a property name in a case-insensitive manner. */
function findPropertyName(
	properties: Record<string, unknown>,
	targetName: string,
): string | null {
	const targetLower = targetName.toLowerCase();
	for (const propName of Object.keys(properties)) {
		if (propName.toLowerCase() === targetLower) {
			return propName;
		}
	}
	return null;
}

/** Extract the URL from a page's properties. */
function getUrlFromPage(
	properties: Record<string, any>,
	propertyName: string,
): string | null {
	const prop = properties[propertyName];
	if (prop && prop.type === "url") {
		return prop.url ?? null;
	}
	return null;
}

/** Extract the title from a page's properties, or "Untitled". */
function getPageTitle(properties: Record<string, any>): string {
	for (const prop of Object.values(properties)) {
		if (prop && prop.type === "title") {
			const titleArray = prop.title ?? [];
			if (titleArray.length > 0) {
				return titleArray[0].plain_text ?? "Untitled";
			}
		}
	}
	return "Untitled";
}

/** Extract the read time (date) start value from a page's properties. */
function getReadTime(
	properties: Record<string, any>,
	propertyName: string,
): string | null {
	const prop = properties[propertyName];
	if (prop && prop.type === "date" && prop.date) {
		return prop.date.start ?? null;
	}
	return null;
}

/** Extract the rating (select) value from a page's properties. */
function getRating(
	properties: Record<string, any>,
	propertyName: string,
): string | null {
	const prop = properties[propertyName];
	if (prop && prop.type === "select" && prop.select) {
		return prop.select.name ?? null;
	}
	return null;
}

// ---------------------------------------------------------------------------
// Notion API helpers
// ---------------------------------------------------------------------------

/**
 * Resolve the actual (case-insensitive) property names from the data source
 * schema. Throws if the URL property cannot be found.
 */
async function resolveProperties(
	notion: any,
	dataSourceId: string,
	urlProperty: string,
	readTimeProperty: string | null,
	ratingProperty: string | null,
): Promise<ResolvedProperties> {
	const dataSource = await notion.dataSources.retrieve({
		data_source_id: dataSourceId,
	});
	const dsProperties: Record<string, any> = dataSource.properties ?? {};

	const actualUrl = findPropertyName(dsProperties, urlProperty);
	if (!actualUrl) {
		const available = Object.entries(dsProperties)
			.map(([name, def]: [string, any]) => `  - ${name} (type: ${def.type ?? "unknown"})`)
			.join("\n");
		throw new Error(
			`Property '${urlProperty}' not found in data source.\n\nAvailable properties:\n${available}`,
		);
	}

	const urlType = dsProperties[actualUrl]?.type;
	if (urlType !== "url") {
		console.warn(`Warning: Property '${actualUrl}' is type '${urlType}', not 'url'`);
	}

	let actualReadTime: string | null = null;
	if (readTimeProperty) {
		actualReadTime = findPropertyName(dsProperties, readTimeProperty);
		if (!actualReadTime) {
			console.warn(`Warning: Read time property '${readTimeProperty}' not found`);
		}
	}

	let actualRating: string | null = null;
	if (ratingProperty) {
		actualRating = findPropertyName(dsProperties, ratingProperty);
		if (!actualRating) {
			console.warn(`Warning: Rating property '${ratingProperty}' not found`);
		}
	}

	return {
		urlProperty: actualUrl,
		readTimeProperty: actualReadTime,
		ratingProperty: actualRating,
	};
}

interface ScanOptions {
	dataSourceId: string;
	urlProperty?: string | null;
	readTimeProperty?: string | null;
	ratingProperty?: string | null;
	includeEmpty?: boolean | null;
}

/** Query every page in the data source and group them by URL. */
async function findDuplicates(
	notion: any,
	options: ScanOptions,
): Promise<{ duplicates: Duplicates; resolved: ResolvedProperties; scanned: number }> {
	const resolved = await resolveProperties(
		notion,
		options.dataSourceId,
		options.urlProperty ?? "URL",
		options.readTimeProperty ?? null,
		options.ratingProperty ?? null,
	);

	const urlToPages: Duplicates = new Map();
	let hasMore = true;
	let startCursor: string | undefined = undefined;
	let scanned = 0;

	while (hasMore) {
		const response: any = await notion.dataSources.query({
			data_source_id: options.dataSourceId,
			...(startCursor ? { start_cursor: startCursor } : {}),
		});

		const results: any[] = response.results ?? [];

		for (const page of results) {
			const properties: Record<string, any> = page.properties ?? {};

			let url = getUrlFromPage(properties, resolved.urlProperty);
			if (!url) {
				if (options.includeEmpty) {
					url = "";
				} else {
					continue;
				}
			}

			const pageData: PageData = {
				id: page.id,
				title: getPageTitle(properties),
				url,
				createdTime: page.created_time ?? null,
			};

			if (resolved.readTimeProperty) {
				pageData.readTime = getReadTime(properties, resolved.readTimeProperty);
			}
			if (resolved.ratingProperty) {
				pageData.rating = getRating(properties, resolved.ratingProperty);
			}

			const existing = urlToPages.get(url);
			if (existing) {
				existing.push(pageData);
			} else {
				urlToPages.set(url, [pageData]);
			}

			scanned += 1;
		}

		hasMore = response.has_more ?? false;
		startCursor = response.next_cursor ?? undefined;
	}

	// Keep only URLs that appear more than once.
	const duplicates: Duplicates = new Map();
	for (const [url, pages] of urlToPages) {
		if (pages.length > 1) {
			duplicates.set(url, pages);
		}
	}

	return { duplicates, resolved, scanned };
}

/** Update a page's read time (date) property. */
async function updateReadTime(
	notion: any,
	pageId: string,
	dateValue: string,
	propertyName: string,
): Promise<void> {
	await notion.pages.update({
		page_id: pageId,
		properties: {
			[propertyName]: { date: { start: dateValue } },
		},
	});
}

/** Update a page's rating (select) property. */
async function updateRating(
	notion: any,
	pageId: string,
	ratingValue: string,
	propertyName: string,
): Promise<void> {
	await notion.pages.update({
		page_id: pageId,
		properties: {
			[propertyName]: { select: { name: ratingValue } },
		},
	});
}

/** Archive (soft-delete) a page. */
async function archivePage(notion: any, pageId: string): Promise<void> {
	await notion.pages.update({ page_id: pageId, archived: true });
}

interface DedupReportEntry {
	url: string;
	kept: { id: string; title: string; createdTime: string | null };
	copiedReadTime?: string;
	copiedRating?: string;
	deleted: { id: string; title: string }[];
}

/**
 * Deduplicate URLs: keep the oldest entry, copy over the most recent read time
 * and a rating, then archive the remaining duplicates.
 */
async function deduplicate(
	notion: any,
	duplicates: Duplicates,
	resolved: ResolvedProperties,
	dryRun: boolean,
): Promise<{ report: DedupReportEntry[]; deletedCount: number }> {
	const report: DedupReportEntry[] = [];
	let deletedCount = 0;

	for (const [url, pages] of duplicates) {
		// Oldest first.
		const sorted = [...pages].sort((a, b) =>
			(a.createdTime ?? "").localeCompare(b.createdTime ?? ""),
		);
		const oldest = sorted[0];
		const toDelete = sorted.slice(1);

		// Most recent non-empty read time.
		let latestReadTime: string | null = null;
		if (resolved.readTimeProperty) {
			for (let i = sorted.length - 1; i >= 0; i--) {
				if (sorted[i].readTime) {
					latestReadTime = sorted[i].readTime ?? null;
					break;
				}
			}
		}

		// Any rating value.
		let ratingValue: string | null = null;
		if (resolved.ratingProperty) {
			for (let i = sorted.length - 1; i >= 0; i--) {
				if (sorted[i].rating) {
					ratingValue = sorted[i].rating ?? null;
					break;
				}
			}
		}

		const entry: DedupReportEntry = {
			url,
			kept: { id: oldest.id, title: oldest.title, createdTime: oldest.createdTime },
			deleted: toDelete.map((p) => ({ id: p.id, title: p.title })),
		};

		const needsReadTime =
			resolved.readTimeProperty !== null &&
			latestReadTime !== null &&
			oldest.readTime !== latestReadTime;
		const needsRating =
			resolved.ratingProperty !== null &&
			ratingValue !== null &&
			oldest.rating !== ratingValue;

		if (needsReadTime) entry.copiedReadTime = latestReadTime!;
		if (needsRating) entry.copiedRating = ratingValue!;

		if (!dryRun) {
			if (needsReadTime) {
				await updateReadTime(notion, oldest.id, latestReadTime!, resolved.readTimeProperty!);
			}
			if (needsRating) {
				await updateRating(notion, oldest.id, ratingValue!, resolved.ratingProperty!);
			}
			for (const page of toDelete) {
				await archivePage(notion, page.id);
			}
		}

		deletedCount += toDelete.length;
		report.push(entry);
	}

	return { report, deletedCount };
}

// ---------------------------------------------------------------------------
// Worker tools
// ---------------------------------------------------------------------------

worker.tool("findDuplicateUrls", {
	title: "Find Duplicate URLs",
	description:
		"Scan a Notion data source and report entries that share the same URL. Does not modify anything.",
	hints: { readOnlyHint: true },
	schema: j.object({
		dataSourceId: j.string().describe("The ID of the Notion data source (database) to scan."),
		urlProperty: j
			.string()
			.nullable()
			.describe("Name of the URL property (case-insensitive). Defaults to 'URL'."),
		includeEmpty: j
			.boolean()
			.nullable()
			.describe("Treat entries with an empty URL as a group too. Defaults to false."),
	}),
	execute: async (input, { notion }: { notion: any }) => {
		const { duplicates, scanned } = await findDuplicates(notion, {
			dataSourceId: input.dataSourceId,
			urlProperty: input.urlProperty,
			includeEmpty: input.includeEmpty,
		});

		const groups = [...duplicates.entries()]
			.sort((a, b) => b[1].length - a[1].length)
			.map(([url, pages]) => ({
				url: url || "(empty)",
				count: pages.length,
				pages: pages.map((p) => ({ id: p.id, title: p.title })),
			}));

		return {
			scanned,
			duplicateUrlCount: groups.length,
			duplicates: groups,
		};
	},
});

worker.tool("deduplicateUrls", {
	title: "Deduplicate URLs",
	description:
		"Scan a Notion data source for duplicate URLs, keep the oldest entry per URL (copying over the most recent read time and rating), and archive the remaining duplicates. Use dryRun to preview without making changes.",
	schema: j.object({
		dataSourceId: j.string().describe("The ID of the Notion data source (database) to deduplicate."),
		urlProperty: j
			.string()
			.nullable()
			.describe("Name of the URL property (case-insensitive). Defaults to 'URL'."),
		readTimeProperty: j
			.string()
			.nullable()
			.describe(
				"Name of the read time (date) property to preserve when deduplicating. Defaults to 'Read time'.",
			),
		ratingProperty: j
			.string()
			.nullable()
			.describe(
				"Name of the rating (select) property to preserve when deduplicating. Defaults to 'Rating'.",
			),
		includeEmpty: j
			.boolean()
			.nullable()
			.describe("Treat entries with an empty URL as a group too. Defaults to false."),
		dryRun: j
			.boolean()
			.nullable()
			.describe("Preview the changes without modifying or archiving anything. Defaults to false."),
	}),
	execute: async (input, { notion }: { notion: any }) => {
		const { duplicates, resolved, scanned } = await findDuplicates(notion, {
			dataSourceId: input.dataSourceId,
			urlProperty: input.urlProperty,
			readTimeProperty: input.readTimeProperty ?? "Read time",
			ratingProperty: input.ratingProperty ?? "Rating",
			includeEmpty: input.includeEmpty,
		});

		const dryRun = input.dryRun ?? false;
		const { report, deletedCount } = await deduplicate(notion, duplicates, resolved, dryRun);

		return {
			scanned,
			duplicateUrlCount: duplicates.size,
			deletedCount,
			dryRun,
			details: report as any,
		};
	},
});
