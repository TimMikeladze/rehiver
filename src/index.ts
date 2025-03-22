import fs from "node:fs/promises";
import path from "node:path";
import {
	ListObjectsV2Command,
	type ListObjectsV2CommandInput,
	S3Client,
} from "@aws-sdk/client-s3";
import micromatch from "micromatch";
import pLimit from "p-limit";

// Helper function to check if a file exists
const pathExists = async (filePath: string): Promise<boolean> => {
	try {
		await fs.access(filePath);
		return true;
	} catch (e) {
		return false;
	}
};

/**
 * A utility class for efficient path lookups using micromatch
 */
export class PathMatcher {
	// Cache for compiled patterns to avoid re-compilation
	private readonly patternCache: Map<string, RegExp> = new Map();

	/**
	 * Creates a new PathMatcher instance
	 * @param options Default options to use for all matches
	 */
	constructor(private readonly options: micromatch.Options = {}) {}

	/**
	 * Check if a path matches a glob pattern
	 * @param path Path to check
	 * @param pattern Glob pattern to match against
	 * @param options Additional options to override defaults
	 * @returns True if the path matches the pattern
	 */
	isMatch(
		path: string,
		pattern: string | string[],
		options?: micromatch.Options,
	): boolean {
		return micromatch.isMatch(path, pattern, { ...this.options, ...options });
	}

	/**
	 * Filter an array of paths based on one or more glob patterns
	 * @param paths Array of paths to filter
	 * @param patterns One or more glob patterns
	 * @param options Additional options to override defaults
	 * @returns Filtered array of paths that match the patterns
	 */
	match(
		paths: string[],
		patterns: string | string[],
		options?: micromatch.Options,
	): string[] {
		return micromatch(paths, patterns, { ...this.options, ...options });
	}

	/**
	 * Create and cache a regular expression for a glob pattern
	 * @param pattern Glob pattern to convert to regex
	 * @param options Additional options to override defaults
	 * @returns Regular expression for the pattern
	 */
	getRegex(pattern: string, options?: micromatch.Options): RegExp {
		const cacheKey = `${pattern}:${JSON.stringify({ ...this.options, ...options })}`;

		if (!this.patternCache.has(cacheKey)) {
			const regex = micromatch.makeRe(pattern, { ...this.options, ...options });
			this.patternCache.set(cacheKey, regex);
		}

		const cachedRegex = this.patternCache.get(cacheKey);
		if (!cachedRegex) {
			throw new Error(
				`Failed to create or retrieve regex for pattern: ${pattern}`,
			);
		}

		return cachedRegex;
	}

	/**
	 * Filter paths using pre-compiled regex patterns for maximum performance
	 * @param paths Array of paths to filter
	 * @param patterns One or more glob patterns
	 * @param options Additional options to override defaults
	 * @returns Filtered array of paths that match the patterns
	 */
	matchFast(
		paths: string[],
		patterns: string | string[],
		options?: micromatch.Options,
	): string[] {
		const patternArray = Array.isArray(patterns) ? patterns : [patterns];
		const regexes = patternArray.map((pattern) =>
			this.getRegex(pattern, options),
		);

		return paths.filter((path) => regexes.some((regex) => regex.test(path)));
	}

	/**
	 * Filter paths that don't match any of the provided patterns
	 * @param paths Array of paths to filter
	 * @param patterns One or more glob patterns to exclude
	 * @param options Additional options to override defaults
	 * @returns Filtered array of paths that don't match any pattern
	 */
	not(
		paths: string[],
		patterns: string | string[],
		options?: micromatch.Options,
	): string[] {
		return micromatch.not(paths, patterns, { ...this.options, ...options });
	}

	/**
	 * Check if all of the provided patterns match the path
	 * @param path Path to check
	 * @param patterns One or more glob patterns
	 * @param options Additional options to override defaults
	 * @returns True if all patterns match the path
	 */
	all(
		path: string,
		patterns: string | string[],
		options?: micromatch.Options,
	): boolean {
		return micromatch.all(path, patterns, { ...this.options, ...options });
	}

	/**
	 * Capture values from a path based on a glob pattern
	 * @param pattern Glob pattern with capture groups
	 * @param path Path to extract values from
	 * @param options Additional options to override defaults
	 * @returns Array of captured values or null if no match
	 */
	capture(
		pattern: string,
		path: string,
		options?: micromatch.Options,
	): string[] | null {
		return micromatch.capture(pattern, path, { ...this.options, ...options });
	}
}

/**
 * Configuration interface for S3 client
 */
export interface S3ClientConfig {
	region?: string;
	endpoint?: string;
	credentials?: {
		accessKeyId: string;
		secretAccessKey: string;
		sessionToken?: string;
	};
	maxRetries?: number;
	requestTimeout?: number;
	connectTimeout?: number;
}

/**
 * A utility class for working with S3 paths and glob patterns
 */
export class S3PathMatcher extends PathMatcher {
	private readonly s3Client: S3Client;

	/**
	 * Creates a new S3PathMatcher instance
	 * @param options Default micromatch options
	 * @param s3Options S3 client options
	 */
	constructor(
		options: micromatch.Options = {},
		s3Options: { region?: string } = {},
	) {
		super(options);
		this.s3Client = new S3Client({
			region: s3Options.region,
			requestHandler: {
				httpOptions: {
					connectTimeout: 5000, // 5 seconds connect timeout
					timeout: 120000, // 2 minutes timeout for long operations
				},
			},
		});
	}

	/**
	 * Lists all objects in an S3 bucket with improved performance for handling large datasets
	 * @param bucketName The name of the S3 bucket
	 * @param prefix Optional prefix to filter objects (e.g., folder path)
	 * @param options Optional configuration
	 * @returns Promise resolving to an array of object paths
	 */
	async listObjects(
		bucketName: string,
		prefix = "",
		options: {
			maxConcurrentRequests?: number;
			maxKeysPerRequest?: number;
			abortSignal?: AbortSignal;
		} = {},
	): Promise<string[]> {
		const {
			maxConcurrentRequests = 5,
			maxKeysPerRequest = 1000,
			abortSignal,
		} = options;

		// Create a limiter for concurrent requests
		const limiter = pLimit(maxConcurrentRequests);
		const objectPaths: string[] = [];

		// Parameters for listing objects
		const params: ListObjectsV2CommandInput = {
			// biome-ignore lint/style/useNamingConvention: <explanation>
			Bucket: bucketName,
			// biome-ignore lint/style/useNamingConvention: <explanation>
			Prefix: prefix,
			// biome-ignore lint/style/useNamingConvention: <explanation>
			MaxKeys: maxKeysPerRequest,
		};

		// Process a single page of results
		async function processPage(
			client: S3Client,
			pageParams: ListObjectsV2CommandInput,
		): Promise<{
			objects: string[];
			nextToken?: string;
		}> {
			// Check if operation has been aborted
			if (abortSignal?.aborted) {
				throw new Error("Operation aborted");
			}

			const response = await client.send(new ListObjectsV2Command(pageParams));

			const objects: string[] = [];
			for (const item of response.Contents ?? []) {
				if (item.Key) {
					objects.push(item.Key);
				}
			}

			return {
				objects,
				nextToken: response.NextContinuationToken,
			};
		}

		// Start with initial request (no token)
		let currentTokens: (string | undefined)[] = [undefined];

		// Continue processing until no more tokens
		while (currentTokens.length > 0 && !abortSignal?.aborted) {
			const processPromises = currentTokens.map((token) => {
				const tokenParams = { ...params };
				if (token) {
					tokenParams.ContinuationToken = token;
				}

				// Use the limiter to control concurrency
				return limiter(() => processPage(this.s3Client, tokenParams));
			});

			try {
				const results = await Promise.all(processPromises);

				// Reset tokens for the next batch
				currentTokens = [];

				// Process results
				for (const result of results) {
					// Add paths to our collection
					objectPaths.push(...result.objects);

					// Queue up next token if available
					if (result.nextToken) {
						currentTokens.push(result.nextToken);
					}
				}
			} catch (error) {
				if (abortSignal?.aborted) {
					throw new Error("Operation aborted");
				}
				throw error;
			}
		}

		return objectPaths;
	}

	/**
	 * List and filter S3 objects in one operation
	 * @param bucketName The name of the S3 bucket
	 * @param patterns Glob pattern(s) to match against object keys
	 * @param options S3 listing options and micromatch options
	 * @returns Promise resolving to filtered array of object keys
	 */
	async findMatchingObjects(
		bucketName: string,
		patterns: string | string[],
		options: {
			prefix?: string;
			maxConcurrentRequests?: number;
			maxKeysPerRequest?: number;
			matchOptions?: micromatch.Options;
			useNegation?: boolean;
			abortSignal?: AbortSignal;
			onProgress?: (stats: {
				processed: number;
				total: number;
				matched: number;
				skippedExisting?: number;
			}) => void;
			concurrency?: {
				requestLimit?: number; // S3 API calls limit
				processingLimit?: number; // Pattern matching limit
			};
			localCache?: {
				enabled: boolean;
				basePath: string;
				skipExisting?: boolean;
			};
		} = {},
	): Promise<string[]> {
		const {
			prefix = "",
			maxConcurrentRequests = 5,
			maxKeysPerRequest = 1000,
			matchOptions,
			useNegation = false,
			abortSignal,
			onProgress,
			concurrency = {},
			localCache,
		} = options;

		const { requestLimit = maxConcurrentRequests, processingLimit = 20 } =
			concurrency;

		// Create limiters for both types of operations
		const requestLimiter = pLimit(requestLimit);
		const processingLimiter = pLimit(processingLimit);

		// Precompile patterns for maximum performance
		const patternArray = Array.isArray(patterns) ? patterns : [patterns];
		const regexes = patternArray.map((pattern) =>
			this.getRegex(pattern, matchOptions),
		);

		// Function to check if a path matches the pattern
		const pathMatchesFn = useNegation
			? (path: string): boolean => !regexes.some((regex) => regex.test(path))
			: (path: string): boolean => regexes.some((regex) => regex.test(path));

		// List and filter objects in streaming fashion
		const matchingPaths: string[] = [];
		let processed = 0;
		let total = 0;
		let skippedExisting = 0;

		// Parameters for listing objects
		const params: ListObjectsV2CommandInput = {
			// biome-ignore lint/style/useNamingConvention: <explanation>
			Bucket: bucketName,
			// biome-ignore lint/style/useNamingConvention: <explanation>
			Prefix: prefix,
			// biome-ignore lint/style/useNamingConvention: <explanation>
			MaxKeys: maxKeysPerRequest,
		};

		// Start with initial request (no token)
		let currentTokens: (string | undefined)[] = [undefined];

		// Continue processing until no more tokens or abort
		while (currentTokens.length > 0 && !abortSignal?.aborted) {
			// Create a batch of limited API requests
			const batchPromises = currentTokens.map((token) => {
				return requestLimiter(async () => {
					// Check for abort
					if (abortSignal?.aborted) {
						throw new Error("Operation aborted");
					}

					const tokenParams = { ...params };
					if (token) {
						tokenParams.ContinuationToken = token;
					}

					const response = await this.s3Client.send(
						new ListObjectsV2Command(tokenParams),
					);

					return {
						contents: response.Contents ?? [],
						nextToken: response.NextContinuationToken,
					};
				});
			});

			try {
				const batchResults = await Promise.all(batchPromises);

				// Reset tokens for next batch
				currentTokens = [];

				// Process paths and collect next tokens
				for (const result of batchResults) {
					// Queue up next token if available
					if (result.nextToken) {
						currentTokens.push(result.nextToken);
					}

					// Update total for progress reporting
					total =
						processed +
						result.contents.length +
						currentTokens.length * maxKeysPerRequest; // Estimate

					// Process contents with controlled concurrency
					const processingPromises = result.contents.map((item) => {
						return processingLimiter(async () => {
							processed++;

							if (!item.Key || !pathMatchesFn(item.Key)) {
								return null; // Not matching pattern
							}

							// Check if we should skip existing files
							if (localCache?.enabled && localCache.skipExisting) {
								const localFilePath = path.join(localCache.basePath, item.Key);
								const exists = await pathExists(localFilePath);

								if (exists) {
									skippedExisting++;
									return null; // Skip - file exists
								}
							}

							return item.Key; // Return matching path
						});
					});

					// Collect paths that passed all checks
					const processedResults = await Promise.all(processingPromises);
					const validPaths = processedResults.filter(
						(filePath): filePath is string => filePath !== null,
					);
					matchingPaths.push(...validPaths);

					// Report progress if callback provided
					if (onProgress) {
						onProgress({
							processed,
							total,
							matched: matchingPaths.length,
							skippedExisting,
						});
					}
				}
			} catch (error) {
				if (abortSignal?.aborted) {
					return matchingPaths;
				}
				throw error;
			}
		}

		return matchingPaths;
	}

	/**
	 * Stream matching S3 objects for memory-efficient processing
	 * @param bucketName The name of the S3 bucket
	 * @param patterns Glob pattern(s) to match against object keys
	 * @param processor Function to process each matching path
	 * @param options S3 listing options and micromatch options
	 * @returns Promise resolving when all processing is complete with statistics
	 */
	async streamMatchingObjects(
		bucketName: string,
		patterns: string | string[],
		processor: (path: string) => Promise<void>,
		options: {
			prefix?: string;
			batchSize?: number;
			maxConcurrentRequests?: number; // Controls S3 API calls concurrency
			maxKeysPerRequest?: number;
			matchOptions?: micromatch.Options;
			maxConcurrentProcessing?: number; // Controls file processing concurrency
			abortSignal?: AbortSignal;
			onProgress?: (stats: {
				processed: number;
				total: number;
				matched: number;
				skippedExisting?: number;
			}) => void;
			localCache?: {
				enabled: boolean;
				basePath: string;
				skipExisting?: boolean;
			};
		} = {},
	): Promise<{
		processed: number;
		matched: number;
		skipped: number;
		skippedExisting: number;
	}> {
		const {
			prefix = "",
			batchSize = 1000,
			maxConcurrentRequests = 5,
			maxKeysPerRequest = 1000,
			matchOptions,
			maxConcurrentProcessing = 10,
			abortSignal,
			onProgress,
			localCache,
		} = options;

		// Create limiters for both S3 API calls and file processing
		const requestLimiter = pLimit(maxConcurrentRequests);
		const processingLimiter = pLimit(maxConcurrentProcessing);

		// Precompile patterns for faster matching
		const patternArray = Array.isArray(patterns) ? patterns : [patterns];
		const regexes = patternArray.map((pattern) =>
			this.getRegex(pattern, matchOptions),
		);

		// Function to check if a path matches any pattern
		const pathMatches = (path: string): boolean =>
			regexes.some((regex) => regex.test(path));

		// Parameters for listing objects
		const params: ListObjectsV2CommandInput = {
			// biome-ignore lint/style/useNamingConvention: <explanation>
			Bucket: bucketName,
			// biome-ignore lint/style/useNamingConvention: <explanation>
			Prefix: prefix,
			// biome-ignore lint/style/useNamingConvention: <explanation>
			MaxKeys: maxKeysPerRequest,
		};

		// Stats tracking
		let totalProcessed = 0;
		let totalMatched = 0;
		let totalSkipped = 0;
		let totalSkippedExisting = 0;

		// Start with initial request (no token)
		let currentTokens: (string | undefined)[] = [undefined];

		// Continue processing until no more tokens or abort
		while (currentTokens.length > 0 && !abortSignal?.aborted) {
			// Create a batch of limited API requests
			const batchPromises = currentTokens.map((token) => {
				return requestLimiter(async () => {
					// Check for abort
					if (abortSignal?.aborted) {
						throw new Error("Operation aborted");
					}

					const tokenParams = { ...params };
					if (token) {
						tokenParams.ContinuationToken = token;
					}

					const response = await this.s3Client.send(
						new ListObjectsV2Command(tokenParams),
					);

					// Process contents to find matching paths
					const contents = response.Contents ?? [];
					const matchingPaths: string[] = [];

					// Process all items to check pattern match and local existence
					for (const item of contents) {
						totalProcessed++;

						if (!item.Key) continue;

						// Check if it matches the pattern
						if (!pathMatches(item.Key)) {
							totalSkipped++;
							continue;
						}

						// Check if the file exists locally and should be skipped
						if (localCache?.enabled && localCache.skipExisting) {
							const localPath = path.join(localCache.basePath, item.Key);
							const exists = await pathExists(localPath);

							if (exists) {
								totalSkippedExisting++;
								continue;
							}
						}

						// File matches and doesn't exist locally (or we don't care)
						matchingPaths.push(item.Key);
						totalMatched++;
					}

					return {
						nextToken: response.NextContinuationToken,
						matchingPaths,
					};
				});
			});

			try {
				const batchResults = await Promise.all(batchPromises);

				// Reset tokens for next batch
				currentTokens = [];

				// Process matching files and collect next tokens
				for (const result of batchResults) {
					// Queue up next token if available
					if (result.nextToken) {
						currentTokens.push(result.nextToken);
					}

					if (result.matchingPaths.length > 0) {
						// Process in chunks with the processing limiter
						for (let i = 0; i < result.matchingPaths.length; i += batchSize) {
							// Check for abort before processing batch
							if (abortSignal?.aborted) {
								throw new Error("Operation aborted");
							}

							const pathBatch = result.matchingPaths.slice(i, i + batchSize);

							// Process paths with controlled concurrency
							await Promise.all(
								pathBatch.map((path) =>
									processingLimiter(() => processor(path)),
								),
							);
						}
					}

					// Report progress if callback provided
					if (onProgress) {
						onProgress({
							processed: totalProcessed,
							total: totalProcessed + currentTokens.length * maxKeysPerRequest,
							matched: totalMatched,
							skippedExisting: totalSkippedExisting,
						});
					}
				}
			} catch (error) {
				if (abortSignal?.aborted) {
					return {
						processed: totalProcessed,
						matched: totalMatched,
						skipped: totalSkipped,
						skippedExisting: totalSkippedExisting,
					};
				}
				throw error;
			}
		}

		return {
			processed: totalProcessed,
			matched: totalMatched,
			skipped: totalSkipped,
			skippedExisting: totalSkippedExisting,
		};
	}

	/**
	 * Group S3 objects by capture patterns
	 * @param bucketName The name of the S3 bucket
	 * @param capturePattern Glob pattern with capture groups
	 * @param options S3 listing options and micromatch options
	 * @returns Promise resolving to an object with groups of paths
	 */
	async groupObjectsByCapture(
		bucketName: string,
		capturePattern: string,
		options: {
			prefix?: string;
			maxConcurrentRequests?: number;
			maxKeysPerRequest?: number;
			matchOptions?: micromatch.Options;
			abortSignal?: AbortSignal;
			groupKeyFn?: (captures: string[]) => string;
			localCache?: {
				enabled: boolean;
				basePath: string;
				skipExisting?: boolean;
			};
		} = {},
	): Promise<Record<string, string[]>> {
		const {
			prefix = "",
			maxConcurrentRequests,
			maxKeysPerRequest,
			matchOptions,
			abortSignal,
			groupKeyFn = (captures) => captures[0], // Default to first capture
			localCache,
		} = options;

		// List all objects with the given prefix
		const allPaths = await this.listObjects(bucketName, prefix, {
			maxConcurrentRequests,
			maxKeysPerRequest,
			abortSignal,
		});

		// Group objects by the capture pattern using p-limit for potential regex work
		const groups: Record<string, string[]> = {};
		const limiter = pLimit(Math.max(1, maxConcurrentRequests || 5));

		// Process in batches to avoid memory issues with very large datasets
		const batchSize = 1000;
		const batches = [];

		for (let i = 0; i < allPaths.length; i += batchSize) {
			if (abortSignal?.aborted) {
				break;
			}

			batches.push(allPaths.slice(i, i + batchSize));
		}

		for (const batch of batches) {
			if (abortSignal?.aborted) {
				break;
			}

			// Process each batch concurrently
			await Promise.all(
				batch.map((filePath) =>
					limiter(async () => {
						if (abortSignal?.aborted) {
							return;
						}

						// Skip if file exists locally and skipExisting is enabled
						if (localCache?.enabled && localCache.skipExisting) {
							const localPath = path.join(localCache.basePath, filePath);
							const exists = await pathExists(localPath);

							if (exists) {
								return;
							}
						}

						const captured = this.capture(
							capturePattern,
							filePath,
							matchOptions,
						);

						if (captured) {
							// Use the provided function to determine the group key
							const key = groupKeyFn(captured);

							if (!groups[key]) {
								groups[key] = [];
							}

							groups[key].push(filePath);
						}
					}),
				),
			);
		}

		return groups;
	}
}

// Example usage
async function example() {
	// Create an S3 path matcher with options
	const matcher = new S3PathMatcher(
		{
			dot: true, // Match dotfiles
			nocase: false, // Case sensitive matching
		},
		{ region: "us-west-2" },
	);

	try {
		// Create an abort controller for cancellable operations
		const controller = new AbortController();
		const signal = controller.signal;

		// Set a timeout to abort after 5 minutes
		const timeout = setTimeout(() => controller.abort(), 5 * 60 * 1000);

		// List and filter objects with p-limit for concurrency control
		const matchingPaths = await matcher.findMatchingObjects(
			"my-bucket",
			["*.json", "*.csv"], // Match JSON and CSV files
			{
				prefix: "data/", // Only look in the data/ prefix
				abortSignal: signal,
				onProgress: (stats) => {
					console.log(
						`Progress: ${stats.processed}/${stats.total} (${stats.matched} matches)`,
					);
				},
				concurrency: {
					requestLimit: 10, // Limit to 10 concurrent S3 API calls
					processingLimit: 50, // Limit to 50 concurrent pattern matches
				},
				// Example of the new option to skip existing files
				localCache: {
					enabled: true,
					basePath: "/path/to/local/cache",
					skipExisting: true, // Skip files that already exist locally
				},
			},
		);
	} catch (error) {
		console.error("Error:", error);
	}
}
