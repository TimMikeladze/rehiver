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
 * Interface for object metadata
 */
export interface ObjectMetadata {
	key: string;
	size: number;
	etag: string;
	lastModified: Date;
}

/**
 * S3 object interface matching AWS SDK structure with camelCase properties
 */
export interface S3Object {
	key?: string;
	size?: number;
	etag?: string;
	lastModified?: Date;
}

/**
 * Change types for tracking object modifications
 */
export enum ChangeType {
	Added = "added",
	Modified = "modified",
	Deleted = "deleted",
	Unchanged = "unchanged",
}

/**
 * Result of a change detection comparison
 */
export interface ChangeResult {
	changeType: ChangeType;
	object: ObjectMetadata;
	previousVersion?: ObjectMetadata;
}

/**
 * Options for change detection
 */
export interface ChangeDetectionOptions {
	stateFilePath?: string;
	compareMode?: "quick" | "full";
	ignoreEtagOnSize?: boolean;
	trackDeleted?: boolean;
}

/**
 * Engine for detecting changes in S3 objects between runs
 */
export class ChangeDetectionEngine {
	private previousState: Map<string, ObjectMetadata> = new Map();
	private currentState: Map<string, ObjectMetadata> = new Map();
	private options: ChangeDetectionOptions;

	/**
	 * Creates a new ChangeDetectionEngine
	 * @param options Engine configuration options
	 */
	constructor(options: ChangeDetectionOptions = {}) {
		this.options = {
			stateFilePath: "change-detection-state.json",
			compareMode: "full",
			ignoreEtagOnSize: false,
			trackDeleted: true,
			...options,
		};
	}

	/**
	 * Loads the previous state from a JSON file or creates a new state if none exists
	 */
	async loadPreviousState(): Promise<void> {
		if (!this.options.stateFilePath) {
			return;
		}

		try {
			const stateData = await fs.readFile(this.options.stateFilePath, "utf-8");
			const parsedState = JSON.parse(stateData);

			// Convert the array back to a Map
			this.previousState = new Map(
				Object.entries(parsedState).map(([key, value]) => {
					const metadata = value as ObjectMetadata;
					// Convert ISO string back to Date object
					if (
						metadata.lastModified &&
						typeof metadata.lastModified === "string"
					) {
						metadata.lastModified = new Date(metadata.lastModified);
					}
					return [key, metadata];
				}),
			);
		} catch (error) {
			// If file doesn't exist or is invalid, start with an empty state
			this.previousState = new Map();
		}
	}

	/**
	 * Saves the current state to a JSON file
	 */
	async saveCurrentState(): Promise<void> {
		if (!this.options.stateFilePath) {
			return;
		}

		// Convert Map to a plain object for serialization
		const stateObject = Object.fromEntries(this.currentState);
		const stateData = JSON.stringify(stateObject, null, 2);

		// Ensure directory exists
		const dir = path.dirname(this.options.stateFilePath);
		await fs.mkdir(dir, { recursive: true });

		// Write state file
		await fs.writeFile(this.options.stateFilePath, stateData, "utf-8");
	}

	/**
	 * Adds an object to the current state
	 * @param object Object metadata to track
	 */
	addObject(object: ObjectMetadata): void {
		this.currentState.set(object.key, object);
	}

	/**
	 * Adds multiple objects to the current state
	 * @param objects Array of object metadata
	 */
	addObjects(objects: ObjectMetadata[]): void {
		for (const object of objects) {
			this.addObject(object);
		}
	}

	/**
	 * Converts an S3 object to our metadata format
	 * @param s3Object S3 object from AWS SDK
	 * @returns Normalized object metadata
	 */
	static fromS3Object(s3Object: S3Object): ObjectMetadata {
		return {
			key: s3Object.key || "",
			size: s3Object.size || 0,
			etag: (s3Object.etag || "").replace(/"/g, ""), // Remove quotes from ETag
			lastModified: s3Object.lastModified || new Date(),
		};
	}

	/**
	 * Determines if an object has changed between states
	 * @param current Current object metadata
	 * @param previous Previous object metadata
	 * @returns True if the object has changed
	 */
	private hasObjectChanged(
		current: ObjectMetadata,
		previous: ObjectMetadata,
	): boolean {
		if (this.options.compareMode === "quick") {
			// Quick comparison - only check size and last modified date
			return (
				current.size !== previous.size ||
				current.lastModified.getTime() !== previous.lastModified.getTime()
			);
		}

		// Full comparison - check ETag (hash) as well
		// If ignoreEtagOnSize is true, skip ETag check when sizes match
		if (this.options.ignoreEtagOnSize && current.size === previous.size) {
			return current.lastModified.getTime() !== previous.lastModified.getTime();
		}

		return (
			current.size !== previous.size ||
			current.etag !== previous.etag ||
			current.lastModified.getTime() !== previous.lastModified.getTime()
		);
	}

	/**
	 * Compares current state with previous state to detect changes
	 * @returns Array of change results
	 */
	detectChanges(): ChangeResult[] {
		const changes: ChangeResult[] = [];

		// Check for new and modified objects
		this.currentState.forEach((currentObject, key) => {
			const previousObject = this.previousState.get(key);

			if (!previousObject) {
				// New object
				changes.push({
					changeType: ChangeType.Added,
					object: currentObject,
				});
			} else if (this.hasObjectChanged(currentObject, previousObject)) {
				// Modified object
				changes.push({
					changeType: ChangeType.Modified,
					object: currentObject,
					previousVersion: previousObject,
				});
			} else {
				// Unchanged object
				changes.push({
					changeType: ChangeType.Unchanged,
					object: currentObject,
					previousVersion: previousObject,
				});
			}
		});

		// Check for deleted objects if enabled
		if (this.options.trackDeleted) {
			this.previousState.forEach((previousObject, key) => {
				if (!this.currentState.has(key)) {
					changes.push({
						changeType: ChangeType.Deleted,
						object: previousObject,
					});
				}
			});
		}

		return changes;
	}

	/**
	 * Filter changes by type
	 * @param changes Array of change results
	 * @param types Types of changes to include
	 * @returns Filtered array of change results
	 */
	static filterChangesByType(
		changes: ChangeResult[],
		types: ChangeType[],
	): ChangeResult[] {
		return changes.filter((change) => types.includes(change.changeType));
	}

	/**
	 * Creates a new state from the current state
	 */
	commitChanges(): void {
		this.previousState = new Map(this.currentState);
	}

	/**
	 * Clears the current state
	 */
	resetCurrentState(): void {
		this.currentState = new Map();
	}

	/**
	 * Clears all state (previous and current)
	 */
	resetAllState(): void {
		this.previousState = new Map();
		this.currentState = new Map();
	}
}

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
		// Implementation that mimics micromatch.capture
		// Parse the pattern and extract capture groups from the path
		const placeholderRegex = /:[^\/\.]+/g;
		const placeholders = pattern.match(placeholderRegex) || [];

		// Replace placeholders with capture groups
		let regexPattern = pattern.replace(/\//g, "\\/");
		regexPattern = regexPattern.replace(/\./g, "\\.");

		for (const placeholder of placeholders) {
			// Replace :name with capturing group
			regexPattern = regexPattern.replace(placeholder, "([^/\\.]+)");
		}

		// Replace asterisks with capturing groups for filenames
		regexPattern = regexPattern.replace(/\*/g, "([^/]+)");

		// Create a regex with the pattern
		const regex = new RegExp(`^${regexPattern}$`, options?.nocase ? "i" : "");
		const match = path.match(regex);

		if (!match) {
			return null;
		}

		// Return the captured values (skip the first element which is the full match)
		return match.slice(1);
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
		s3Options: {
			region?: string;
			endpoint?: string;
			credentials?: { accessKeyId: string; secretAccessKey: string };
			forcePathStyle?: boolean;
		} = {},
	) {
		super(options);
		this.s3Client = new S3Client({
			region: s3Options.region,
			endpoint: s3Options.endpoint,
			credentials: s3Options.credentials,
			forcePathStyle: s3Options.forcePathStyle,
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

		// Check upfront if operation is already aborted
		if (abortSignal?.aborted) {
			throw new Error("Operation aborted");
		}

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
		while (currentTokens.length > 0) {
			// Check if operation has been aborted before starting a new batch
			if (abortSignal?.aborted) {
				throw new Error("Operation aborted");
			}

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
				// Let abort errors propagate
				if (error instanceof Error && error.message === "Operation aborted") {
					throw error;
				}
				// Rethrow other errors
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

							if (!(item.Key && pathMatchesFn(item.Key))) {
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

						if (!item.Key) {
							continue;
						}

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

		// Clear the timeout
		clearTimeout(timeout);
	} catch (error) {
		console.error("Error:", error);
	}
}

// Example of using the ChangeDetectionEngine
async function changeDetectionExample() {
	try {
		// Create a change detection engine with custom options
		const changeEngine = new ChangeDetectionEngine({
			stateFilePath: "./state/s3-changes.json",
			compareMode: "quick", // Use quick comparison (size and lastModified only)
			trackDeleted: true,
		});

		// Load the previous state from disk (if it exists)
		await changeEngine.loadPreviousState();

		// Create an S3 path matcher
		const matcher = new S3PathMatcher({ dot: true }, { region: "us-west-2" });

		// List objects using the S3PathMatcher's listObjects method
		const allPaths = await matcher.listObjects("my-bucket", "data/");

		// Create a client to get full metadata
		const s3Client = new S3Client({ region: "us-west-2" });

		// Create a batch of objects for the change detection engine
		const objects: ObjectMetadata[] = allPaths.map((path) => ({
			key: path,
			size: 0, // These would be populated with real metadata
			etag: "",
			lastModified: new Date(),
		}));

		// In a real implementation, you would fetch the actual size, etag, and lastModified
		// Let's assume we have that information for this example

		// Add all objects to the current state
		changeEngine.addObjects(objects);

		// Detect changes from the previous run
		const changes = changeEngine.detectChanges();

		// Filter for just the new and modified files
		const newFiles = ChangeDetectionEngine.filterChangesByType(changes, [
			ChangeType.Added,
		]);

		const modifiedFiles = ChangeDetectionEngine.filterChangesByType(changes, [
			ChangeType.Modified,
		]);

		const deletedFiles = ChangeDetectionEngine.filterChangesByType(changes, [
			ChangeType.Deleted,
		]);

		// Print change summary
		console.log(`Found ${newFiles.length} new files`);
		console.log(`Found ${modifiedFiles.length} modified files`);
		console.log(`Found ${deletedFiles.length} deleted files`);

		// Define a local directory for downloading files
		const downloadDir = "./downloads";
		await fs.mkdir(downloadDir, { recursive: true });

		// Process only new and modified files
		const filesToProcess = [...newFiles, ...modifiedFiles];

		// Limit concurrent processing
		const processLimiter = pLimit(5);

		// Process files concurrently
		await Promise.all(
			filesToProcess.map((changeResult) =>
				processLimiter(async () => {
					const { key } = changeResult.object;
					console.log(`Processing ${key} (${changeResult.changeType})`);

					// Example: Define local path for this file
					const localPath = path.join(downloadDir, path.basename(key));

					try {
						// Download the file (simulated with a write operation)
						// In a real scenario, you would use getObject from S3Client
						await fs.writeFile(
							localPath,
							`Content for ${key} would be here`,
							"utf-8",
						);

						// Example: Process the file based on its extension
						const extension = path.extname(key).toLowerCase();

						if (extension === ".json") {
							console.log(`Parsing JSON file: ${key}`);
							// JSON processing would go here
						} else if (extension === ".csv") {
							console.log(`Parsing CSV file: ${key}`);
							// CSV processing would go here
						} else {
							console.log(`Skipping unknown file type: ${key}`);
						}

						console.log(`Successfully processed ${key}`);
					} catch (err) {
						console.error(`Error processing ${key}:`, err);
					}
				}),
			),
		);

		// Handle deleted files if needed
		for (const deletedFile of deletedFiles) {
			const { key } = deletedFile.object;
			console.log(`Handling deleted file: ${key}`);

			// Example: Remove local copy if it exists
			const localPath = path.join(downloadDir, path.basename(key));
			try {
				await fs.unlink(localPath);
				console.log(`Removed local copy of deleted file: ${key}`);
			} catch (err) {
				// File might not exist locally, that's fine
			}
		}

		// Save the current state for future comparison
		await changeEngine.saveCurrentState();
		console.log("Change detection workflow completed successfully");
	} catch (error) {
		console.error("Error in change detection:", error);
	}
}

import { z } from "zod";

/**
 * HivePartitionParser - A utility for parsing Hive partition paths and validating them with Zod schemas
 */
export class HivePartitionParser<T extends z.ZodTypeAny> {
	private schema: T;
	private partitionKeys: string[];

	/**
	 * Create a new HivePartitionParser with a Zod schema
	 * @param schema A Zod schema that defines the structure and types of partition keys
	 */
	constructor(schema: T) {
		this.schema = schema;
		// Extract expected keys from the Zod schema
		// @ts-expect-error
		this.partitionKeys = Object.keys(schema.shape || {});
	}

	/**
	 * Parse a Hive partition path and return a typed object
	 * @param path The partition path to parse (e.g., "/table/year=2023/month=12/day=25")
	 * @returns A validated object matching the schema type
	 * @throws If validation fails or required partitions are missing
	 */
	parse(path: string): z.infer<T> {
		// Extract key-value pairs from the path
		const segments = path.split("/").filter((segment) => segment.length > 0);
		const rawPartitions: Record<string, string> = {};

		for (const segment of segments) {
			if (segment.includes("=")) {
				const [key, value] = segment.split("=", 2);
				if (key && value !== undefined) {
					rawPartitions[key] = value;
				}
			}
		}

		// Validate using the Zod schema
		return this.schema.parse(rawPartitions);
	}

	/**
	 * Try to parse a path, returning a result object with success/error information
	 * @param path The partition path to parse
	 * @returns A Zod parse result with either data or error
	 */
	safeParse(path: string): z.SafeParseReturnType<unknown, z.infer<T>> {
		// Extract key-value pairs from the path
		const segments = path.split("/").filter((segment) => segment.length > 0);
		const rawPartitions: Record<string, string> = {};

		for (const segment of segments) {
			if (segment.includes("=")) {
				const [key, value] = segment.split("=", 2);
				if (key && value !== undefined) {
					rawPartitions[key] = value;
				}
			}
		}

		// Validate using the Zod schema
		return this.schema.safeParse(rawPartitions);
	}

	/**
	 * Format a typed object into a Hive partition path
	 * @param data An object matching the schema
	 * @returns A formatted partition path string
	 */
	format(data: z.infer<T>): string {
		const validated = this.schema.parse(data);
		const segments: string[] = [];

		for (const key of this.partitionKeys) {
			const value = validated[key as keyof typeof validated];
			if (value !== undefined) {
				segments.push(`${key}=${value}`);
			}
		}

		return segments.join("/");
	}

	/**
	 * Create a glob pattern from partial partition data
	 * @param partialData Partial data with some partition keys specified
	 * @returns A glob pattern that can match multiple partitions
	 */
	createGlobPattern(partialData: Partial<z.infer<T>>): string {
		const segments: string[] = [];

		for (const key of this.partitionKeys) {
			const keyName = key as keyof typeof partialData;
			if (keyName in partialData && partialData[keyName] !== undefined) {
				segments.push(`${key}=${partialData[keyName]}`);
			} else {
				segments.push(`${key}=*`);
			}
		}

		return segments.join("/");
	}

	/**
	 * Check if a partition path is valid according to the schema
	 * @param path The partition path to validate
	 * @returns True if the path is valid, false otherwise
	 */
	isValid(path: string): boolean {
		const result = this.safeParse(path);
		return result.success;
	}

	/**
	 * Get validation errors for a path
	 * @param path The partition path to validate
	 * @returns Array of error messages or empty array if valid
	 */
	getValidationErrors(path: string): string[] {
		const result = this.safeParse(path);
		if (result.success) {
			return [];
		}
		return result.error.errors.map(
			(err) => `${err.path.join(".")}: ${err.message}`,
		);
	}

	/**
	 * Find missing partition keys in a path
	 * @param path The partition path to check
	 * @returns Array of missing key names
	 */
	getMissingKeys(path: string): string[] {
		const segments = path.split("/").filter((segment) => segment.length > 0);
		const foundKeys = new Set<string>();

		for (const segment of segments) {
			if (segment.includes("=")) {
				const [key] = segment.split("=", 1);
				if (key) {
					foundKeys.add(key);
				}
			}
		}

		return this.partitionKeys.filter((key) => !foundKeys.has(key));
	}

	/**
	 * Extract only specific keys from a partition path
	 * @param path The partition path
	 * @param keys Keys to extract
	 * @returns Object containing only the specified keys
	 */
	extractKeys(path: string, keys: string[]): Partial<z.infer<T>> {
		const full = this.safeParse(path);
		if (!full.success) {
			throw new Error(`Invalid partition path: ${path}`);
		}

		const result: Partial<z.infer<T>> = {};
		for (const key of keys) {
			if (key in full.data) {
				result[key as keyof typeof result] =
					full.data[key as keyof typeof full.data];
			}
		}

		return result;
	}

	/**
	 * Apply a transformation function to partition values
	 * @param path Original partition path
	 * @param transformFn Function that takes current values and returns new values
	 * @returns New partition path with transformed values
	 */
	transform(
		path: string,
		transformFn: (data: z.infer<T>) => Partial<z.infer<T>>,
	): string {
		const parsed = this.parse(path);
		const transformed = { ...parsed, ...transformFn(parsed) };
		return this.format(transformed);
	}

	/**
	 * Check if a path matches a glob pattern
	 * Uses simplified glob matching logic for * wildcards
	 * @param path The path to check
	 * @param pattern The glob pattern
	 * @returns True if the path matches the pattern
	 */
	matchesGlob(path: string, pattern: string): boolean {
		const pathSegments = path
			.split("/")
			.filter((segment) => segment.length > 0);
		const patternSegments = pattern
			.split("/")
			.filter((segment) => segment.length > 0);

		if (pathSegments.length !== patternSegments.length) {
			return false;
		}

		for (let i = 0; i < pathSegments.length; i++) {
			const pathSeg = pathSegments[i];
			const patternSeg = patternSegments[i];

			if (!this.segmentMatchesPattern(pathSeg, patternSeg)) {
				return false;
			}
		}

		return true;
	}

	private segmentMatchesPattern(segment: string, pattern: string): boolean {
		// Handle exact match
		if (segment === pattern) {
			return true;
		}

		// Handle key=* wildcard pattern
		if (pattern.includes("=*")) {
			const [patternKey] = pattern.split("=", 1);
			const [segmentKey] = segment.split("=", 1);
			return patternKey === segmentKey;
		}

		// Handle more complex patterns with wildcards
		const regex = new RegExp(
			`^${pattern.replace(/\*/g, ".*").replace(/\?/g, ".")}$`,
		);

		return regex.test(segment);
	}
}

// Extended example usage
function runExamples() {
	// 1. Basic date partition schema
	const datePartitionSchema = z.object({
		year: z.coerce.number().int().min(2000).max(2100),
		month: z.coerce.number().int().min(1).max(12),
		day: z.coerce.number().int().min(1).max(31),
	});

	const dateParser = new HivePartitionParser(datePartitionSchema);

	// Parse a date partition
	const dateData = dateParser.parse("/data/year=2023/month=12/day=25");
	console.log("Date Partition Data:", dateData);

	// Format data back to a path
	const datePath = dateParser.format({ year: 2024, month: 3, day: 22 });
	console.log("Formatted Date Path:", datePath);

	// Create a glob pattern for March 2024
	const dateGlob = dateParser.createGlobPattern({ year: 2024, month: 3 });
	console.log("Date Glob Pattern:", dateGlob);

	// Check if a path is valid
	console.log(
		"Valid path?",
		dateParser.isValid("/data/year=2023/month=12/day=25"),
	);
	console.log(
		"Invalid path?",
		dateParser.isValid("/data/year=2023/month=13/day=25"),
	);

	// Get validation errors
	const dateErrors = dateParser.getValidationErrors(
		"/data/year=2023/month=13/day=32",
	);
	console.log("Validation Errors:", dateErrors);

	// 2. More complex analytics partition schema
	const analyticsPartitionSchema = z.object({
		region: z.enum(["us-east", "us-west", "eu", "asia"]),
		service: z.string().min(1),
		year: z.coerce.number().int().min(2000),
		month: z.coerce.number().int().min(1).max(12),
		day: z.coerce.number().int().min(1).max(31),
		event_type: z.enum(["click", "view", "purchase", "error"]),
	});

	const analyticsParser = new HivePartitionParser(analyticsPartitionSchema);

	// Parse a complex analytics path
	const analyticsData = analyticsParser.safeParse(
		"/analytics/region=us-east/service=checkout/year=2023/month=12/day=25/event_type=purchase",
	);
	console.log(
		"Analytics Parse Result:",
		analyticsData.success ? analyticsData.data : analyticsData.error,
	);

	// Extract only specific keys
	if (analyticsData.success) {
		const regionAndService = analyticsParser.extractKeys(
			"/analytics/region=us-east/service=checkout/year=2023/month=12/day=25/event_type=purchase",
			["region", "service"],
		);
		console.log("Extracted Region and Service:", regionAndService);
	}

	// Transform a path
	const transformed = analyticsParser.transform(
		"/analytics/region=us-east/service=checkout/year=2023/month=12/day=25/event_type=purchase",
		(data) => ({ region: "eu", month: data.month + 1 }),
	);
	console.log("Transformed Path:", transformed);

	// Check glob matching
	const matchesGlob = analyticsParser.matchesGlob(
		"/analytics/region=us-east/service=checkout/year=2023/month=12/day=25/event_type=purchase",
		"region=us-east/service=*/year=2023/month=*/day=*/event_type=purchase",
	);
	console.log("Matches Glob Pattern:", matchesGlob);

	// 3. Partition with optional and nullable fields
	const logPartitionSchema = z.object({
		app: z.string(),
		environment: z.enum(["dev", "test", "staging", "prod"]),
		date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
		level: z.enum(["INFO", "WARN", "ERROR", "DEBUG"]).optional(),
		instance: z.string().nullable(),
	});

	const logParser = new HivePartitionParser(logPartitionSchema);

	const logData = logParser.parse(
		"/logs/app=api/environment=prod/date=2023-12-25/level=ERROR/instance=null",
	);
	console.log("Log Data:", logData);
}
