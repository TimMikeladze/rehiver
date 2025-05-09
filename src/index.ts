import fs from "node:fs/promises";
import path from "node:path";
import {
	type BucketLocationConstraint,
	CreateBucketCommand,
	type CreateBucketCommandInput,
	HeadBucketCommand,
	HeadObjectCommand,
	ListObjectsV2Command,
	type ListObjectsV2CommandInput,
	PutObjectCommand,
	S3Client,
} from "@aws-sdk/client-s3";
import { LRUCache } from "lru-cache";
import micromatch from "micromatch";
import mime from "mime-types";
import pLimit from "p-limit";
import type { z } from "zod";

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
 * Validate S3 bucket name according to AWS naming rules
 * @param bucketName Name of the bucket to validate
 * @returns True if valid, false otherwise
 */
export function isValidBucketName(bucketName: string): boolean {
	// AWS bucket naming rules
	// - 3-63 characters long
	// - Can contain lowercase letters, numbers, dots, and hyphens
	// - Must start and end with a letter or number
	// - Cannot contain two adjacent periods
	// - Cannot be formatted as an IP address (e.g., 192.168.5.4)
	// - Cannot start with the prefix 'xn--'
	// - Cannot end with the suffix '-s3alias'

	if (!bucketName || bucketName.length < 3 || bucketName.length > 63) {
		return false;
	}

	// Check if starts/ends with letter or number
	if (!/^[a-z0-9].*[a-z0-9]$/i.test(bucketName)) {
		return false;
	}

	// Check for valid characters
	if (!/^[a-z0-9.-]+$/i.test(bucketName)) {
		return false;
	}

	// Check for adjacent periods
	if (bucketName.includes("..")) {
		return false;
	}

	// Check for IP address format
	if (/^\d+\.\d+\.\d+\.\d+$/.test(bucketName)) {
		return false;
	}

	// Check for forbidden prefixes/suffixes
	if (bucketName.startsWith("xn--") || bucketName.endsWith("-s3alias")) {
		return false;
	}

	return true;
}

/**
 * Log levels for the logger
 */
export enum LogLevel {
	Debug = 0,
	Info = 1,
	Warn = 2,
	Error = 3,
	None = 100,
}

/**
 * Logger interface for custom logging implementations
 */
export interface Logger {
	debug(message: string, ...args: unknown[]): void;
	info(message: string, ...args: unknown[]): void;
	warn(message: string, ...args: unknown[]): void;
	error(message: string, ...args: unknown[]): void;
	setLevel(level: LogLevel): void;
	getLevel(): LogLevel;
}

/**
 * Content type detection and handling utilities
 */
export const ContentType = {
	/**
	 * Detect content type from a file path or key
	 * @param path Path or key to determine content type from
	 * @returns The detected MIME type or application/octet-stream if unknown
	 */
	detect(path: string): string {
		const contentType = mime.lookup(path);
		return contentType || "application/octet-stream";
	},

	/**
	 * Get charset for a given content type
	 * @param contentType MIME type to get charset for
	 * @returns Charset string or null if not applicable
	 */
	charset(contentType: string): string | null {
		return mime.charset(contentType) || null;
	},

	/**
	 * Get file extension for a content type
	 * @param contentType MIME type
	 * @returns File extension (with dot) or false if not found
	 */
	extension(contentType: string): string | false {
		return mime.extension(contentType);
	},

	/**
	 * Check if a content type represents text data
	 * @param contentType MIME type to check
	 * @returns True if content type is text-based
	 */
	isText(contentType: string): boolean {
		return (
			contentType.startsWith("text/") ||
			contentType === "application/json" ||
			contentType === "application/xml" ||
			contentType === "application/javascript" ||
			contentType === "application/typescript"
		);
	},

	/**
	 * Check if a content type represents binary data
	 * @param contentType MIME type to check
	 * @returns True if content type is binary
	 */
	isBinary(contentType: string): boolean {
		return !this.isText(contentType);
	},
};

/**
 * Default logger implementation using console
 */
export class ConsoleLogger implements Logger {
	private level: LogLevel = LogLevel.Info;

	constructor(level?: LogLevel) {
		if (level !== undefined) {
			this.level = level;
		}
	}

	debug(message: string, ...args: unknown[]): void {
		if (this.level <= LogLevel.Debug) {
			console.debug(`[DEBUG] ${message}`, ...args);
		}
	}

	info(message: string, ...args: unknown[]): void {
		if (this.level <= LogLevel.Info) {
			console.info(`[INFO] ${message}`, ...args);
		}
	}

	warn(message: string, ...args: unknown[]): void {
		if (this.level <= LogLevel.Warn) {
			console.warn(`[WARN] ${message}`, ...args);
		}
	}

	error(message: string, ...args: unknown[]): void {
		if (this.level <= LogLevel.Error) {
			console.error(`[ERROR] ${message}`, ...args);
		}
	}

	setLevel(level: LogLevel): void {
		this.level = level;
	}

	getLevel(): LogLevel {
		return this.level;
	}
}

/**
 * No-op logger that discards all log messages
 */
export class NoopLogger implements Logger {
	debug(): void {}
	info(): void {}
	warn(): void {}
	error(): void {}
	setLevel(): void {}
	getLevel(): LogLevel {
		return LogLevel.None;
	}
}

// Global logger instance
let globalLogger: Logger = new ConsoleLogger();

/**
 * Set the global logger instance
 * @param logger The logger instance to use globally
 */
export function setGlobalLogger(logger: Logger): void {
	globalLogger = logger;
}

/**
 * Get the current global logger instance
 * @returns The current global logger
 */
export function getGlobalLogger(): Logger {
	return globalLogger;
}

/**
 * Sleep function for retry delays
 * @param ms Milliseconds to sleep
 */
const sleep = (ms: number): Promise<void> =>
	new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Retry a function with exponential backoff
 * @param fn Function to retry
 * @param retries Maximum number of retries
 * @param baseDelay Base delay in milliseconds
 * @param maxDelay Maximum delay in milliseconds
 * @param logger Optional logger to use instead of global logger
 */
async function retryWithBackoff<T>(
	fn: () => Promise<T>,
	retries = 5,
	baseDelay = 100,
	maxDelay = 30000,
	logger: Logger = globalLogger,
): Promise<T> {
	let lastError: Error | undefined;

	for (let i = 0; i < retries; i++) {
		try {
			return await fn();
		} catch (error) {
			lastError = error as Error;

			// Only retry on throttling, timeout, or network errors
			const errorCode =
				(error as Record<string, unknown>)?.Code ||
				(error as Record<string, unknown>)?.code;
			const errorMessage = (error as Error)?.message || "";
			const isRetryable =
				errorCode === "ThrottlingException" ||
				errorCode === "RequestTimeout" ||
				errorCode === "NetworkingError" ||
				errorCode === "TimeoutError" ||
				errorCode === "RequestTimeTooSkewed" ||
				errorCode === "ProvisionedThroughputExceededException" ||
				errorCode === "SlowDown" ||
				errorCode === "InternalError" ||
				errorCode === "ServiceUnavailable" ||
				errorCode === "429" ||
				errorCode === "500" ||
				errorCode === "503" ||
				errorMessage.includes("timeout") ||
				errorMessage.includes("throttl") ||
				errorMessage.includes("network") ||
				errorMessage.includes("connection");

			if (!isRetryable) {
				throw error;
			}

			// Calculate exponential backoff with jitter
			const delay = Math.min(
				maxDelay,
				baseDelay * 2 ** i * (0.8 + Math.random() * 0.4),
			);

			// Log retry attempt
			logger.debug(
				`S3 operation failed (attempt ${i + 1}/${retries}), retrying in ${Math.round(delay)}ms: ${errorCode || errorMessage}`,
			);

			await sleep(delay);
		}
	}

	throw lastError;
}

/**
 * Safely encodes S3 keys for use in URLs and handling special characters
 * @param key S3 key to encode
 * @returns URL-encoded key
 */
export function encodeS3Key(key: string): string {
	// Encode all special characters except forward slashes
	// This makes the key safe for URL use and file operations
	return key
		.split("/")
		.map((segment) => encodeURIComponent(segment))
		.join("/");
}

/**
 * Safely decodes S3 keys from URLs
 * @param encodedKey URL-encoded S3 key
 * @returns Decoded key
 */
export function decodeS3Key(encodedKey: string): string {
	// Decode all URL-encoded components
	return encodedKey
		.split("/")
		.map((segment) => decodeURIComponent(segment))
		.join("/");
}

/**
 * Interface for object metadata
 */
export interface ObjectMetadata {
	key: string;
	size: number;
	etag: string;
	lastModified: Date;
	contentType?: string;
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
	client?: S3Client;
}

/**
 * Metadata cache configuration
 */
export interface MetadataCacheConfig {
	enabled?: boolean;
	maxSize?: number;
	ttl?: number; // Time to live in ms
	refreshThreshold?: number; // % of TTL when to refresh in background
}

/**
 * A utility class for working with S3 paths and glob patterns
 */
export class S3PathMatcher extends PathMatcher {
	protected s3Client: S3Client;
	private readonly metadataCache: LRUCache<string, ObjectMetadata>;
	private readonly cacheConfig: Required<MetadataCacheConfig>;
	private readonly pendingRefreshes: Set<string> = new Set();
	protected readonly logger: Logger;

	/**
	 * Creates a new S3PathMatcher with S3 client and caching configuration
	 * @param options Micromatch options for path matching
	 * @param s3Options S3 client configuration
	 * @param cacheOptions Metadata cache configuration
	 * @param logger Custom logger to use (defaults to global logger)
	 */
	constructor(
		options: micromatch.Options = {},
		s3Options: {
			region?: string;
			endpoint?: string;
			credentials?: { accessKeyId: string; secretAccessKey: string };
			forcePathStyle?: boolean;
			maxRetries?: number;
			client?: S3Client;
		} = {},
		cacheOptions: MetadataCacheConfig = {},
		logger: Logger = globalLogger,
	) {
		super(options);

		this.logger = logger;

		// Use provided S3 client or create a new one
		if (s3Options.client) {
			this.s3Client = s3Options.client;
		} else {
			// Configure S3 client
			this.s3Client = new S3Client({
				region: s3Options.region || "us-east-1",
				endpoint: s3Options.endpoint,
				credentials: s3Options.credentials,
				forcePathStyle: s3Options.forcePathStyle,
				maxAttempts: s3Options.maxRetries || 3,
			});
		}

		// Configure metadata cache
		this.cacheConfig = {
			enabled: cacheOptions.enabled ?? true,
			maxSize: cacheOptions.maxSize ?? 1000,
			ttl: cacheOptions.ttl ?? 5 * 60 * 1000, // Default 5 minutes
			refreshThreshold: cacheOptions.refreshThreshold ?? 80, // Refresh at 80% of TTL by default
		};

		// Initialize LRU cache
		this.metadataCache = new LRUCache<string, ObjectMetadata>({
			max: this.cacheConfig.maxSize,
			ttl: this.cacheConfig.ttl,
		});
	}

	/**
	 * Validates a bucket name according to AWS rules
	 * @param bucketName Name of the bucket to validate
	 * @throws Error if bucket name is invalid
	 */
	protected validateBucket(bucketName: string): void {
		if (!isValidBucketName(bucketName)) {
			throw new Error(
				`Invalid bucket name: ${bucketName}. S3 bucket names must be 3-63 characters, contain only lowercase letters, numbers, periods, and hyphens, and cannot be formatted as an IP address.`,
			);
		}
	}

	/**
	 * Gets or fetches object metadata from cache or S3
	 * @param bucketName S3 bucket name
	 * @param key Object key
	 * @returns Object metadata or null if not found
	 */
	async getObjectMetadata(
		bucketName: string,
		key: string,
	): Promise<ObjectMetadata | null> {
		this.validateBucket(bucketName);

		// Handle special characters in the key
		const safeKey = key.includes("%") ? decodeS3Key(key) : key;
		const cacheKey = `${bucketName}:${safeKey}`;

		// Check if we have a valid cached entry
		if (this.cacheConfig.enabled) {
			const cached = this.metadataCache.get(cacheKey);
			if (cached) {
				// Check if we should refresh in the background
				const cacheEntryAge =
					Date.now() - (this.metadataCache.getRemainingTTL(cacheKey) || 0);
				const refreshThreshold =
					this.cacheConfig.ttl * (this.cacheConfig.refreshThreshold / 100);

				if (
					cacheEntryAge > refreshThreshold &&
					!this.pendingRefreshes.has(cacheKey)
				) {
					// Start background refresh
					this.pendingRefreshes.add(cacheKey);
					this.refreshMetadataInBackground(bucketName, safeKey, cacheKey);
				}

				return cached;
			}
		}

		try {
			// Fetch headObject using retryWithBackoff
			const metadata = await retryWithBackoff(
				async () => {
					try {
						const headObjectCommand = new HeadObjectCommand({
							// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
							Bucket: bucketName,
							// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
							Key: safeKey,
						});

						const response = await this.s3Client.send(headObjectCommand);

						// Convert the S3 response to our metadata format
						return {
							key: safeKey,
							size: response.ContentLength || 0,
							etag: response.ETag
								? response.ETag.replace(/^"(.+)"$/, "$1")
								: "",
							lastModified: response.LastModified || new Date(),
							contentType: response.ContentType || ContentType.detect(safeKey),
						};
					} catch (error: unknown) {
						// If object doesn't exist, return null
						if (
							typeof error === "object" &&
							error !== null &&
							((error as { name?: string }).name === "NotFound" ||
								(error as { $metadata?: { httpStatusCode?: number } }).$metadata
									?.httpStatusCode === 404)
						) {
							return null;
						}
						throw error;
					}
				},
				5,
				100,
				30000,
				this.logger,
			);

			// Cache the result if found
			if (metadata && this.cacheConfig.enabled) {
				this.metadataCache.set(cacheKey, metadata);
			}

			return metadata;
		} catch (error) {
			this.logger.error(`Error fetching S3 object metadata: ${error}`);
			return null;
		} finally {
			this.pendingRefreshes.delete(cacheKey);
		}
	}

	/**
	 * Refreshes metadata in the background without blocking
	 * @param bucketName Bucket name
	 * @param key Object key
	 * @param cacheKey Cache key
	 */
	private async refreshMetadataInBackground(
		bucketName: string,
		key: string,
		cacheKey: string,
	): Promise<void> {
		try {
			const headObjectCommand = new HeadObjectCommand({
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
				Bucket: bucketName,
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
				Key: key,
			});

			const response = await this.s3Client.send(headObjectCommand);

			// Update cache with fresh data
			const metadata = {
				key,
				size: response.ContentLength || 0,
				etag: response.ETag ? response.ETag.replace(/^"(.+)"$/, "$1") : "",
				lastModified: response.LastModified || new Date(),
				contentType: response.ContentType || ContentType.detect(key),
			};

			this.metadataCache.set(cacheKey, metadata);
		} catch (error) {
			// Ignore errors in background refresh
			this.logger.debug(`Background refresh failed for ${cacheKey}: ${error}`);
		} finally {
			this.pendingRefreshes.delete(cacheKey);
		}
	}

	/**
	 * Invalidates a specific cache entry
	 * @param bucketName Bucket name
	 * @param key Object key
	 */
	invalidateCache(bucketName: string, key: string): void {
		if (this.cacheConfig.enabled) {
			const safeKey = key.includes("%") ? decodeS3Key(key) : key;
			this.metadataCache.delete(`${bucketName}:${safeKey}`);
		}
	}

	/**
	 * Clears the entire metadata cache
	 */
	clearCache(): void {
		if (this.cacheConfig.enabled) {
			this.metadataCache.clear();
			this.pendingRefreshes.clear();
		}
	}

	/**
	 * Lists all objects in an S3 bucket, with optional prefix
	 * @param bucketName S3 bucket name
	 * @param prefix Key prefix to filter by
	 * @param options Additional options for listing
	 * @returns Array of object keys
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
		this.validateBucket(bucketName);

		const {
			maxConcurrentRequests = 5,
			maxKeysPerRequest = 1000,
			abortSignal,
		} = options;

		// Create a set to store all object keys
		const allKeys = new Set<string>();

		// Create a queue for pagination tokens to process
		const paginationQueue: Array<string | undefined> = [undefined]; // Start with undefined for first page
		const limit = pLimit(maxConcurrentRequests);
		const tasks: Promise<void>[] = [];

		// Define the page processing function
		const processPage = async (
			client: S3Client,
			pageParams: ListObjectsV2CommandInput,
		): Promise<{
			objects: string[];
			nextToken?: string;
		}> => {
			try {
				// Use retryWithBackoff for resilience
				const response = await retryWithBackoff(
					() => client.send(new ListObjectsV2Command(pageParams)),
					5,
					100,
					30000,
					this.logger,
				);

				// Extract keys from the response
				const objects: string[] = [];
				if (response.Contents) {
					for (const object of response.Contents) {
						if (object.Key) {
							// Handle special characters in keys by decoding if needed
							const decodedKey = object.Key.includes("%")
								? decodeS3Key(object.Key)
								: object.Key;
							objects.push(decodedKey);
						}
					}
				}

				return {
					objects,
					nextToken: response.NextContinuationToken,
				};
			} catch (error) {
				this.logger.error(`Error listing S3 objects: ${error}`);
				throw error;
			}
		};

		// Process all pagination tokens
		while (paginationQueue.length > 0 && !abortSignal?.aborted) {
			// Get the next continuation token
			const continuationToken = paginationQueue.shift();

			// Create a task to process the page
			const task = limit(async () => {
				if (abortSignal?.aborted) {
					return;
				}

				const params: ListObjectsV2CommandInput = {
					// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
					Bucket: bucketName,
					// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
					Prefix: prefix,
					// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
					MaxKeys: maxKeysPerRequest,
				};

				if (continuationToken) {
					// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
					params.ContinuationToken = continuationToken;
				}

				try {
					const { objects, nextToken } = await processPage(
						this.s3Client,
						params,
					);

					// Add keys to our set
					for (const key of objects) {
						allKeys.add(key);
					}

					// Queue the next page if there is one
					if (nextToken) {
						paginationQueue.push(nextToken);
					}
				} catch (error) {
					// Handle page-level errors but allow other pages to continue
					this.logger.error(`Error processing page: ${error}`);
				}
			});

			tasks.push(task);
		}

		// Wait for all tasks to complete
		await Promise.all(tasks);

		if (abortSignal?.aborted) {
			throw new Error("S3 object listing was aborted");
		}

		// Convert set to array
		return Array.from(allKeys);
	}

	/**
	 * Uploads content to S3 with automatic content-type detection
	 * @param bucketName S3 bucket name
	 * @param key Object key
	 * @param body Object content
	 * @param options Additional upload options
	 * @returns ETag of the uploaded object
	 */
	async putObject(
		bucketName: string,
		key: string,
		body: Buffer | Uint8Array | string | Blob | ReadableStream,
		options: {
			contentType?: string;
			contentEncoding?: string;
			contentDisposition?: string;
			cacheControl?: string;
			metadata?: Record<string, string>;
			tagging?: string;
		} = {},
	): Promise<string> {
		this.validateBucket(bucketName);

		// Detect content type if not provided
		const contentType = options.contentType || ContentType.detect(key);

		try {
			const command = new PutObjectCommand({
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
				Bucket: bucketName,
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
				Key: key,
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
				Body: body,
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
				ContentType: contentType,
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
				ContentEncoding: options.contentEncoding,
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
				ContentDisposition: options.contentDisposition,
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
				CacheControl: options.cacheControl,
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
				Metadata: options.metadata,
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
				Tagging: options.tagging,
			});

			const response = await retryWithBackoff(
				() => this.s3Client.send(command),
				5,
				100,
				30000,
				this.logger,
			);

			// Invalidate cache for this object
			this.invalidateCache(bucketName, key);

			// Return the ETag without quotes
			return response.ETag ? response.ETag.replace(/^"(.+)"$/, "$1") : "";
		} catch (error) {
			this.logger.error(`Error uploading object to S3: ${error}`);
			throw error;
		}
	}

	/**
	 * Find objects in an S3 bucket that match the given patterns
	 * @param bucketOrOptions Bucket name or options object
	 * @param patterns Glob patterns to match against object keys
	 * @param options Additional options for listing and matching
	 * @returns Array of matching object keys
	 */
	async findMatchingObjects(
		bucketOrOptions:
			| string
			| {
					bucket: string;
					patterns: string | string[];
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
						requestLimit?: number;
						processingLimit?: number;
					};
					localCache?: {
						enabled: boolean;
						basePath: string;
						skipExisting?: boolean;
					};
					handleSpecialChars?: boolean;
			  },
		patterns?: string | string[],
		options?: {
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
				requestLimit?: number;
				processingLimit?: number;
			};
			localCache?: {
				enabled: boolean;
				basePath: string;
				skipExisting?: boolean;
			};
			handleSpecialChars?: boolean;
		},
	): Promise<string[]> {
		let bucket: string;
		let patternsToUse: string | string[];
		let optionsToUse: typeof options = {};

		// Parse arguments based on the first parameter type
		if (typeof bucketOrOptions === "string") {
			bucket = bucketOrOptions;
			patternsToUse = patterns || "**";
			optionsToUse = options || {};
		} else {
			bucket = bucketOrOptions.bucket;
			patternsToUse = bucketOrOptions.patterns;
			optionsToUse = {
				prefix: bucketOrOptions.prefix,
				maxConcurrentRequests: bucketOrOptions.maxConcurrentRequests,
				maxKeysPerRequest: bucketOrOptions.maxKeysPerRequest,
				matchOptions: bucketOrOptions.matchOptions,
				useNegation: bucketOrOptions.useNegation,
				abortSignal: bucketOrOptions.abortSignal,
				onProgress: bucketOrOptions.onProgress,
				concurrency: bucketOrOptions.concurrency,
				localCache: bucketOrOptions.localCache,
				handleSpecialChars: bucketOrOptions.handleSpecialChars,
			};
		}

		this.validateBucket(bucket);

		const {
			prefix = "",
			maxConcurrentRequests = 5,
			maxKeysPerRequest = 1000,
			matchOptions = {},
			useNegation = false,
			abortSignal,
			onProgress,
			concurrency = {},
			localCache = { enabled: false, basePath: "" },
			handleSpecialChars = true,
		} = optionsToUse;

		// List all objects in the bucket
		let allKeys = await this.listObjects(bucket, prefix, {
			maxConcurrentRequests: concurrency.requestLimit || maxConcurrentRequests,
			maxKeysPerRequest,
			abortSignal,
		});

		// Handle special characters in keys if requested
		if (handleSpecialChars) {
			allKeys = allKeys.map((key) =>
				key.includes("%") ? decodeS3Key(key) : key,
			);
		}

		// Match keys against patterns
		let matchedKeys: string[];
		if (useNegation) {
			matchedKeys = this.not(allKeys, patternsToUse, matchOptions);
		} else {
			matchedKeys = this.match(allKeys, patternsToUse, matchOptions);
		}

		// Call progress callback if provided
		if (onProgress) {
			onProgress({
				processed: allKeys.length,
				total: allKeys.length,
				matched: matchedKeys.length,
				skippedExisting: 0,
			});
		}

		return matchedKeys;
	}

	/**
	 * Streams matching objects through a processor function
	 * @param bucketOrOptions Bucket name or options object
	 * @param patterns Glob patterns to match
	 * @param processor Function to process each matching object
	 * @param options Additional options
	 * @returns Processing statistics
	 */
	async streamMatchingObjects(
		bucketOrOptions:
			| string
			| {
					bucket: string;
					patterns: string | string[];
					processor: (path: string) => Promise<void>;
					prefix?: string;
					batchSize?: number;
					maxConcurrentRequests?: number;
					maxKeysPerRequest?: number;
					matchOptions?: micromatch.Options;
					maxConcurrentProcessing?: number;
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
					handleSpecialChars?: boolean;
			  },
		patterns?: string | string[],
		processor?: (path: string) => Promise<void>,
		options?: {
			prefix?: string;
			batchSize?: number;
			maxConcurrentRequests?: number;
			maxKeysPerRequest?: number;
			matchOptions?: micromatch.Options;
			maxConcurrentProcessing?: number;
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
			handleSpecialChars?: boolean;
		},
	): Promise<{
		processed: number;
		matched: number;
		skipped: number;
		skippedExisting: number;
	}> {
		let bucket: string;
		let patternsToUse: string | string[];
		let processorFn: (path: string) => Promise<void>;
		let optionsToUse: typeof options = {};

		// Parse arguments based on the first parameter type
		if (typeof bucketOrOptions === "string") {
			bucket = bucketOrOptions;
			patternsToUse = patterns || "**";
			processorFn = processor || (async () => {});
			optionsToUse = options || {};
		} else {
			bucket = bucketOrOptions.bucket;
			patternsToUse = bucketOrOptions.patterns;
			processorFn = bucketOrOptions.processor;
			optionsToUse = {
				prefix: bucketOrOptions.prefix,
				batchSize: bucketOrOptions.batchSize,
				maxConcurrentRequests: bucketOrOptions.maxConcurrentRequests,
				maxKeysPerRequest: bucketOrOptions.maxKeysPerRequest,
				matchOptions: bucketOrOptions.matchOptions,
				maxConcurrentProcessing: bucketOrOptions.maxConcurrentProcessing,
				abortSignal: bucketOrOptions.abortSignal,
				onProgress: bucketOrOptions.onProgress,
				localCache: bucketOrOptions.localCache,
				handleSpecialChars: bucketOrOptions.handleSpecialChars,
			};
		}

		this.validateBucket(bucket);

		const {
			prefix = "",
			batchSize = 100,
			maxConcurrentRequests = 5,
			maxKeysPerRequest = 1000,
			matchOptions = {},
			maxConcurrentProcessing = 10,
			abortSignal,
			onProgress,
			localCache = { enabled: false, basePath: "" },
			handleSpecialChars = true,
		} = optionsToUse;

		// Find all matching objects
		const matchingObjects = await this.findMatchingObjects({
			bucket,
			patterns: patternsToUse,
			prefix,
			maxConcurrentRequests,
			maxKeysPerRequest,
			matchOptions,
			abortSignal,
			handleSpecialChars,
		});

		// Process matching objects with concurrency control
		const processingLimit = pLimit(maxConcurrentProcessing);
		const stats = {
			processed: 0,
			matched: matchingObjects.length,
			skipped: 0,
			skippedExisting: 0,
		};

		// Process in batches for better performance
		for (let i = 0; i < matchingObjects.length; i += batchSize) {
			if (abortSignal?.aborted) {
				break;
			}

			const batch = matchingObjects.slice(i, i + batchSize);
			const batchPromises = batch.map((objectKey) =>
				processingLimit(async () => {
					if (abortSignal?.aborted) {
						return;
					}

					try {
						await processorFn(objectKey);
						stats.processed++;
					} catch (error) {
						console.warn(`Error processing object ${objectKey}: ${error}`);
						stats.skipped++;
					}

					// Report progress periodically
					if (onProgress && (stats.processed + stats.skipped) % 100 === 0) {
						onProgress({
							processed: stats.processed + stats.skipped,
							total: matchingObjects.length,
							matched: stats.matched,
							skippedExisting: stats.skippedExisting,
						});
					}
				}),
			);

			await Promise.all(batchPromises);

			// Report progress after each batch
			if (onProgress) {
				onProgress({
					processed: stats.processed + stats.skipped,
					total: matchingObjects.length,
					matched: stats.matched,
					skippedExisting: stats.skippedExisting,
				});
			}
		}

		return stats;
	}

	/**
	 * Get the S3 client instance
	 * @returns The S3Client instance
	 */
	getS3Client(): S3Client {
		return this.s3Client;
	}
}

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

/**
 * Supported time granularity types
 */
export enum TimeGranularity {
	Hourly = "hourly",
	Daily = "daily",
	Monthly = "monthly",
	Yearly = "yearly",
}

/**
 * Configuration for time-based partition path generation
 */
export interface TimePartitionConfig {
	granularity: TimeGranularity;
	includeHour?: boolean;
	includeMinute?: boolean;
	format?: "hive" | "path";
	prefix?: string;
	dateFormat?: {
		year?: string;
		month?: string;
		day?: string;
		hour?: string;
		minute?: string;
	};
}

/**
 * Utility for generating time-based partition paths
 */
export class TimePartitionGenerator {
	private config: TimePartitionConfig;

	/**
	 * Creates a new TimePartitionGenerator
	 * @param config Configuration for partition path generation
	 */
	constructor(config: TimePartitionConfig) {
		// Set defaults
		this.config = {
			includeHour: false,
			includeMinute: false,
			format: "hive",
			prefix: "",
			dateFormat: {
				year: "yyyy",
				month: "MM",
				day: "dd",
				hour: "HH",
				minute: "mm",
			},
			...config,
		};

		// Validate configuration
		if (
			this.config.granularity !== TimeGranularity.Hourly &&
			this.config.includeMinute
		) {
			this.config.includeHour = true; // Minutes require hours
		}

		if (this.config.granularity === TimeGranularity.Hourly) {
			this.config.includeHour = true; // Hourly granularity always includes hours
		}
	}

	/**
	 * Generate a partition path for a specific date
	 * @param date The date to generate the partition for
	 * @returns Formatted partition path string
	 */
	generatePath(date: Date = new Date()): string {
		const year = date.getFullYear().toString();
		// Month is 0-indexed in JS Date, so add 1 and pad with leading zero if needed
		const month = (date.getMonth() + 1).toString().padStart(2, "0");
		const day = date.getDate().toString().padStart(2, "0");
		const hour = date.getHours().toString().padStart(2, "0");
		const minute = date.getMinutes().toString().padStart(2, "0");

		const segments: string[] = [];
		const { format, prefix, granularity } = this.config;

		// Add prefix if provided
		if (prefix && prefix.length > 0) {
			segments.push(prefix);
		}

		// Always include year
		if (format === "hive") {
			segments.push(`year=${year}`);
		} else {
			segments.push(year);
		}

		// For granularity monthly, yearly, or when explicitly requested
		if (
			granularity === TimeGranularity.Monthly ||
			granularity === TimeGranularity.Daily ||
			granularity === TimeGranularity.Hourly
		) {
			if (format === "hive") {
				segments.push(`month=${month}`);
			} else {
				segments.push(month);
			}
		}

		// For granularity daily or hourly, or when explicitly requested
		if (
			granularity === TimeGranularity.Daily ||
			granularity === TimeGranularity.Hourly
		) {
			if (format === "hive") {
				segments.push(`day=${day}`);
			} else {
				segments.push(day);
			}
		}

		// Include hour if configured
		if (this.config.includeHour) {
			if (format === "hive") {
				segments.push(`hour=${hour}`);
			} else {
				segments.push(hour);
			}
		}

		// Include minute if configured
		if (this.config.includeMinute) {
			if (format === "hive") {
				segments.push(`minute=${minute}`);
			} else {
				segments.push(minute);
			}
		}

		return segments.join("/");
	}

	/**
	 * Generate partition paths for a time range
	 * @param startDate Start of the range
	 * @param endDate End of the range (inclusive)
	 * @returns Array of partition paths
	 */
	generatePathsForRange(startDate: Date, endDate: Date): string[] {
		const paths: string[] = [];
		const current = new Date(startDate);

		while (current <= endDate) {
			paths.push(this.generatePath(current));

			// Move to next period based on granularity
			switch (this.config.granularity) {
				case TimeGranularity.Yearly:
					current.setFullYear(current.getFullYear() + 1);
					break;
				case TimeGranularity.Monthly:
					current.setMonth(current.getMonth() + 1);
					break;
				case TimeGranularity.Daily:
					current.setDate(current.getDate() + 1);
					break;
				case TimeGranularity.Hourly:
					current.setHours(current.getHours() + 1);
					break;
				default:
					current.setDate(current.getDate() + 1);
			}
		}

		return paths;
	}

	/**
	 * Generate a partition path for the current time
	 * @returns Formatted partition path for current time
	 */
	generateCurrentPath(): string {
		return this.generatePath(new Date());
	}
}

/**
 * Main Rehiver class for S3 operations with pattern matching
 */
export class Rehiver extends S3PathMatcher {
	// Static factory methods for creating components
	static partition = {
		create: <T extends z.ZodTypeAny>(schema: T): HivePartitionParser<T> => {
			return new HivePartitionParser(schema);
		},
	};

	static time = {
		daily: (options?: Partial<TimePartitionConfig>): TimePartitionGenerator => {
			return new TimePartitionGenerator({
				granularity: TimeGranularity.Daily,
				...options,
			});
		},
		hourly: (
			options?: Partial<TimePartitionConfig>,
		): TimePartitionGenerator => {
			return new TimePartitionGenerator({
				granularity: TimeGranularity.Hourly,
				...options,
			});
		},
		monthly: (
			options?: Partial<TimePartitionConfig>,
		): TimePartitionGenerator => {
			return new TimePartitionGenerator({
				granularity: TimeGranularity.Monthly,
				...options,
			});
		},
		yearly: (
			options?: Partial<TimePartitionConfig>,
		): TimePartitionGenerator => {
			return new TimePartitionGenerator({
				granularity: TimeGranularity.Yearly,
				...options,
			});
		},
		custom: (config: TimePartitionConfig): TimePartitionGenerator => {
			return new TimePartitionGenerator(config);
		},
	};

	static changes = {
		detect: (options?: ChangeDetectionOptions): ChangeDetectionEngine => {
			return new ChangeDetectionEngine(options);
		},
	};

	/**
	 * Creates an S3 bucket if it doesn't already exist
	 * @param bucketName Name of the bucket to create
	 * @param options S3 client configuration and bucket creation options
	 * @returns True if the bucket was created, false if it already existed
	 * @throws If bucket name is invalid or creation fails for other reasons
	 */
	async createBucketIfNotExists(
		bucketName: string,
		options: {
			region?: string;
			endpoint?: string;
			credentials?: {
				accessKeyId: string;
				secretAccessKey: string;
				sessionToken?: string;
			};
			forcePathStyle?: boolean;
			maxRetries?: number;
			bucketOptions?: {
				locationConstraint?: string;
				acl?:
					| "private"
					| "public-read"
					| "public-read-write"
					| "authenticated-read";
			};
			logger?: Logger;
			client?: S3Client;
		} = {},
	): Promise<boolean> {
		// Validate bucket name
		if (!isValidBucketName(bucketName)) {
			throw new Error(
				`Invalid bucket name: ${bucketName}. S3 bucket names must be 3-63 characters, contain only lowercase letters, numbers, periods, and hyphens, and cannot be formatted as an IP address.`,
			);
		}

		const logger = options.logger || globalLogger;

		// Use provided S3 client or create a new one
		let s3Client: S3Client;
		let shouldDestroyClient = false;

		if (options.client) {
			s3Client = options.client;
		} else {
			// Create S3 client
			s3Client = new S3Client({
				region: options.region || "us-east-1",
				endpoint: options.endpoint,
				credentials: options.credentials,
				forcePathStyle: options.forcePathStyle,
				maxAttempts: options.maxRetries || 3,
			});
			shouldDestroyClient = true;
		}

		try {
			// Check if bucket exists
			try {
				// Try to head the bucket to check if it exists
				// biome-ignore lint/style/useNamingConvention: <explanation>
				await s3Client.send(new HeadBucketCommand({ Bucket: bucketName }));
				logger.debug(`Bucket ${bucketName} already exists`);
				return false; // Bucket already exists
			} catch (error: unknown) {
				// If the error is not a "bucket does not exist" error, rethrow it
				const statusCode = (
					error as { $metadata?: { httpStatusCode?: number } }
				)?.$metadata?.httpStatusCode;
				if (statusCode !== 404 && statusCode !== 403) {
					throw error;
				}

				// Bucket doesn't exist, continue to creation
				logger.info(`Bucket ${bucketName} does not exist, creating it`);
			}

			// Create bucket with options
			const createBucketParams: CreateBucketCommandInput = {
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
				Bucket: bucketName,
			};

			// Add location constraint if provided and not in us-east-1
			// (us-east-1 is the default and should not be specified explicitly)
			if (
				options.bucketOptions?.locationConstraint &&
				options.bucketOptions.locationConstraint !== "us-east-1"
			) {
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
				createBucketParams.CreateBucketConfiguration = {
					// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
					LocationConstraint: options.bucketOptions
						.locationConstraint as BucketLocationConstraint,
				};
			}

			// Add ACL if provided
			if (options.bucketOptions?.acl) {
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
				createBucketParams.ACL = options.bucketOptions.acl;
			}

			// Create the bucket with retries
			await retryWithBackoff(
				async () => s3Client.send(new CreateBucketCommand(createBucketParams)),
				5,
				100,
				30000,
				logger,
			);

			logger.info(`Successfully created bucket ${bucketName}`);
			return true; // Bucket was created
		} finally {
			// Clean up the client only if we created it
			if (shouldDestroyClient) {
				s3Client.destroy();
			}
		}
	}

	partition = Rehiver.partition;
	time = Rehiver.time;
	changes = Rehiver.changes;

	protected logger: Logger;
	private s3ConfigOptions: {
		region?: string;
		endpoint?: string;
		credentials?: { accessKeyId: string; secretAccessKey: string };
		forcePathStyle?: boolean;
		maxRetries?: number;
		client?: S3Client;
	};

	constructor(
		options: {
			matchOptions?: micromatch.Options;
			s3Options?: {
				region?: string;
				endpoint?: string;
				credentials?: { accessKeyId: string; secretAccessKey: string };
				forcePathStyle?: boolean;
				maxRetries?: number;
				client?: S3Client;
			};
			cacheOptions?: MetadataCacheConfig;
			loggerOptions?: {
				logger?: Logger;
				level?: LogLevel;
			};
		} = {},
	) {
		// Configure logger first
		const logger = options.loggerOptions?.logger || globalLogger;

		// Call super with the logger
		super(
			options.matchOptions || {},
			options.s3Options || {},
			options.cacheOptions || {},
			logger,
		);

		// Store the logger reference
		this.logger = logger;

		// Store S3 options for later use
		this.s3ConfigOptions = options.s3Options || {};

		// Set logger level if specified
		if (options.loggerOptions?.level !== undefined) {
			this.logger.setLevel(options.loggerOptions.level);
		}

		// Store S3 client
		this.s3Client =
			options.s3Options?.client ||
			new S3Client({
				region: options.s3Options?.region || "us-east-1",
				endpoint: options.s3Options?.endpoint,
				credentials: options.s3Options?.credentials,
				forcePathStyle: options.s3Options?.forcePathStyle,
				maxAttempts: options.s3Options?.maxRetries || 3,
			});
	}

	/**
	 * Set the logger for this instance
	 * @param logger Logger to use
	 */
	setLogger(logger: Logger): void {
		Object.defineProperty(this, "logger", {
			value: logger,
			writable: true,
			configurable: true,
		});
	}

	/**
	 * Get the current logger
	 * @returns The current logger instance
	 */
	getLogger(): Logger {
		return this.logger as Logger;
	}

	/**
	 * Create a HivePartitionParser with a given schema
	 * @param schema The Zod schema defining the partition structure
	 */
	partitionParser<T extends z.ZodTypeAny>(schema: T): HivePartitionParser<T> {
		return new HivePartitionParser(schema);
	}

	/**
	 * Create a TimePartitionGenerator with the given configuration
	 * @param config Configuration for time partitioning
	 */
	timePartitioner(config: TimePartitionConfig): TimePartitionGenerator {
		return new TimePartitionGenerator(config);
	}

	/**
	 * Create a ChangeDetectionEngine for tracking object changes
	 * @param options Configuration options for change detection
	 */
	changeDetector(options: ChangeDetectionOptions = {}): ChangeDetectionEngine {
		return new ChangeDetectionEngine(options);
	}

	/**
	 * Check if a path matches a pattern
	 * @param path The path to check
	 * @param pattern The pattern to match against
	 * @param options Additional match options
	 */
	isMatch(
		path: string,
		pattern: string | string[],
		options?: micromatch.Options,
	): boolean {
		return super.isMatch(path, pattern, options);
	}

	/**
	 * Match an array of paths against a pattern
	 * @param paths The paths to filter
	 * @param patterns The pattern(s) to match against
	 * @param options Additional match options
	 */
	match(
		paths: string[],
		patterns: string | string[],
		options?: micromatch.Options,
	): string[] {
		return super.match(paths, patterns, options);
	}

	/**
	 * Match paths using precompiled regular expressions for better performance
	 * @param paths The paths to filter
	 * @param patterns The pattern(s) to match against
	 * @param options Additional match options
	 */
	matchFast(
		paths: string[],
		patterns: string | string[],
		options?: micromatch.Options,
	): string[] {
		return super.matchFast(paths, patterns, options);
	}

	/**
	 * Get paths that do NOT match the pattern
	 * @param paths The paths to filter
	 * @param patterns The pattern(s) to match against
	 * @param options Additional match options
	 */
	not(
		paths: string[],
		patterns: string | string[],
		options?: micromatch.Options,
	): string[] {
		return super.not(paths, patterns, options);
	}

	/**
	 * Capture values from a path using a pattern with named placeholders
	 * @param pattern The pattern with placeholders (e.g., 'year=:year/month=:month')
	 * @param path The path to extract values from
	 * @param options Additional match options
	 */
	capture(
		pattern: string,
		path: string,
		options?: micromatch.Options,
	): string[] | null {
		return super.capture(pattern, path, options);
	}

	/**
	 * Find objects in S3 that match the specified patterns with support for both positional
	 * and object parameter styles
	 */
	async findMatchingObjects(
		bucketOrOptions:
			| string
			| {
					bucket: string;
					patterns: string | string[];
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
						requestLimit?: number;
						processingLimit?: number;
					};
					localCache?: {
						enabled: boolean;
						basePath: string;
						skipExisting?: boolean;
					};
					handleSpecialChars?: boolean;
			  },
		patterns?: string | string[],
		options?: {
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
				requestLimit?: number;
				processingLimit?: number;
			};
			localCache?: {
				enabled: boolean;
				basePath: string;
				skipExisting?: boolean;
			};
			handleSpecialChars?: boolean;
		},
	): Promise<string[]> {
		// Support for object-based API
		if (typeof bucketOrOptions === "object") {
			const { bucket, patterns: objPatterns, ...objOptions } = bucketOrOptions;

			// Validate bucket name
			this.validateBucket(bucket);

			// Add special character handling by default
			const finalOptions = {
				handleSpecialChars: true,
				...objOptions,
			};

			return super.findMatchingObjects(bucket, objPatterns, finalOptions);
		}

		// Support for positional parameters API (backward compatible)
		// Validate bucket name
		this.validateBucket(bucketOrOptions);

		// Add special character handling by default
		const finalOptions = {
			handleSpecialChars: true,
			...options,
		};

		return super.findMatchingObjects(
			bucketOrOptions,
			patterns || "",
			finalOptions,
		);
	}

	/**
	 * Stream and process objects in S3 that match the specified patterns with support
	 * for both positional and object parameter styles
	 */
	async streamMatchingObjects(
		bucketOrOptions:
			| string
			| {
					bucket: string;
					patterns: string | string[];
					processor: (path: string) => Promise<void>;
					prefix?: string;
					batchSize?: number;
					maxConcurrentRequests?: number;
					maxKeysPerRequest?: number;
					matchOptions?: micromatch.Options;
					maxConcurrentProcessing?: number;
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
			  },
		patterns?: string | string[],
		processor?: (path: string) => Promise<void>,
		options?: {
			prefix?: string;
			batchSize?: number;
			maxConcurrentRequests?: number;
			maxKeysPerRequest?: number;
			matchOptions?: micromatch.Options;
			maxConcurrentProcessing?: number;
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
		},
	): Promise<{
		processed: number;
		matched: number;
		skipped: number;
		skippedExisting: number;
	}> {
		// Support for object-based API
		if (typeof bucketOrOptions === "object") {
			const {
				bucket,
				patterns: objPatterns,
				processor: objProcessor,
				...objOptions
			} = bucketOrOptions;
			return super.streamMatchingObjects(
				bucket,
				objPatterns,
				objProcessor,
				objOptions,
			);
		}

		// Support for positional parameters API (backward compatible)
		if (!processor) {
			throw new Error(
				"Processor function is required when using positional parameters",
			);
		}
		return super.streamMatchingObjects(
			bucketOrOptions,
			patterns || "",
			processor,
			options || {},
		);
	}

	/**
	 * Creates an S3 bucket if it doesn't already exist
	 * Simplified version that uses the instance's configuration
	 * @param bucketName Name of the bucket to create
	 * @param bucketOptions Bucket creation options
	 * @returns True if the bucket was created, false if it already existed
	 * @throws If bucket name is invalid or creation fails for other reasons
	 */
	async createBucket(
		bucketName: string,
		bucketOptions?: {
			locationConstraint?: string;
			acl?:
				| "private"
				| "public-read"
				| "public-read-write"
				| "authenticated-read";
		},
	): Promise<boolean> {
		// Create options object for the full implementation
		const options = {
			region: this.s3ConfigOptions.region,
			endpoint: this.s3ConfigOptions.endpoint,
			credentials: this.s3ConfigOptions.credentials,
			forcePathStyle: this.s3ConfigOptions.forcePathStyle,
			maxRetries: this.s3ConfigOptions.maxRetries,
			bucketOptions,
			logger: this.logger,
			client: this.getS3Client(),
		};

		// Validate bucket name
		if (!isValidBucketName(bucketName)) {
			throw new Error(
				`Invalid bucket name: ${bucketName}. S3 bucket names must be 3-63 characters, contain only lowercase letters, numbers, periods, and hyphens, and cannot be formatted as an IP address.`,
			);
		}

		const logger = options.logger || globalLogger;
		const s3Client = options.client || this.getS3Client();

		try {
			// Check if bucket exists
			try {
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase
				await s3Client.send(new HeadBucketCommand({ Bucket: bucketName }));
				logger.debug(`Bucket ${bucketName} already exists`);
				return false; // Bucket already exists
			} catch (error: unknown) {
				// If the error is not a "bucket does not exist" error, rethrow it
				const statusCode = (
					error as { $metadata?: { httpStatusCode?: number } }
				)?.$metadata?.httpStatusCode;
				if (statusCode !== 404 && statusCode !== 403) {
					throw error;
				}

				// Bucket doesn't exist, continue to creation
				logger.info(`Bucket ${bucketName} does not exist, creating it`);
			}

			// Create bucket with options
			const createBucketParams: CreateBucketCommandInput = {
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase
				Bucket: bucketName,
			};

			// Add location constraint if provided and not in us-east-1
			if (
				bucketOptions?.locationConstraint &&
				bucketOptions.locationConstraint !== "us-east-1"
			) {
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase
				createBucketParams.CreateBucketConfiguration = {
					// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase
					LocationConstraint:
						bucketOptions.locationConstraint as BucketLocationConstraint,
				};
			}

			// Add ACL if provided
			if (bucketOptions?.acl) {
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase
				createBucketParams.ACL = bucketOptions.acl;
			}

			// Create the bucket with retries
			await retryWithBackoff(
				async () => s3Client.send(new CreateBucketCommand(createBucketParams)),
				5,
				100,
				30000,
				logger,
			);

			logger.info(`Successfully created bucket ${bucketName}`);
			return true; // Bucket was created
		} catch (error) {
			logger.error(`Failed to create bucket ${bucketName}: ${error}`);
			throw error;
		}
	}
}

// Set default export to Rehiver
export default Rehiver;
