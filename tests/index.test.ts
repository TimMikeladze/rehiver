import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import {
	CreateBucketCommand,
	DeleteObjectCommand,
	PutObjectCommand,
	S3Client,
} from "@aws-sdk/client-s3";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { z } from "zod";
import {
	ChangeDetectionEngine,
	ChangeType,
	HivePartitionParser,
	type ObjectMetadata,
	PathMatcher,
	Rehiver,
	S3PathMatcher,
	TimeGranularity,
	TimePartitionGenerator,
} from "../src/index";
import DefaultRehiver from "../src/index";

describe("PathMatcher", () => {
	const matcher = new PathMatcher({ dot: true });

	describe("isMatch", () => {
		it("should correctly match a single pattern", () => {
			expect(matcher.isMatch("foo/bar.txt", "**/*.txt")).toBe(true);
			expect(matcher.isMatch("foo/bar.json", "**/*.txt")).toBe(false);
		});

		it("should match with an array of patterns", () => {
			expect(matcher.isMatch("foo/bar.txt", ["**/*.txt", "**/*.json"])).toBe(
				true,
			);
			expect(matcher.isMatch("foo/bar.json", ["**/*.txt", "**/*.json"])).toBe(
				true,
			);
			expect(matcher.isMatch("foo/bar.png", ["**/*.txt", "**/*.json"])).toBe(
				false,
			);
		});
	});

	describe("match", () => {
		it("should filter an array of paths based on a pattern", () => {
			const paths = ["foo/bar.txt", "foo/baz.json", "bar/qux.png", "root.txt"];

			expect(matcher.match(paths, "**/*.txt")).toEqual([
				"foo/bar.txt",
				"root.txt",
			]);
			expect(matcher.match(paths, ["**/*.json", "**/*.png"])).toEqual([
				"foo/baz.json",
				"bar/qux.png",
			]);
		});
	});

	describe("getRegex", () => {
		it("should convert glob patterns to regular expressions", () => {
			const regex = matcher.getRegex("**/*.txt");
			expect(regex).toBeInstanceOf(RegExp);
			expect(regex.test("foo/bar.txt")).toBe(true);
			expect(regex.test("foo/bar.json")).toBe(false);
		});

		it("should cache compiled patterns", () => {
			// Call twice to ensure caching works
			const regex1 = matcher.getRegex("**/*.txt");
			const regex2 = matcher.getRegex("**/*.txt");
			expect(regex1).toBe(regex2); // Same object reference due to caching
		});
	});

	describe("matchFast", () => {
		it("should use precompiled regex for better performance", () => {
			const paths = ["foo/bar.txt", "foo/baz.json", "bar/qux.png", "root.txt"];

			expect(matcher.matchFast(paths, "**/*.txt")).toEqual([
				"foo/bar.txt",
				"root.txt",
			]);
		});
	});

	describe("not", () => {
		it("should return paths that do not match the pattern", () => {
			const paths = ["foo/bar.txt", "foo/baz.json", "bar/qux.png", "root.txt"];

			expect(matcher.not(paths, "**/*.txt")).toEqual([
				"foo/baz.json",
				"bar/qux.png",
			]);
		});
	});

	describe("all", () => {
		it("should check if all patterns match a path", () => {
			expect(matcher.all("foo/bar.txt", ["**/*", "*.txt"])).toBe(false);
			expect(matcher.all("foo/bar.txt", ["**/*", "**/bar.txt"])).toBe(true);
		});
	});

	describe("capture", () => {
		it("should capture values from a path based on a pattern", () => {
			expect(
				matcher.capture("users/:id/posts/:postId", "users/123/posts/456"),
			).toEqual(["123", "456"]);
			expect(
				matcher.capture(
					"files/:year/:month/:day/*.txt",
					"files/2023/01/15/report.txt",
				),
			).toEqual(["2023", "01", "15", "report"]);
			expect(
				matcher.capture(
					"files/:year/:month/:day/*.txt",
					"files/2023/01/15/report.json",
				),
			).toBeNull();
		});
	});
});

describe("ChangeDetectionEngine", () => {
	const testStateDir = path.join(process.cwd(), "test-state");
	const testStateFile = path.join(testStateDir, "test-state.json");
	let engine: ChangeDetectionEngine;

	beforeEach(async () => {
		// Create a fresh engine for each test
		engine = new ChangeDetectionEngine({
			stateFilePath: testStateFile,
			compareMode: "full",
			trackDeleted: true,
		});

		// Create the test state directory if needed
		try {
			await fs.mkdir(testStateDir, { recursive: true });
		} catch (err) {
			// Directory may already exist
		}

		// Clean up any existing state file
		try {
			await fs.unlink(testStateFile);
		} catch (err) {
			// File might not exist yet
		}
	});

	afterAll(async () => {
		// Clean up the test state directory
		try {
			await fs.rm(testStateDir, { recursive: true, force: true });
		} catch (err) {
			console.error("Error cleaning up test state directory:", err);
		}
	});

	describe("object tracking", () => {
		it("should add objects to the current state", () => {
			const object1: ObjectMetadata = {
				key: "test/file1.txt",
				size: 100,
				etag: "abc123",
				lastModified: new Date("2023-01-01T00:00:00Z"),
			};

			const object2: ObjectMetadata = {
				key: "test/file2.txt",
				size: 200,
				etag: "def456",
				lastModified: new Date("2023-01-02T00:00:00Z"),
			};

			// Add individual object
			engine.addObject(object1);

			// Add multiple objects
			engine.addObjects([object2]);

			// Detect changes (all should be new since there's no previous state)
			const changes = engine.detectChanges();

			// Verify both objects are detected as new
			const addedChanges = ChangeDetectionEngine.filterChangesByType(changes, [
				ChangeType.Added,
			]);

			expect(addedChanges.length).toBe(2);
			expect(
				addedChanges.find((c) => c.object.key === object1.key),
			).toBeTruthy();
			expect(
				addedChanges.find((c) => c.object.key === object2.key),
			).toBeTruthy();
		});

		it("should convert S3 objects correctly", () => {
			const s3Object = {
				key: "test/file.txt",
				size: 100,
				etag: '"abc123"', // With quotes as returned by S3
				lastModified: new Date("2023-01-01T00:00:00Z"),
			};

			const converted = ChangeDetectionEngine.fromS3Object(s3Object);

			expect(converted.key).toBe("test/file.txt");
			expect(converted.size).toBe(100);
			expect(converted.etag).toBe("abc123"); // Quotes should be removed
			expect(converted.lastModified).toEqual(new Date("2023-01-01T00:00:00Z"));
		});
	});

	describe("change detection", () => {
		it("should detect new files", () => {
			// Add objects to the current state
			engine.addObjects([
				{
					key: "test/file1.txt",
					size: 100,
					etag: "abc123",
					lastModified: new Date("2023-01-01T00:00:00Z"),
				},
				{
					key: "test/file2.txt",
					size: 200,
					etag: "def456",
					lastModified: new Date("2023-01-02T00:00:00Z"),
				},
			]);

			// Detect changes (all should be new)
			const changes = engine.detectChanges();

			// Should have 2 new files
			expect(changes.length).toBe(2);
			expect(changes.every((c) => c.changeType === ChangeType.Added)).toBe(
				true,
			);
		});

		it("should detect modified files", async () => {
			// Add initial state
			engine.addObjects([
				{
					key: "test/file1.txt",
					size: 100,
					etag: "abc123",
					lastModified: new Date("2023-01-01T00:00:00Z"),
				},
				{
					key: "test/file2.txt",
					size: 200,
					etag: "def456",
					lastModified: new Date("2023-01-02T00:00:00Z"),
				},
			]);

			// Commit the current state to make it the previous state
			engine.commitChanges();

			// Reset current state
			engine.resetCurrentState();

			// Add modified objects
			engine.addObjects([
				{
					key: "test/file1.txt",
					size: 150, // Changed size
					etag: "abc123",
					lastModified: new Date("2023-01-01T00:00:00Z"),
				},
				{
					key: "test/file2.txt",
					size: 200,
					etag: "def456",
					lastModified: new Date("2023-01-02T00:00:00Z"), // Unchanged
				},
				{
					key: "test/file3.txt", // New file
					size: 300,
					etag: "ghi789",
					lastModified: new Date("2023-01-03T00:00:00Z"),
				},
			]);

			// Detect changes
			const changes = engine.detectChanges();

			// Filter by type
			const addedChanges = ChangeDetectionEngine.filterChangesByType(changes, [
				ChangeType.Added,
			]);

			const modifiedChanges = ChangeDetectionEngine.filterChangesByType(
				changes,
				[ChangeType.Modified],
			);

			const unchangedChanges = ChangeDetectionEngine.filterChangesByType(
				changes,
				[ChangeType.Unchanged],
			);

			// Verify changes
			expect(addedChanges.length).toBe(1); // One new file
			expect(addedChanges[0].object.key).toBe("test/file3.txt");

			expect(modifiedChanges.length).toBe(1); // One modified file
			expect(modifiedChanges[0].object.key).toBe("test/file1.txt");
			expect(modifiedChanges[0].object.size).toBe(150); // New size
			expect(modifiedChanges[0].previousVersion?.size).toBe(100); // Old size

			expect(unchangedChanges.length).toBe(1); // One unchanged file
			expect(unchangedChanges[0].object.key).toBe("test/file2.txt");
		});

		it("should detect deleted files", () => {
			// Add initial state
			engine.addObjects([
				{
					key: "test/file1.txt",
					size: 100,
					etag: "abc123",
					lastModified: new Date("2023-01-01T00:00:00Z"),
				},
				{
					key: "test/file2.txt",
					size: 200,
					etag: "def456",
					lastModified: new Date("2023-01-02T00:00:00Z"),
				},
			]);

			// Commit the current state to make it the previous state
			engine.commitChanges();

			// Reset current state and add only one file (simulating deletion)
			engine.resetCurrentState();
			engine.addObject({
				key: "test/file1.txt",
				size: 100,
				etag: "abc123",
				lastModified: new Date("2023-01-01T00:00:00Z"),
			});

			// Detect changes
			const changes = engine.detectChanges();

			// Filter deleted changes
			const deletedChanges = ChangeDetectionEngine.filterChangesByType(
				changes,
				[ChangeType.Deleted],
			);

			// Verify deletions
			expect(deletedChanges.length).toBe(1);
			expect(deletedChanges[0].object.key).toBe("test/file2.txt");
		});

		it("should respect the compareMode option", () => {
			// Create an engine with quick compare mode
			const quickEngine = new ChangeDetectionEngine({
				stateFilePath: testStateFile,
				compareMode: "quick", // Only compares size and lastModified
				trackDeleted: true,
			});

			// Add initial state
			quickEngine.addObjects([
				{
					key: "test/file1.txt",
					size: 100,
					etag: "abc123",
					lastModified: new Date("2023-01-01T00:00:00Z"),
				},
			]);

			// Commit the current state to make it the previous state
			quickEngine.commitChanges();

			// Reset current state and add with changed etag but same size/date
			quickEngine.resetCurrentState();
			quickEngine.addObject({
				key: "test/file1.txt",
				size: 100, // Same size
				etag: "changed-etag", // Different etag
				lastModified: new Date("2023-01-01T00:00:00Z"), // Same date
			});

			// Detect changes with quick mode
			const changes = quickEngine.detectChanges();

			// Filter for modifications
			const modifiedChanges = ChangeDetectionEngine.filterChangesByType(
				changes,
				[ChangeType.Modified],
			);

			// In quick mode, the etag change shouldn't trigger a modification
			expect(modifiedChanges.length).toBe(0);

			// Now try with full compare mode (the default)
			engine.addObjects([
				{
					key: "test/file1.txt",
					size: 100,
					etag: "abc123",
					lastModified: new Date("2023-01-01T00:00:00Z"),
				},
			]);

			engine.commitChanges();
			engine.resetCurrentState();

			engine.addObject({
				key: "test/file1.txt",
				size: 100,
				etag: "changed-etag", // Different etag
				lastModified: new Date("2023-01-01T00:00:00Z"),
			});

			// In full mode, the etag change should trigger a modification
			const fullChanges = engine.detectChanges();
			const fullModifiedChanges = ChangeDetectionEngine.filterChangesByType(
				fullChanges,
				[ChangeType.Modified],
			);

			expect(fullModifiedChanges.length).toBe(1);
		});
	});

	describe("state persistence", () => {
		it("should save and load state", async () => {
			// Add objects to the current state
			engine.addObjects([
				{
					key: "test/file1.txt",
					size: 100,
					etag: "abc123",
					lastModified: new Date("2023-01-01T00:00:00Z"),
				},
				{
					key: "test/file2.txt",
					size: 200,
					etag: "def456",
					lastModified: new Date("2023-01-02T00:00:00Z"),
				},
			]);

			// Save the state
			await engine.saveCurrentState();

			// Create a new engine instance that should load the saved state
			const newEngine = new ChangeDetectionEngine({
				stateFilePath: testStateFile,
			});

			// Load the previous state
			await newEngine.loadPreviousState();

			// Add the same objects (should be unchanged)
			newEngine.addObjects([
				{
					key: "test/file1.txt",
					size: 100,
					etag: "abc123",
					lastModified: new Date("2023-01-01T00:00:00Z"),
				},
				{
					key: "test/file2.txt",
					size: 200,
					etag: "def456",
					lastModified: new Date("2023-01-02T00:00:00Z"),
				},
			]);

			// Detect changes
			const changes = newEngine.detectChanges();

			// Filter for unchanged files
			const unchangedChanges = ChangeDetectionEngine.filterChangesByType(
				changes,
				[ChangeType.Unchanged],
			);

			// Both files should be unchanged
			expect(unchangedChanges.length).toBe(2);
		});
	});
});

// S3 Integration Tests
// Note: These tests require an S3-compatible server like Minio running
describe("S3 Change Detection Integration", () => {
	// Create a unique bucket name for testing
	const bucketName = `s3pathmatcher-test-${randomUUID().substring(0, 8)}`;

	// Minio configuration based on docker-compose.yml
	const minioEndpoint = "http://localhost:9000";
	const minioAccessKey = "root"; // From docker-compose.yml MINIO_ROOT_USER
	const minioSecretKey = "password"; // From docker-compose.yml MINIO_ROOT_PASSWORD

	const testStateFile = path.join(
		process.cwd(),
		"test-state",
		"s3-changes.json",
	);

	// Test files to create
	const testFiles = [
		{
			key: "data/users/user1.json",
			content: JSON.stringify({ id: "user1", name: "John Doe" }),
		},
		{
			key: "data/users/user2.json",
			content: JSON.stringify({ id: "user2", name: "Jane Smith" }),
		},
	];

	// Create direct S3 client for setup/teardown
	const s3 = new S3Client({
		region: "us-east-1", // Region doesn't matter for Minio but is required
		endpoint: minioEndpoint,
		forcePathStyle: true,
		credentials: {
			accessKeyId: process.env.MINIO_ACCESS_KEY || minioAccessKey,
			secretAccessKey: process.env.MINIO_SECRET_KEY || minioSecretKey,
		},
	});

	// Create S3PathMatcher instance with properly typed config
	const matcher = new S3PathMatcher(
		{ dot: true },
		{
			region: "us-east-1",
			endpoint: minioEndpoint,
			forcePathStyle: true,
			credentials: {
				accessKeyId: process.env.MINIO_ACCESS_KEY || minioAccessKey,
				secretAccessKey: process.env.MINIO_SECRET_KEY || minioSecretKey,
			},
		},
	);

	// Create ChangeDetectionEngine
	let changeEngine: ChangeDetectionEngine;

	// Set up test bucket and objects
	beforeAll(async () => {
		try {
			// Create bucket
			// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
			await s3.send(new CreateBucketCommand({ Bucket: bucketName }));

			// Upload test files
			await Promise.all(
				testFiles.map((file) =>
					s3.send(
						new PutObjectCommand({
							// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
							Bucket: bucketName,
							// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
							Key: file.key,
							// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
							Body: file.content,
						}),
					),
				),
			);

			console.log(
				`Test bucket ${bucketName} created with ${testFiles.length} test objects`,
			);

			// Create test state directory
			await fs.mkdir(path.dirname(testStateFile), { recursive: true });
		} catch (error) {
			console.error("Error setting up test bucket:", error);
			throw error;
		}
	}, 30000); // 30 second timeout for setup

	// Clean up test bucket and objects
	afterAll(async () => {
		try {
			// Delete test objects
			await Promise.all(
				testFiles.map((file) =>
					s3.send(
						new DeleteObjectCommand({
							// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
							Bucket: bucketName,
							// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
							Key: file.key,
						}),
					),
				),
			);

			console.log(`Test objects deleted from bucket ${bucketName}`);

			// Clean up state file
			try {
				await fs.unlink(testStateFile);
				await fs.rmdir(path.dirname(testStateFile), { recursive: true });
			} catch (err) {
				// Might already be gone
			}
		} catch (error) {
			console.error("Error cleaning up test bucket:", error);
		}
	}, 30000); // 30 second timeout for cleanup

	it("should detect changes in S3 bucket content", async () => {
		// Initialize change engine
		changeEngine = new ChangeDetectionEngine({
			stateFilePath: testStateFile,
			compareMode: "full",
		});

		// Load previous state (should be empty first time)
		await changeEngine.loadPreviousState();

		// List objects and get metadata
		const response = await s3.send(
			new PutObjectCommand({
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
				Bucket: bucketName,
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
				Key: "data/users/user1.json",
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
				Body: JSON.stringify({ id: "user1", name: "UPDATED NAME" }),
			}),
		);

		// Get objects from S3
		const objects = await matcher.listObjects(bucketName);

		// This is a simplified approach - in real code, you'd fetch full object metadata
		const objectMetadata: ObjectMetadata[] = objects.map((key) => ({
			key,
			size: 100, // Example size
			etag: "test-etag", // Example etag
			lastModified: new Date(),
		}));

		// Add all objects to the current state
		changeEngine.addObjects(objectMetadata);

		// In first run, all files should be detected as new
		const firstRunChanges = changeEngine.detectChanges();
		const newFiles = ChangeDetectionEngine.filterChangesByType(
			firstRunChanges,
			[ChangeType.Added],
		);

		expect(newFiles.length).toBe(objects.length);

		// Save state
		await changeEngine.saveCurrentState();

		// Add a new file to the bucket
		await s3.send(
			new PutObjectCommand({
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
				Bucket: bucketName,
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
				Key: "data/users/user3.json",
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
				Body: JSON.stringify({ id: "user3", name: "New User" }),
			}),
		);

		// Create a new engine instance that should load the saved state
		const secondEngine = new ChangeDetectionEngine({
			stateFilePath: testStateFile,
		});

		// Load the previous state
		await secondEngine.loadPreviousState();

		// Get updated objects from S3
		const updatedObjects = await matcher.listObjects(bucketName);

		// Create updated metadata
		const updatedMetadata: ObjectMetadata[] = updatedObjects.map((key) => ({
			key,
			size: 100, // Example size
			etag: "test-etag", // Example etag
			lastModified: new Date(),
		}));

		// Add all updated objects to the current state
		secondEngine.addObjects(updatedMetadata);

		// Detect changes
		const secondRunChanges = secondEngine.detectChanges();

		// The new file should be detected as added
		const addedFiles = ChangeDetectionEngine.filterChangesByType(
			secondRunChanges,
			[ChangeType.Added],
		);

		expect(addedFiles.length).toBe(1);
		expect(addedFiles[0].object.key).toBe("data/users/user3.json");

		// Clean up the new file
		await s3.send(
			new DeleteObjectCommand({
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
				Bucket: bucketName,
				// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
				Key: "data/users/user3.json",
			}),
		);
	}, 30000);
});

describe("HivePartitionParser", () => {
	// Basic partition schema for most tests
	const datePartitionSchema = z.object({
		year: z.coerce.number().int().min(2000).max(2100),
		month: z.coerce.number().int().min(1).max(12),
		day: z.coerce.number().int().min(1).max(31),
	});

	const dateParser = new HivePartitionParser(datePartitionSchema);

	describe("constructor", () => {
		it("should create a parser with the given schema", () => {
			const parser = new HivePartitionParser(datePartitionSchema);
			expect(parser).toBeDefined();
		});
	});

	describe("parse", () => {
		it("should parse a valid path", () => {
			const result = dateParser.parse("/data/year=2023/month=12/day=25");
			expect(result).toEqual({ year: 2023, month: 12, day: 25 });
		});

		it("should parse values with leading zeros", () => {
			const result = dateParser.parse("/data/year=2023/month=01/day=05");
			expect(result).toEqual({ year: 2023, month: 1, day: 5 });
		});

		it("should ignore non-partition path segments", () => {
			const result = dateParser.parse(
				"/warehouse/mydb/table/year=2023/month=12/day=25",
			);
			expect(result).toEqual({ year: 2023, month: 12, day: 25 });
		});

		it("should throw on invalid values", () => {
			expect(() =>
				dateParser.parse("/data/year=2023/month=13/day=25"),
			).toThrow();
			expect(() =>
				dateParser.parse("/data/year=2023/month=12/day=32"),
			).toThrow();
			expect(() =>
				dateParser.parse("/data/year=1999/month=12/day=25"),
			).toThrow();
		});

		it("should throw on missing partition keys", () => {
			expect(() => dateParser.parse("/data/year=2023/month=12")).toThrow();
			expect(() => dateParser.parse("/data/year=2023/day=25")).toThrow();
		});
	});

	describe("safeParse", () => {
		it("should return success result for valid path", () => {
			const result = dateParser.safeParse("/data/year=2023/month=12/day=25");
			expect(result.success).toBe(true);
			if (result.success) {
				expect(result.data).toEqual({ year: 2023, month: 12, day: 25 });
			}
		});

		it("should return error result for invalid path", () => {
			const result = dateParser.safeParse("/data/year=2023/month=13/day=25");
			expect(result.success).toBe(false);
		});
	});

	describe("format", () => {
		it("should format partition data to a path", () => {
			const path = dateParser.format({ year: 2024, month: 3, day: 22 });
			expect(path).toBe("year=2024/month=3/day=22");
		});

		it("should validate data before formatting", () => {
			expect(() =>
				dateParser.format({
					year: 2024,
					month: 13,
					day: 22,
				} as z.infer<typeof datePartitionSchema>),
			).toThrow();
		});
	});

	describe("createGlobPattern", () => {
		it("should create a glob pattern with wildcards for unspecified fields", () => {
			const pattern = dateParser.createGlobPattern({ year: 2024, month: 3 });
			expect(pattern).toBe("year=2024/month=3/day=*");
		});

		it("should create a fully wildcarded pattern when no fields specified", () => {
			const pattern = dateParser.createGlobPattern({});
			expect(pattern).toBe("year=*/month=*/day=*");
		});

		it("should not validate the provided partial data", () => {
			// This would normally fail validation, but should work for glob patterns
			const pattern = dateParser.createGlobPattern({ year: 1999 });
			expect(pattern).toBe("year=1999/month=*/day=*");
		});
	});

	describe("isValid", () => {
		it("should return true for valid paths", () => {
			expect(dateParser.isValid("/data/year=2023/month=12/day=25")).toBe(true);
		});

		it("should return false for invalid paths", () => {
			expect(dateParser.isValid("/data/year=2023/month=13/day=25")).toBe(false);
			expect(dateParser.isValid("/data/year=2023/month=12")).toBe(false);
		});
	});

	describe("getValidationErrors", () => {
		it("should return empty array for valid paths", () => {
			const errors = dateParser.getValidationErrors(
				"/data/year=2023/month=12/day=25",
			);
			expect(errors).toEqual([]);
		});

		it("should return error messages for invalid paths", () => {
			const errors = dateParser.getValidationErrors(
				"/data/year=2023/month=13/day=32",
			);
			expect(errors.length).toBeGreaterThan(0);
			expect(errors[0]).toContain("month");
			expect(errors[1]).toContain("day");
		});
	});

	describe("getMissingKeys", () => {
		it("should return empty array when all keys present", () => {
			const missing = dateParser.getMissingKeys(
				"/data/year=2023/month=12/day=25",
			);
			expect(missing).toEqual([]);
		});

		it("should return missing key names", () => {
			const missing = dateParser.getMissingKeys("/data/year=2023/month=12");
			expect(missing).toEqual(["day"]);
		});
	});

	describe("extractKeys", () => {
		it("should extract only specified keys", () => {
			const extracted = dateParser.extractKeys(
				"/data/year=2023/month=12/day=25",
				["year", "month"],
			);
			expect(extracted).toEqual({ year: 2023, month: 12 });
		});

		it("should throw for invalid paths", () => {
			expect(() =>
				dateParser.extractKeys("/data/year=2023/month=13/day=25", [
					"year",
					"month",
				]),
			).toThrow();
		});
	});

	describe("transform", () => {
		it("should apply transformations to partition values", () => {
			// Expect a validation error to be thrown when attempting to format
			// an invalid transformation (month=13)
			expect(() =>
				dateParser.transform("/data/year=2023/month=11/day=25", (data) => ({
					month: data.month + 2,
				})),
			).toThrow();
		});

		it("should apply valid transformations", () => {
			const transformed = dateParser.transform(
				"/data/year=2023/month=11/day=25",
				(data) => ({ month: data.month + 1 }),
			);

			const result = dateParser.parse(transformed);
			expect(result).toEqual({ year: 2023, month: 12, day: 25 });
		});
	});

	describe("matchesGlob", () => {
		it("should match exact paths", () => {
			expect(
				dateParser.matchesGlob(
					"year=2023/month=12/day=25",
					"year=2023/month=12/day=25",
				),
			).toBe(true);
		});

		it("should match paths with wildcards", () => {
			expect(
				dateParser.matchesGlob(
					"year=2023/month=12/day=25",
					"year=2023/month=*/day=*",
				),
			).toBe(true);
		});

		it("should not match paths with different segment counts", () => {
			expect(
				dateParser.matchesGlob(
					"year=2023/month=12/day=25",
					"year=2023/month=12",
				),
			).toBe(false);
		});

		it("should not match paths with non-matching segments", () => {
			expect(
				dateParser.matchesGlob(
					"year=2023/month=12/day=25",
					"year=2024/month=*/day=*",
				),
			).toBe(false);
		});
	});

	// Test with a more complex schema
	describe("complex schema", () => {
		const analyticsSchema = z.object({
			region: z.enum(["us-east", "us-west", "eu", "asia"]),
			service: z.string().min(1),
			year: z.coerce.number().int().min(2000),
			month: z.coerce.number().int().min(1).max(12),
			eventType: z.enum(["click", "view", "purchase", "error"]),
		});

		const analyticsParser = new HivePartitionParser(analyticsSchema);

		it("should parse complex paths", () => {
			const result = analyticsParser.parse(
				"/analytics/region=us-east/service=checkout/year=2023/month=12/eventType=purchase",
			);

			expect(result).toEqual({
				region: "us-east",
				service: "checkout",
				year: 2023,
				month: 12,
				eventType: "purchase",
			});
		});

		it("should validate enum values", () => {
			expect(() =>
				analyticsParser.parse(
					"/analytics/region=invalid/service=checkout/year=2023/month=12/event_type=purchase",
				),
			).toThrow();
		});
	});

	// Test with optional and nullable fields
	describe("optional fields", () => {
		const logSchema = z.object({
			app: z.string(),
			environment: z.enum(["dev", "test", "staging", "prod"]),
			date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
			level: z.enum(["INFO", "WARN", "ERROR", "DEBUG"]).optional(),
			instance: z
				.string()
				.nullable()
				.transform((v) => (v === "null" ? null : v)),
		});

		const logParser = new HivePartitionParser(logSchema);

		it("should parse with optional fields present", () => {
			const result = logParser.parse(
				"/logs/app=api/environment=prod/date=2023-12-25/level=ERROR/instance=server01",
			);

			expect(result).toEqual({
				app: "api",
				environment: "prod",
				date: "2023-12-25",
				level: "ERROR",
				instance: "server01",
			});
		});

		it("should parse with optional fields missing", () => {
			const result = logParser.parse(
				"/logs/app=api/environment=prod/date=2023-12-25/instance=server01",
			);

			expect(result).toEqual({
				app: "api",
				environment: "prod",
				date: "2023-12-25",
				instance: "server01",
			});
		});

		it("should parse null values", () => {
			const result = logParser.parse(
				"/logs/app=api/environment=prod/date=2023-12-25/level=ERROR/instance=null",
			);

			expect(result).toEqual({
				app: "api",
				environment: "prod",
				date: "2023-12-25",
				level: "ERROR",
				instance: null,
			});
		});
	});
});

describe("TimePartitionGenerator", () => {
	describe("constructor", () => {
		it("should create a generator with default options", () => {
			const generator = new TimePartitionGenerator({
				granularity: TimeGranularity.Daily,
			});
			expect(generator).toBeDefined();
		});

		it("should set includeHour to true when includeMinute is true", () => {
			const generator = new TimePartitionGenerator({
				granularity: TimeGranularity.Daily,
				includeMinute: true,
			});

			// Testing internal state via the generatePath output
			const path = generator.generatePath(new Date(2023, 0, 1, 14, 30));
			expect(path).toContain("hour=14");
			expect(path).toContain("minute=30");
		});

		it("should set includeHour to true for hourly granularity", () => {
			const generator = new TimePartitionGenerator({
				granularity: TimeGranularity.Hourly,
			});

			// Testing internal state via the generatePath output
			const path = generator.generatePath(new Date(2023, 0, 1, 14, 30));
			expect(path).toContain("hour=14");
			expect(path).not.toContain("minute=30");
		});
	});

	describe("generatePath", () => {
		it("should generate a basic daily path in hive format", () => {
			const generator = new TimePartitionGenerator({
				granularity: TimeGranularity.Daily,
			});

			const date = new Date(2023, 0, 1); // Jan 1, 2023
			const path = generator.generatePath(date);

			expect(path).toBe("year=2023/month=01/day=01");
		});

		it("should include a prefix when specified", () => {
			const generator = new TimePartitionGenerator({
				granularity: TimeGranularity.Daily,
				prefix: "data",
			});

			const date = new Date(2023, 0, 1);
			const path = generator.generatePath(date);

			expect(path).toBe("data/year=2023/month=01/day=01");
		});

		it("should generate paths in path format", () => {
			const generator = new TimePartitionGenerator({
				granularity: TimeGranularity.Daily,
				format: "path",
			});

			const date = new Date(2023, 0, 1);
			const path = generator.generatePath(date);

			expect(path).toBe("2023/01/01");
		});

		it("should include hour when specified", () => {
			const generator = new TimePartitionGenerator({
				granularity: TimeGranularity.Daily,
				includeHour: true,
			});

			const date = new Date(2023, 0, 1, 14, 30);
			const path = generator.generatePath(date);

			expect(path).toBe("year=2023/month=01/day=01/hour=14");
		});

		it("should include minute when specified", () => {
			const generator = new TimePartitionGenerator({
				granularity: TimeGranularity.Daily,
				includeMinute: true,
				includeHour: true,
			});

			const date = new Date(2023, 0, 1, 14, 30);
			const path = generator.generatePath(date);

			expect(path).toBe("year=2023/month=01/day=01/hour=14/minute=30");
		});

		it("should generate monthly paths", () => {
			const generator = new TimePartitionGenerator({
				granularity: TimeGranularity.Monthly,
			});

			const date = new Date(2023, 0, 15); // Jan 15, 2023
			const path = generator.generatePath(date);

			expect(path).toBe("year=2023/month=01");
			expect(path).not.toContain("day=");
		});

		it("should generate yearly paths", () => {
			const generator = new TimePartitionGenerator({
				granularity: TimeGranularity.Yearly,
			});

			const date = new Date(2023, 5, 15); // Jun 15, 2023
			const path = generator.generatePath(date);

			expect(path).toBe("year=2023");
			expect(path).not.toContain("month=");
			expect(path).not.toContain("day=");
		});

		it("should generate hourly paths", () => {
			const generator = new TimePartitionGenerator({
				granularity: TimeGranularity.Hourly,
			});

			const date = new Date(2023, 0, 1, 14, 30);
			const path = generator.generatePath(date);

			expect(path).toBe("year=2023/month=01/day=01/hour=14");
		});

		it("should use the current date when no date is provided", () => {
			const generator = new TimePartitionGenerator({
				granularity: TimeGranularity.Daily,
			});

			const now = new Date();
			const year = now.getFullYear().toString();
			const month = (now.getMonth() + 1).toString().padStart(2, "0");
			const day = now.getDate().toString().padStart(2, "0");

			const path = generator.generatePath();

			expect(path).toBe(`year=${year}/month=${month}/day=${day}`);
		});
	});

	describe("generatePathsForRange", () => {
		it("should generate an array of daily paths for a date range", () => {
			const generator = new TimePartitionGenerator({
				granularity: TimeGranularity.Daily,
			});

			const startDate = new Date(2023, 0, 1); // Jan 1, 2023
			const endDate = new Date(2023, 0, 3); // Jan 3, 2023

			const paths = generator.generatePathsForRange(startDate, endDate);

			expect(paths).toHaveLength(3);
			expect(paths[0]).toBe("year=2023/month=01/day=01");
			expect(paths[1]).toBe("year=2023/month=01/day=02");
			expect(paths[2]).toBe("year=2023/month=01/day=03");
		});

		it("should generate an array of monthly paths for a date range", () => {
			const generator = new TimePartitionGenerator({
				granularity: TimeGranularity.Monthly,
			});

			const startDate = new Date(2023, 0, 15); // Jan 15, 2023
			const endDate = new Date(2023, 2, 15); // Mar 15, 2023

			const paths = generator.generatePathsForRange(startDate, endDate);

			expect(paths).toHaveLength(3);
			expect(paths[0]).toBe("year=2023/month=01");
			expect(paths[1]).toBe("year=2023/month=02");
			expect(paths[2]).toBe("year=2023/month=03");
		});

		it("should generate an array of yearly paths for a date range", () => {
			const generator = new TimePartitionGenerator({
				granularity: TimeGranularity.Yearly,
			});

			const startDate = new Date(2022, 0, 1); // Jan 1, 2022
			const endDate = new Date(2024, 0, 1); // Jan 1, 2024

			const paths = generator.generatePathsForRange(startDate, endDate);

			expect(paths).toHaveLength(3);
			expect(paths[0]).toBe("year=2022");
			expect(paths[1]).toBe("year=2023");
			expect(paths[2]).toBe("year=2024");
		});

		it("should generate an array of hourly paths for a date range", () => {
			const generator = new TimePartitionGenerator({
				granularity: TimeGranularity.Hourly,
			});

			const startDate = new Date(2023, 0, 1, 10, 0); // Jan 1, 2023, 10:00
			const endDate = new Date(2023, 0, 1, 12, 0); // Jan 1, 2023, 12:00

			const paths = generator.generatePathsForRange(startDate, endDate);

			expect(paths).toHaveLength(3);
			expect(paths[0]).toBe("year=2023/month=01/day=01/hour=10");
			expect(paths[1]).toBe("year=2023/month=01/day=01/hour=11");
			expect(paths[2]).toBe("year=2023/month=01/day=01/hour=12");
		});

		it("should handle single-day ranges correctly", () => {
			const generator = new TimePartitionGenerator({
				granularity: TimeGranularity.Daily,
			});

			const date = new Date(2023, 0, 1);
			const paths = generator.generatePathsForRange(date, date);

			expect(paths).toHaveLength(1);
			expect(paths[0]).toBe("year=2023/month=01/day=01");
		});
	});

	describe("utility methods", () => {
		it("should generate current path", () => {
			const generator = new TimePartitionGenerator({
				granularity: TimeGranularity.Daily,
			});

			const now = new Date();
			const year = now.getFullYear().toString();
			const month = (now.getMonth() + 1).toString().padStart(2, "0");
			const day = now.getDate().toString().padStart(2, "0");

			const path = generator.generateCurrentPath();

			expect(path).toBe(`year=${year}/month=${month}/day=${day}`);
		});

		it("should generate yesterday's path", () => {
			const generator = new TimePartitionGenerator({
				granularity: TimeGranularity.Daily,
			});

			const yesterday = new Date();
			yesterday.setDate(yesterday.getDate() - 1);
			const year = yesterday.getFullYear().toString();
			const month = (yesterday.getMonth() + 1).toString().padStart(2, "0");
			const day = yesterday.getDate().toString().padStart(2, "0");

			const path = generator.generatePath(yesterday);

			expect(path).toBe(`year=${year}/month=${month}/day=${day}`);
		});
	});
});

// Add tests for Rehiver class
describe("Rehiver", () => {
	// Minio configuration based on docker-compose.yml
	const minioEndpoint = "http://localhost:9000";
	const minioAccessKey = "root"; // From docker-compose.yml MINIO_ROOT_USER
	const minioSecretKey = "password"; // From docker-compose.yml MINIO_ROOT_PASSWORD

	// Create a unique bucket name for testing
	const bucketName = `rehiver-test-${randomUUID().substring(0, 8)}`;

	// Create S3 client for setup/teardown
	const s3 = new S3Client({
		region: "us-east-1", // Region doesn't matter for Minio but is required
		endpoint: minioEndpoint,
		forcePathStyle: true,
		credentials: {
			accessKeyId: process.env.MINIO_ACCESS_KEY || minioAccessKey,
			secretAccessKey: process.env.MINIO_SECRET_KEY || minioSecretKey,
		},
	});

	// Test files to create
	const testFiles = [
		{
			key: "data/users/user1.json",
			content: JSON.stringify({ id: "user1", name: "John Doe" }),
		},
		{
			key: "data/users/user2.json",
			content: JSON.stringify({ id: "user2", name: "Jane Smith" }),
		},
	];

	// Set up test bucket and objects
	beforeAll(async () => {
		try {
			// Create bucket
			// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
			await s3.send(new CreateBucketCommand({ Bucket: bucketName }));

			// Upload test files
			await Promise.all(
				testFiles.map((file) =>
					s3.send(
						new PutObjectCommand({
							// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
							Bucket: bucketName,
							// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
							Key: file.key,
							// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
							Body: file.content,
						}),
					),
				),
			);

			console.log(
				`Test bucket ${bucketName} created with ${testFiles.length} test objects`,
			);
		} catch (error) {
			console.error("Error setting up test bucket:", error);
			throw error;
		}
	}, 30000); // 30 second timeout for setup

	// Clean up test bucket and objects
	afterAll(async () => {
		try {
			// Delete test objects
			await Promise.all(
				testFiles.map((file) =>
					s3.send(
						new DeleteObjectCommand({
							// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
							Bucket: bucketName,
							// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
							Key: file.key,
						}),
					),
				),
			);

			console.log(`Test objects deleted from bucket ${bucketName}`);
		} catch (error) {
			console.error("Error cleaning up test bucket:", error);
		}
	}, 30000); // 30 second timeout for cleanup

	it("should create buckets with createBucketIfNotExists", async () => {
		// Generate unique bucket names for this test
		const testBucket1 = `rehiver-create-test-${randomUUID().substring(0, 8)}`;
		const testBucket2 = `rehiver-create-test-${randomUUID().substring(0, 8)}`;

		try {
			// Create a Rehiver instance
			const rehiver = new Rehiver({
				s3Options: {
					region: "us-east-1",
					endpoint: minioEndpoint,
					forcePathStyle: true,
					credentials: {
						accessKeyId: process.env.MINIO_ACCESS_KEY || minioAccessKey,
						secretAccessKey: process.env.MINIO_SECRET_KEY || minioSecretKey,
					},
				},
			});

			// Test creating a bucket with default options
			const created1 = await rehiver.createBucket(testBucket1);

			// Should return true when bucket is created
			expect(created1).toBe(true);

			// Create the same bucket again - should return false
			const created1Again = await rehiver.createBucket(testBucket1);

			// Should return false when bucket already exists
			expect(created1Again).toBe(false);

			// Test creating a bucket with ACL options
			const created2 = await rehiver.createBucket(testBucket2, {
				acl: "public-read", // Set a public-read ACL
			});

			// Should return true when bucket is created
			expect(created2).toBe(true);
		} finally {
			// Clean up the test buckets we created
			try {
				// Delete bucket 1 if it exists
				// Note: In a real-world scenario, you would need to empty the bucket first
				// For this test, we're assuming the buckets are empty
				await s3
					.send(
						new DeleteObjectCommand({
							// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
							Bucket: testBucket1,
							// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
							Key: "dummy-key",
						}),
					)
					.catch(() => {}); // Ignore errors if the key doesn't exist

				// Delete bucket 2 if it exists
				await s3
					.send(
						new DeleteObjectCommand({
							// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
							Bucket: testBucket2,
							// biome-ignore lint/style/useNamingConvention: AWS SDK uses PascalCase for API parameters
							Key: "dummy-key",
						}),
					)
					.catch(() => {}); // Ignore errors if the key doesn't exist

				console.log(
					`Test buckets ${testBucket1} and ${testBucket2} cleaned up`,
				);
			} catch (error) {
				console.error("Error cleaning up test buckets:", error);
			}
		}
	}, 30000); // Increase timeout for this test

	it("should find matching objects with named import", async () => {
		// Create an instance of Rehiver using named import
		const rehiver = new Rehiver({
			s3Options: {
				region: "us-east-1",
				endpoint: minioEndpoint,
				forcePathStyle: true,
				credentials: {
					accessKeyId: process.env.MINIO_ACCESS_KEY || minioAccessKey,
					secretAccessKey: process.env.MINIO_SECRET_KEY || minioSecretKey,
				},
			},
		});

		// Use findMatchingObjects to find JSON files
		const objects = await rehiver.findMatchingObjects(bucketName, "**/*.json");

		// Verify all test files are found
		expect(objects.length).toBe(testFiles.length);
		expect(objects).toContain("data/users/user1.json");
		expect(objects).toContain("data/users/user2.json");
	});

	it("should find matching objects with default import", async () => {
		// Create an instance of Rehiver using default import
		const rehiver = new DefaultRehiver({
			s3Options: {
				region: "us-east-1",
				endpoint: minioEndpoint,
				forcePathStyle: true,
				credentials: {
					accessKeyId: process.env.MINIO_ACCESS_KEY || minioAccessKey,
					secretAccessKey: process.env.MINIO_SECRET_KEY || minioSecretKey,
				},
			},
		});

		// Use findMatchingObjects to find JSON files
		const objects = await rehiver.findMatchingObjects(bucketName, "**/*.json");

		// Verify all test files are found
		expect(objects.length).toBe(testFiles.length);
		expect(objects).toContain("data/users/user1.json");
		expect(objects).toContain("data/users/user2.json");
	});

	it("should find matching objects with object-based API", async () => {
		// Create an instance of Rehiver using default import
		const rehiver = new DefaultRehiver({
			s3Options: {
				region: "us-east-1",
				endpoint: minioEndpoint,
				forcePathStyle: true,
				credentials: {
					accessKeyId: process.env.MINIO_ACCESS_KEY || minioAccessKey,
					secretAccessKey: process.env.MINIO_SECRET_KEY || minioSecretKey,
				},
			},
		});

		// Use findMatchingObjects with object-based API
		const objects = await rehiver.findMatchingObjects({
			bucket: bucketName,
			patterns: "**/*.json",
		});

		// Verify all test files are found
		expect(objects.length).toBe(testFiles.length);
		expect(objects).toContain("data/users/user1.json");
		expect(objects).toContain("data/users/user2.json");
	});

	it("should stream matching objects with object-based API", async () => {
		// Create an instance of Rehiver using default import
		const rehiver = new DefaultRehiver({
			s3Options: {
				region: "us-east-1",
				endpoint: minioEndpoint,
				forcePathStyle: true,
				credentials: {
					accessKeyId: process.env.MINIO_ACCESS_KEY || minioAccessKey,
					secretAccessKey: process.env.MINIO_SECRET_KEY || minioSecretKey,
				},
			},
		});

		// Create a processor function
		const processedFiles: string[] = [];
		const processor = async (path: string) => {
			processedFiles.push(path);
		};

		// Use streamMatchingObjects with object-based API
		const result = await rehiver.streamMatchingObjects({
			bucket: bucketName,
			patterns: "**/*.json",
			processor,
		});

		// Verify all test files are processed
		expect(result.matched).toBe(testFiles.length);
		expect(result.processed).toBe(testFiles.length);
		expect(processedFiles.length).toBe(testFiles.length);
		expect(processedFiles).toContain("data/users/user1.json");
		expect(processedFiles).toContain("data/users/user2.json");
	});

	it("should create and use a partitionParser through factory method", async () => {
		const rehiver = new Rehiver();

		// Define partition schema
		const partitionSchema = z.object({
			year: z.coerce.number().int().min(2000).max(2100),
			month: z.coerce.number().int().min(1).max(12),
			day: z.coerce.number().int().min(1).max(31),
		});

		// Create partition parser through the rehiver factory method
		const parser = rehiver.partitionParser(partitionSchema);

		// Test parsing
		const result = parser.parse("year=2023/month=12/day=25");
		expect(result).toEqual({ year: 2023, month: 12, day: 25 });

		// Test pattern creation
		const pattern = parser.createGlobPattern({ year: 2023, month: 7 });
		expect(pattern).toBe("year=2023/month=7/day=*");
	});

	it("should create and use a timePartitioner through factory method", async () => {
		const rehiver = new Rehiver();

		// Create time partitioner through the rehiver factory method
		const timeGen = rehiver.timePartitioner({
			granularity: TimeGranularity.Daily,
			format: "hive",
		});

		// Test path generation
		const date = new Date(2023, 0, 1); // Jan 1, 2023
		const path = timeGen.generatePath(date);
		expect(path).toBe("year=2023/month=01/day=01");

		// Test range generation
		const startDate = new Date(2023, 0, 1); // Jan 1, 2023
		const endDate = new Date(2023, 0, 3); // Jan 3, 2023
		const paths = timeGen.generatePathsForRange(startDate, endDate);
		expect(paths).toHaveLength(3);
		expect(paths[0]).toBe("year=2023/month=01/day=01");
		expect(paths[1]).toBe("year=2023/month=01/day=02");
		expect(paths[2]).toBe("year=2023/month=01/day=03");
	});

	it("should create and use a changeDetector through factory method", async () => {
		const rehiver = new Rehiver();

		// Create change detector through the rehiver factory method
		const detector = rehiver.changeDetector({
			compareMode: "full",
			trackDeleted: true,
		});

		// Add objects to the current state
		detector.addObjects([
			{
				key: "test/file1.txt",
				size: 100,
				etag: "abc123",
				lastModified: new Date("2023-01-01T00:00:00Z"),
			},
			{
				key: "test/file2.txt",
				size: 200,
				etag: "def456",
				lastModified: new Date("2023-01-02T00:00:00Z"),
			},
		]);

		// Detect changes (all should be new)
		const changes = detector.detectChanges();
		expect(changes.length).toBe(2);
		expect(changes.every((c) => c.changeType === ChangeType.Added)).toBe(true);
	});

	it("should provide path matching functionality directly", async () => {
		const rehiver = new Rehiver();

		// Test isMatch
		expect(rehiver.isMatch("foo/bar.txt", "**/*.txt")).toBe(true);
		expect(rehiver.isMatch("foo/bar.json", "**/*.txt")).toBe(false);

		// Test match
		const paths = ["foo/bar.txt", "foo/baz.json", "bar/qux.png", "root.txt"];
		expect(rehiver.match(paths, "**/*.txt")).toEqual([
			"foo/bar.txt",
			"root.txt",
		]);

		// Test matchFast
		expect(rehiver.matchFast(paths, "**/*.txt")).toEqual([
			"foo/bar.txt",
			"root.txt",
		]);

		// Test not
		expect(rehiver.not(paths, "**/*.txt")).toEqual([
			"foo/baz.json",
			"bar/qux.png",
		]);

		// Test capture
		expect(
			rehiver.capture("users/:id/posts/:postId", "users/123/posts/456"),
		).toEqual(["123", "456"]);
	});
});
