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
import {
	ChangeDetectionEngine,
	ChangeType,
	type ObjectMetadata,
	PathMatcher,
	S3PathMatcher,
} from "../src/index";

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
