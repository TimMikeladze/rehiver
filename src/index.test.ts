import { randomUUID } from "node:crypto";
import {
	CreateBucketCommand,
	DeleteObjectCommand,
	PutObjectCommand,
	S3Client,
} from "@aws-sdk/client-s3";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { PathMatcher, S3PathMatcher } from "./index";

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

// S3 Integration Tests
// Note: These tests require actual AWS credentials and will create and delete real objects in S3
describe("S3PathMatcher Integration Tests", () => {
	// Create a unique bucket name for testing
	const bucketName = `s3pathmatcher-test-${randomUUID().substring(0, 8)}`;
	const localEndpoint = "http://localhost:4566"; // For LocalStack if available

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
		{
			key: "data/posts/post1.json",
			content: JSON.stringify({ id: "post1", title: "First Post" }),
		},
		{
			key: "data/posts/post2.json",
			content: JSON.stringify({ id: "post2", title: "Second Post" }),
		},
		{ key: "data/images/image1.png", content: "fake-image-data-1" },
		{ key: "data/images/image2.png", content: "fake-image-data-2" },
		{ key: "logs/2023/01/15/app.log", content: "log entry 1\nlog entry 2" },
		{ key: "logs/2023/01/16/app.log", content: "log entry 3\nlog entry 4" },
		{ key: "logs/2023/02/01/app.log", content: "log entry 5\nlog entry 6" },
		{ key: "archive/backup-2023-01.zip", content: "backup-data-1" },
		{ key: "archive/backup-2023-02.zip", content: "backup-data-2" },
	];

	// Create direct S3 client for setup/teardown
	const s3 = new S3Client({
		region: "us-east-1",
		endpoint: process.env.CI ? undefined : localEndpoint,
		forcePathStyle: true,
		credentials: {
			accessKeyId: process.env.AWS_ACCESS_KEY_ID || "test",
			secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "test",
		},
	});

	// Create S3PathMatcher instance
	const matcher = new S3PathMatcher(
		{ dot: true },
		{
			region: "us-east-1",
			// Only set endpoint for local development
			...(process.env.CI ? {} : { endpoint: localEndpoint }),
		},
	);

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

			// Delete bucket (would normally require ListObjectVersions + DeleteObjectVersion
			// but we're keeping it simple for the test)

			console.log(`Test objects deleted from bucket ${bucketName}`);
		} catch (error) {
			console.error("Error cleaning up test bucket:", error);
		}
	}, 30000); // 30 second timeout for cleanup

	describe("listObjects", () => {
		it("should list all objects in the bucket", async () => {
			const objects = await matcher.listObjects(bucketName);
			expect(objects.length).toBe(testFiles.length);
			for (const file of testFiles) {
				expect(objects).toContain(file.key);
			}
		}, 10000);

		it("should list objects with a prefix", async () => {
			const objects = await matcher.listObjects(bucketName, "data/");
			expect(objects.length).toBe(6); // Only data/ files

			for (const key of objects) {
				expect(key.startsWith("data/")).toBe(true);
			}
		}, 10000);
	});

	describe("findMatchingObjects", () => {
		it("should find objects matching a pattern", async () => {
			const jsonFiles = await matcher.findMatchingObjects(
				bucketName,
				"**/*.json",
			);
			expect(jsonFiles.length).toBe(4); // 4 JSON files
			for (const key of jsonFiles) {
				expect(key.endsWith(".json")).toBe(true);
			}
		}, 10000);

		it("should find objects with multiple patterns", async () => {
			const mediaFiles = await matcher.findMatchingObjects(bucketName, [
				"**/*.png",
				"**/*.zip",
			]);
			expect(mediaFiles.length).toBe(4); // 2 PNG + 2 ZIP files
			for (const key of mediaFiles) {
				expect(key.endsWith(".png") || key.endsWith(".zip")).toBe(true);
			}
		}, 10000);

		it("should use negation to exclude patterns", async () => {
			const nonJsonFiles = await matcher.findMatchingObjects(
				bucketName,
				"**/*.json",
				{
					useNegation: true,
				},
			);

			expect(nonJsonFiles.length).toBe(testFiles.length - 4); // All files except the 4 JSON files
			for (const key of nonJsonFiles) {
				expect(key.endsWith(".json")).toBe(false);
			}
		}, 10000);

		it("should report progress when callback is provided", async () => {
			let lastStats = { processed: 0, total: 0, matched: 0 };

			const logFiles = await matcher.findMatchingObjects(
				bucketName,
				"**/app.log",
				{
					onProgress: (stats) => {
						lastStats = stats;
						// Verify progress stats are increasing
						expect(stats.processed).toBeGreaterThanOrEqual(0);
					},
				},
			);

			expect(logFiles.length).toBe(3); // 3 log files
			expect(lastStats.processed).toBeGreaterThan(0);
			expect(lastStats.matched).toBe(3);
		}, 10000);
	});

	describe("streamMatchingObjects", () => {
		it("should process matching objects", async () => {
			const processedKeys: string[] = [];

			const stats = await matcher.streamMatchingObjects(
				bucketName,
				"**/*.json",
				async (path) => {
					processedKeys.push(path);
				},
			);

			expect(stats.processed).toBe(testFiles.length);
			expect(stats.matched).toBe(4); // 4 JSON files
			expect(stats.skipped).toBe(testFiles.length - 4);
			expect(processedKeys.length).toBe(4);
			for (const key of processedKeys) {
				expect(key.endsWith(".json")).toBe(true);
			}
		}, 10000);
	});

	describe("groupObjectsByCapture", () => {
		it("should group objects by capture pattern", async () => {
			// Group log files by year/month
			const logGroups = await matcher.groupObjectsByCapture(
				bucketName,
				"logs/:year/:month/**/*.log",
				{
					prefix: "logs/",
					groupKeyFn: (captures) => `${captures[0]}-${captures[1]}`, // Group by "year-month"
				},
			);

			expect(Object.keys(logGroups).length).toBe(2); // Two groups: 2023-01 and 2023-02
			expect(logGroups["2023-01"].length).toBe(2); // Two logs in January
			expect(logGroups["2023-02"].length).toBe(1); // One log in February

			// Check specific keys
			expect(logGroups["2023-01"]).toContain("logs/2023/01/15/app.log");
			expect(logGroups["2023-01"]).toContain("logs/2023/01/16/app.log");
			expect(logGroups["2023-02"]).toContain("logs/2023/02/01/app.log");
		}, 10000);
	});
});
