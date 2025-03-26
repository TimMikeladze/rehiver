import * as fs from "node:fs/promises";
import * as path from "node:path";
import { ChangeDetectionEngine, ChangeType, Rehiver } from "../src";

// Create a new Rehiver instance
const rehiver = new Rehiver();

async function runChangeDetectionExample() {
	// Create a temporary directory for our state file
	const tempDir = path.join(process.cwd(), "temp");
	await fs.mkdir(tempDir, { recursive: true });

	// Initialize a change detection engine with a state file
	const stateFilePath = path.join(tempDir, "change-state.json");
	const changeDetector = rehiver.changeDetector({
		stateFilePath,
		compareMode: "full",
		trackDeleted: true,
	});

	console.log("Change detection example:");

	// Create some example objects (simulating S3 objects)
	const exampleObjects = [
		{
			key: "data/2023/01/file1.csv",
			size: 1024,
			etag: "123456",
			lastModified: new Date("2023-01-15T10:00:00Z"),
			contentType: "text/csv",
		},
		{
			key: "data/2023/01/file2.json",
			size: 512,
			etag: "abcdef",
			lastModified: new Date("2023-01-15T11:00:00Z"),
			contentType: "application/json",
		},
	];

	// Add initial objects (first run, everything is new)
	console.log("\nAdding initial objects:");
	changeDetector.addObjects(exampleObjects);

	// Detect changes - all should be "added" since this is the first run
	const initialChanges = changeDetector.detectChanges();
	console.log("Initial changes:");
	console.log(JSON.stringify(initialChanges, null, 2));

	// Commit these changes to make them the "previous state"
	changeDetector.commitChanges();

	// Now simulate the next scan with some changes
	console.log("\nPerforming second scan with changes:");

	// Create modified and new objects
	const updatedObjects = [
		{
			// Modified object (changed size and etag)
			key: "data/2023/01/file1.csv",
			size: 2048, // Size changed
			etag: "789012", // ETag changed
			lastModified: new Date("2023-01-15T12:00:00Z"), // Time changed
			contentType: "text/csv",
		},
		{
			// Unchanged object
			key: "data/2023/01/file2.json",
			size: 512,
			etag: "abcdef",
			lastModified: new Date("2023-01-15T11:00:00Z"),
			contentType: "application/json",
		},
		{
			// New object
			key: "data/2023/01/file3.xml",
			size: 768,
			etag: "fedcba",
			lastModified: new Date("2023-01-15T13:00:00Z"),
			contentType: "application/xml",
		},
	];

	// Reset the current state before adding the updated objects
	changeDetector.resetCurrentState();

	// Add the updated objects
	changeDetector.addObjects(updatedObjects);

	// Detect changes
	const secondChanges = changeDetector.detectChanges();
	console.log("Changes detected in second scan:");
	console.log(JSON.stringify(secondChanges, null, 2));

	// Filter to show only added and modified objects
	console.log("\nFiltering to show only added and modified objects:");
	const filteredChanges = ChangeDetectionEngine.filterChangesByType(
		secondChanges,
		[ChangeType.Added, ChangeType.Modified],
	);
	console.log(JSON.stringify(filteredChanges, null, 2));

	// Clean up (delete temp directory)
	try {
		await fs.rm(tempDir, { recursive: true, force: true });
		console.log("\nCleanup completed.");
	} catch (error) {
		console.error("Error during cleanup:", error);
	}

	console.log("\nChange detection example completed.");
}

// Run the example
runChangeDetectionExample().catch((error) => {
	console.error("Error running change detection example:", error);
});
