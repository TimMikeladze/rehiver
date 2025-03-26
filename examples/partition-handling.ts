import { z } from "zod";
import { Rehiver, TimeGranularity } from "../src";

// Create a new Rehiver instance
const rehiver = new Rehiver();

// Create a partition schema using zod
const partitionSchema = z.object({
	year: z.string().regex(/^\d{4}$/),
	month: z.string().regex(/^\d{2}$/),
	day: z.string().regex(/^\d{2}$/),
	hour: z
		.string()
		.regex(/^\d{2}$/)
		.optional(),
	region: z.enum(["us-east", "us-west", "eu-west", "ap-south"]),
	eventType: z.enum(["click", "view", "purchase"]),
});

// Create a partition parser
const partitionParser = rehiver.partitionParser(partitionSchema);

// Example paths
const validPath =
	"year=2023/month=01/day=15/hour=12/region=us-east/eventType=click";
const invalidPath = "year=2023/month=01/day=15/region=unknown/eventType=click";

// Parsing partition paths
console.log("Parsing valid partition path:");
try {
	const result = partitionParser.parse(validPath);
	console.log(JSON.stringify(result, null, 2));
} catch (error) {
	console.error("Error parsing path:", error);
}

// Safely parsing an invalid path
console.log("\nSafely parsing invalid partition path:");
const safeResult = partitionParser.safeParse(invalidPath);
if (safeResult.success) {
	console.log(JSON.stringify(safeResult.data, null, 2));
} else {
	console.log("Invalid path. Errors:", safeResult.error.errors);
}

// Creating glob patterns from partial data
console.log("\nCreating glob pattern from partial data:");
const partialData = {
	year: "2023",
	month: "01",
	region: "us-east" as const,
};
const globPattern = partitionParser.createGlobPattern(partialData);
console.log(globPattern);

// Extracting specific keys from a path
console.log("\nExtracting specific keys from a path:");
const extractedData = partitionParser.extractKeys(validPath, [
	"year",
	"month",
	"region",
]);
console.log(JSON.stringify(extractedData, null, 2));

// Time partitioning examples
console.log("\nTime partitioning examples:");

// Create a daily partitioner
const dailyPartitioner = rehiver.timePartitioner({
	granularity: TimeGranularity.Daily,
	format: "hive",
});

// Create a hourly partitioner
const hourlyPartitioner = rehiver.timePartitioner({
	granularity: TimeGranularity.Hourly,
	format: "hive",
});

// Generate current daily partition
const currentDailyPartition = dailyPartitioner.generateCurrentPath();
console.log("Current daily partition:", currentDailyPartition);

// Generate current hourly partition
const currentHourlyPartition = hourlyPartitioner.generateCurrentPath();
console.log("Current hourly partition:", currentHourlyPartition);

// Generate partitions for a date range
const startDate = new Date("2023-01-01");
const endDate = new Date("2023-01-05");
console.log("\nGenerating daily partitions for date range:");
const dailyPartitions = dailyPartitioner.generatePathsForRange(
	startDate,
	endDate,
);
console.log(dailyPartitions);

console.log("\nPartition handling example completed.");
