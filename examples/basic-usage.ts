import { ContentType, LogLevel, Rehiver } from "../src";

// Create a new Rehiver instance with custom options
const rehiver = new Rehiver({
	// Configure matching options
	matchOptions: {
		nocase: true,
		dot: true,
	},
	// Configure S3 options
	s3Options: {
		region: process.env.AWS_REGION || "us-east-1",
		endpoint: process.env.S3_ENDPOINT,
		credentials: {
			accessKeyId: process.env.AWS_ACCESS_KEY_ID || "minioadmin",
			secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "minioadmin",
		},
		forcePathStyle: true,
		maxRetries: 3,
	},
	// Configure logger options
	loggerOptions: {
		level: LogLevel.Debug,
	},
});

// Example of pattern matching with paths
const paths = [
	"data/year=2023/month=01/day=15/hour=12/events.json",
	"data/year=2023/month=02/day=01/hour=00/events.json",
	"logs/2023/02/01/app.log",
	"reports/monthly/2023-01.csv",
	"reports/quarterly/2023-Q1.xlsx",
];

// Match paths with glob patterns
console.log("Matching data paths:");
const dataMatches = rehiver.match(paths, "data/**/*.json");
console.log(dataMatches);

// Match with multiple patterns
console.log("\nMatching with multiple patterns:");
const reportMatches = rehiver.match(paths, [
	"reports/**/*.csv",
	"reports/**/*.xlsx",
]);
console.log(reportMatches);

// Using negation patterns
console.log("\nExcluding log files:");
const nonLogMatches = rehiver.not(paths, "**/*.log");
console.log(nonLogMatches);

// Simple demonstration of the ContentType utility
console.log("\nContent type detection:");
const filePaths = [
	"document.pdf",
	"image.png",
	"data.json",
	"script.js",
	"styles.css",
];

for (const filePath of filePaths) {
	const contentType = ContentType.detect(filePath);
	console.log(`${filePath}: ${contentType}`);
}

// Run the example
console.log("\nRehiver example completed.");
