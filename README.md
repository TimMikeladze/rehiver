# 🐝 rehiver

> **Super-charge your S3 hive partitioned based file operations with intelligent pattern matching, change detection, optimized data-fetching, and out-of-the-box time series support.**
>

```bash
pnpm install rehiver
```

## 📋 Overview

rehiver is your TypeScript powerhouse for S3 operations that makes working with partitioned data and cloud storage effortless. It combines intelligent glob pattern matching with flexible Hive partitioning, local data caching, and efficient change detection to simplify complex data operations - all with type safety built in.

### Key Features

- **🍯 Hive Partitioning** - Parse and generate Hive-style partitions with type safety and custom partition layouts.
- **🔍 Pattern Matching** - Target exactly the files you need with expressive glob patterns.
- **⏱️ Time Partitioning** - Built-in support for time-based partitioning (hourly, daily, monthly, yearly).
- **🔄 Change Detection** - Track additions, modifications, and deletions efficiently with disk-based state.
- **💾 Local Data Management** - Smart local caching with disk-based storage and efficient change detection.
- **⚡ Concurrency Controls** - Process multiple objects in parallel with fine-tuned settings.
- **📊 Progress Tracking** - Monitor long-running operations with built-in hooks.
- **🚀 Optimized Data Fetching** - Smart caching, batch processing, and efficient pattern matching for large-scale operations.

## 🚀 Quick Start

Here's a quick example demonstrating the core functionality of rehiver for a simple time series data pipeline:

```typescript
import rehiver from 'rehiver';

// Initialize with your configuration
const rehiver = new Rehiver({
  s3Options: { region: 'us-east-1' }
});

// Set up time partitioning for hourly data
const timeGen = rehiver.timePartitioner({
  granularity: 'hourly',
  format: 'hive'
});

// Get the last 24 hours of metrics
const now = new Date();
const yesterday = new Date(now);
yesterday.setDate(now.getDate() - 1);

// Find and process time series data
await rehiver.streamMatchingObjects(
  'metrics-bucket',
  timeGen.generatePathsForRange(yesterday, now).map(p => `${p}/metrics.parquet`),
  async (key) => {
    const timestamp = timeGen.parsePath(key).toDate();
    await processTimeSeriesData(key, timestamp);
  }
);
```

## 🔥 Features in Action

### Powerful Pattern Matching

Target exactly what you need with glob patterns:

```typescript
// Multiple patterns with negation
const dataFiles = await rehiver.findMatchingObjects({
  bucket: 'analytics-bucket',
  patterns: [
    '**/*.json',           // All JSON files
    '!**/temp/**/*.json'   // Exclude temp files
  ],
  maxConcurrentRequests: 20
});
```

Under the hood, rehiver optimizes pattern matching by:
- Compiling patterns to regular expressions for performance
- Caching compiled patterns to avoid redundant processing
- Supporting advanced glob syntax including negation and alternation

### Time-Series Made Simple

Effortlessly work with time-partitioned data:

```typescript
// Create a time partitioner
const timeGen = rehiver.timePartitioner({
  granularity: 'daily',
  format: 'hive'  // Creates "year=2023/month=07/day=15" style paths
});

// Generate paths for the last 7 days
const today = new Date();
const weekAgo = new Date(today);
weekAgo.setDate(today.getDate() - 7);

const paths = timeGen.generatePathsForRange(weekAgo, today);

// Use generated paths to find matching objects
const weeklyData = await rehiver.findMatchingObjects({
  bucket: 'timeseries-bucket',
  patterns: paths.map(p => `${p}/**/*.parquet`)
});
```

Time series features include:
- Multiple granularity levels (hourly, daily, monthly, yearly)
- Range generation for time windows
- Integration with Hive partitioning and pattern matching
- Efficient querying of historical data with smart path generation
- Support for custom time formats and timezone handling

### Type-Safe Hive Partitioning

Handle partitioned data with confidence:

```typescript
import { z } from 'zod';

// Define your partition schema with Zod
const partitionSchema = z.object({
  year: z.coerce.number().int().min(2000).max(2100),
  month: z.coerce.number().int().min(1).max(12),
  day: z.coerce.number().int().min(1).max(31),
  region: z.enum(['us', 'eu', 'asia'])
});

// Create a partition parser
const parser = rehiver.partitionParser(partitionSchema);

// Parse with type inference
const partitionData = parser.parse('year=2023/month=07/day=15/region=us');
// => { year: 2023, month: 7, day: 15, region: 'us' }

// Generate a glob pattern for partial specifications
const pattern = parser.createGlobPattern({ year: 2023, region: 'us' });
// => "year=2023/month=*/day=*/region=us"
```

The Hive partitioning system provides:
- Runtime validation through Zod schemas
- Type-safe access to partition components
- Bidirectional conversion between paths and structured data
- Seamless integration with Apache Hive, Presto, and other query engines
- Support for nested partitioning (e.g., year/month/day/hour)
- Automatic partition pruning for efficient querying
- Built-in support for common partition types (date, region, customer, etc.)

### Time Series Database Integration

rehiver excels at working with time series data:

```typescript
// Set up hourly partitioning for high-frequency data
const hourlyGen = rehiver.timePartitioner({
  granularity: 'hourly',
  format: 'hive'
});

// Generate paths for the last 24 hours
const now = new Date();
const yesterday = new Date(now);
yesterday.setDate(now.getDate() - 1);
const paths = hourlyGen.generatePathsForRange(yesterday, now);

// Find and process time series data
const timeSeriesData = await rehiver.findMatchingObjects({
  bucket: 'metrics-bucket',
  patterns: paths.map(p => `${p}/metrics.parquet`),
  // Optional: Add metadata for time series specific operations
  metadata: {
    retentionPeriod: '30d',
    compression: 'snappy'
  }
});

// Process with time-aware operations
for (const data of timeSeriesData) {
  const partition = parser.parse(data.key);
  const timestamp = new Date(partition.year, partition.month - 1, partition.day, partition.hour);
  await processTimeSeriesData(data.key, timestamp);
}
```

Time series database features:
- Optimized for high-frequency data ingestion
- Efficient querying of time ranges
- Automatic data lifecycle management
- Support for data retention policies
- Integration with popular time series databases
- Built-in support for data downsampling and aggregation
- Smart caching for frequently accessed time ranges

### Efficient Change Detection

Track what's changed between runs:

```typescript
// Create a change detector
const detector = rehiver.changeDetector();

// Load previous state
await detector.loadPreviousState('state.json');

// Add current objects
const currentObjects = await rehiver.findMatchingObjects('data-lake', '**/*.parquet');
detector.addObjects(currentObjects.map(key => ({ 
  key, size: 0, etag: '', lastModified: new Date() 
})));

// Get only what changed
const changes = detector.detectChanges();

// Process each change type
for (const change of changes) {
  if (change.changeType === 'added') {
    await processNewFile(change.object.key);
  } else if (change.changeType === 'modified') {
    await reprocessFile(change.object.key);
  }
}

// Save current state for next run
await detector.saveCurrentState('state.json');
```

Change detection capabilities:
- Track additions, modifications, and deletions
- Configurable comparison modes (quick or full)
- Persistent state between application runs

## 🌍 Real-World Examples

### Data Lake ETL Pipeline

Build a robust ETL pipeline with change detection:

```typescript
// 1. Set up time partitioning and change detection
const timeGen = rehiver.timePartitioner({ granularity: 'daily' });
const todayPath = timeGen.generateCurrentPath();
const detector = rehiver.changeDetector();

// 2. Load previous state
await detector.loadPreviousState();

// 3. Get current raw files
const rawFiles = await rehiver.findMatchingObjects(
  'data-lake',
  `${todayPath}/raw/**/*.json`
);

// 4. Track the objects for change detection
detector.addObjects(rawFiles.map(key => ({
  key,
  size: 0,
  etag: '',
  lastModified: new Date()
})));

// 5. Process only new or modified files
const changes = detector.detectChanges();
for (const { changeType, object } of changes) {
  if (changeType === 'added' || changeType === 'modified') {
    await transformAndLoad(object.key);
  }
}

// 6. Save state for next run
await detector.saveCurrentState();
```

### Event Log Processing

Stream and process logs with concurrency control:

```typescript
// Process logs with controlled concurrency
const { processed, matched } = await rehiver.streamMatchingObjects({
  bucket: 'logs-bucket',
  patterns: '**/*.log',
  processor: async (key) => {
    const logContent = await downloadLogFile(key);
    await processLogEvents(logContent);
  },
  maxConcurrentProcessing: 5,
  onProgress: ({ processed, total, matched }) => {
    console.log(`Processed ${processed}/${total} objects, matched ${matched}`);
  }
});

console.log(`Completed processing ${processed} out of ${matched} logs`);
```

### Multi-Region Data Processing

```typescript
// Define your partition schema
const schema = z.object({
  year: z.coerce.number(),
  month: z.coerce.number(),
  day: z.coerce.number(),
  region: z.enum(['us', 'eu', 'asia'])
});

// Create a partition parser
const parser = rehiver.partitionParser(schema);

// Find data for US region from last month
const lastMonth = new Date();
lastMonth.setMonth(lastMonth.getMonth() - 1);
const year = lastMonth.getFullYear();
const month = lastMonth.getMonth() + 1;

// Create a pattern for the specific month and region
const pattern = parser.createGlobPattern({ 
  year, 
  month, 
  region: 'us' 
});

// Find and process matching objects
const usData = await rehiver.findMatchingObjects(
  'analytics-bucket',
  `${pattern}/**/*.parquet`
);

console.log(`Processing ${usData.length} US region files from ${year}-${month}`);
```

### Real-Time Data Monitoring

```typescript
// Create hourly partitioner
const hourlyGen = rehiver.timePartitioner({
  granularity: 'hourly',
  format: 'hive'
});

// Generate paths for the last 24 hours
const now = new Date();
const yesterday = new Date(now);
yesterday.setDate(now.getDate() - 1);
const paths = hourlyGen.generatePathsForRange(yesterday, now);

// Find the latest data files
const latestData = await rehiver.findMatchingObjects(
  'metrics-bucket',
  paths.map(p => `${p}/metrics.json`)
);

console.log(`Found ${latestData.length} hourly metric files for the dashboard`);
```

## 💻 API Overview

rehiver provides a clean, unified API for all functionality:

```typescript
// Create a single rehiver instance for all operations
const rehiver = new Rehiver({
  s3Options: {
    region: 'us-east-1',
    // Optional AWS credentials
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!
    },
    // Optional S3 endpoint for custom S3-compatible storage
    endpoint: 'http://minio.example.com',
    forcePathStyle: true
  },
  // Optional caching configuration
  cacheOptions: {
    enabled: true,
    maxSize: 1000,
    ttl: 5 * 60 * 1000 // 5 minutes
  }
});

// All functionality through the same interface
// 1. Pattern matching
const matched = rehiver.match(paths, '**/*.json');

// 2. S3 operations
const objects = await rehiver.findMatchingObjects('bucket', '**/*.parquet');

// 3. Hive partitioning
const parser = rehiver.partitionParser(schema);

// 4. Time partitioning
const timeGen = rehiver.timePartitioner({ granularity: 'daily' });

// 5. Change detection
const detector = rehiver.changeDetector();
```

## 🏗️ Technical Implementation

### Architecture Overview

rehiver is built on a modular architecture with specialized components:

1. **PathMatcher**: Core pattern matching capabilities
2. **S3PathMatcher**: S3-specific operations and optimizations
3. **HivePartitionParser**: Partition path parsing and validation
4. **TimePartitionGenerator**: Time-based path generation
5. **ChangeDetectionEngine**: File change tracking
6. **rehiver**: Main class that orchestrates all components

### S3 Integration

rehiver's S3 integration is designed for reliability and performance:

- Automatic retry with exponential backoff
- Concurrency controls to prevent API throttling
- Metadata caching for improved performance
- Support for custom S3-compatible storage endpoints

### Data Fetching Optimizations

rehiver includes several powerful optimizations for efficient data fetching at scale:

- **Smart Caching System**
  - LRU-based metadata caching with configurable TTL
  - Background cache refresh to prevent stale data
  - Automatic cache invalidation on object updates
  - Configurable cache size and refresh thresholds

- **Concurrency Controls**
  - Fine-grained control over request and processing concurrency
  - Batch processing with configurable batch sizes
  - Automatic throttling to prevent API rate limits
  - Progress tracking for long-running operations

- **Pattern Matching Optimizations**
  - Compiled regex caching for faster pattern matching
  - Fast path matching with precompiled patterns
  - Support for negation patterns to exclude files
  - Efficient handling of special characters in paths

- **Local Caching**
  - Optional local file caching to reduce S3 requests
  - Skip existing files to avoid redundant downloads
  - Configurable cache base paths and policies
  - Automatic cache cleanup and management

- **Performance Monitoring**
  - Built-in progress tracking hooks
  - Detailed statistics for processed objects
  - Support for abort signals to cancel long operations
  - Comprehensive error handling and retry logic

Example of optimized data fetching:

```typescript
// Configure optimized data fetching
const rehiver = new Rehiver({
  s3Options: {
    region: 'us-east-1',
    maxRetries: 3
  },
  cacheOptions: {
    enabled: true,
    maxSize: 2000,        // Store up to 2000 items
    ttl: 10 * 60 * 1000, // Cache for 10 minutes
    refreshThreshold: 70  // Refresh at 70% of TTL
  }
});

// Process files with optimized settings
await rehiver.streamMatchingObjects({
  bucket: 'data-bucket',
  patterns: '**/*.parquet',
  processor: async (key) => processFile(key),
  // Concurrency controls
  concurrency: {
    requestLimit: 5,      // Max concurrent S3 requests
    processingLimit: 10   // Max concurrent file processing
  },
  // Batch processing
  batchSize: 100,
  // Local caching
  localCache: {
    enabled: true,
    basePath: './cache',
    skipExisting: true
  },
  // Progress tracking
  onProgress: ({ processed, total, matched }) => {
    console.log(`Processed: ${processed}/${total} (${matched} matched)`);
  }
});
```

## 🤝 Contributing

Contributions are welcome! Here's how to get started:

1. Fork the repository
2. Clone your fork and create a new branch
3. Install dependencies: `pnpm install`
4. Start docker containers for testing: `docker compose up -d`
5. Run tests during development: `pnpm dev`
6. Make your changes and add tests
7. Ensure all tests pass: `pnpm test`
8. Commit your changes with conventional commits
9. Push your branch and open a pull request
