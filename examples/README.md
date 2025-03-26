# Rehiver Examples

This directory contains examples demonstrating the usage of the Rehiver library.

## Prerequisites

Make sure you have Node.js installed (version 18 or later) and pnpm as your package manager.

## Running the Examples

You can run any example using the `pnpm tsx` command, which allows you to execute TypeScript files directly without a separate compilation step:

```bash
pnpm tsx examples/basic-usage.ts
```

## Available Examples

1. **Basic Usage** (`basic-usage.ts`): Demonstrates core functionality including pattern matching and content type detection.

2. **Partition Handling** (`partition-handling.ts`): Shows how to use Hive partition parsers with zod schemas and time partitioning features.

3. **Change Detection** (`change-detection.ts`): Demonstrates how to use the change detection engine to track changes in a set of objects.

## Environment Variables

Some examples may require environment variables to be set. For example, when working with S3:

```
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
S3_ENDPOINT=https://your-s3-endpoint.com
```

If you're testing locally with MinIO or a similar S3-compatible storage, you can set `S3_ENDPOINT` to your local endpoint (e.g., `http://localhost:9000`).

## Notes

- These examples are provided for demonstration purposes and may need to be adapted to your specific use case.
- For production usage, refer to the main [Rehiver documentation](../README.md) for more detailed information.
- When running examples that interact with external services like S3, ensure you have the proper permissions and credentials configured. 