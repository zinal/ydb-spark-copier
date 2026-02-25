# YDB Spark Sample

Sample Java application using Apache Spark 3.5 with YDB Spark Connector. Connects to YDB via the Catalog API.

## Modes

1. **Default** (no mode): `SHOW TABLES FROM src`
2. **export**: Read from YDB table via Spark SQL (optional filter), write to Parquet directory
3. **import**: Read from Parquet directory, apply optional filter, ingest into YDB table

## Requirements

- Java 17
- Maven 3.6+
- YDB instance (local Docker or remote)

## Configuration

Edit `src/main/resources/spark-config.xml` to set your YDB connection parameters:

- **url** – YDB connection string (e.g. `grpc://localhost:2136/local` for local Docker)
- **auth.token.file** – Path to token file for cloud/remote YDB (uncomment if needed)
- **auth.ca.file** – Path to TLS certificate for secure connections (uncomment if needed)

See [YDB Spark documentation](https://ydb.tech/docs/en/integrations/ingestion/spark) for all options.

## Build

```bash
mvn clean package
```

## Run

### Command line options

| Option | Description |
|--------|-------------|
| `--config <path>` | Spark config XML (default: spark-config.xml) |
| `--mode export\|import` | Execution mode |
| `--input <path>` | Input: table (e.g. src.myschema.mytable) for export, directory for import |
| `--output <path>` | Output: directory for export, table for import |
| `--filter <sql>` | Optional WHERE clause (e.g. "a=1 AND b=2") |

### Examples

**Show tables (default):**
```bash
mvn exec:java
```

**Export YDB table to Parquet:**
```bash
mvn exec:java -Dexec.args="--mode export --input src.myschema.mytable --output /tmp/parquet-out"
```

**Export with filter:**
```bash
mvn exec:java -Dexec.args="--mode export --input src.myschema.mytable --output /tmp/parquet-out --filter 'id>100 AND status=\"active\"'"
```

**Import Parquet to YDB:**
```bash
mvn exec:java -Dexec.args="--mode import --input /tmp/parquet-in --output src.myschema.mytable"
```

**Import with filter:**
```bash
mvn exec:java -Dexec.args="--mode import --input /tmp/parquet-in --output src.myschema.mytable --filter 'year=2024'"
```

### spark-submit

```bash
spark-submit \
  --master "local[*]" \
  --packages tech.ydb.spark:ydb-spark-connector-shaded:2.1.0 \
  --conf spark.executor.memory=4g \
  --class tech.ydb.samples.spark.Main \
  target/ydb-spark-sample-1.0-SNAPSHOT.jar \
  --config spark-config.xml --mode export --input src.mytable --output /tmp/out
```
