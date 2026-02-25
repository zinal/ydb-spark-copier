# YDB Copy Table Sample

Sample Java application to export and import YDB tables using Apache Spark with the YDB Spark Connector. Connects to YDB via the Catalog API and uses Parquet as the intermediate storage format.

## Principles of Operation

The application runs in two modes:

1. **Export** – Reads data from a YDB table via Spark SQL, optionally applying a `WHERE` filter, and writes the result to a Parquet directory on the local filesystem.
2. **Import** – Reads data from a Parquet directory, optionally applies a `WHERE` filter, and inserts the result into a YDB table.

### How It Works

- **Catalog integration**: YDB is exposed as a Spark SQL catalog named `src`. The catalog configuration (connection URL, auth, etc.) is loaded from an XML properties file.
- **Export flow**: `SELECT * FROM src.<table> [WHERE ...]` → Spark DataFrame → Parquet files (overwrite mode).
- **Import flow**: Parquet directory → Spark DataFrame → temp view → `INSERT INTO src.<table> SELECT * FROM parquet_src [WHERE ...]`.
- **Table naming**: YDB table paths use dots (e.g. `database.schema.table`). Slashes in the table name are converted to dots automatically.

## Requirements

- Java 17
- Maven 3.6+
- YDB instance (local Docker or remote)

## Configuration

Edit `scripts/spark-config.xml` (or your custom config file) to set YDB connection parameters:

- **spark.sql.catalog.src** – YDB catalog implementation class
- **spark.sql.catalog.src.url** – YDB connection string (e.g. `grpc://localhost:2136/local` for local Docker)
- **spark.sql.catalog.src.auth.token.file** – Path to token file for cloud/remote YDB (uncomment if needed)
- **spark.sql.catalog.src.auth.ca.file** – Path to TLS certificate for secure connections (uncomment if needed)

See [YDB Spark documentation](https://ydb.tech/docs/en/integrations/ingestion/spark) for all options.

## Build

```bash
mvn clean package
```

Produces `target/ydb-copier-1.0-SNAPSHOT.jar` and, with the release profile, `target/ydb-copier-1.0-SNAPSHOT-bin.zip` containing the JAR, dependencies, and scripts.

## Usage

### Command Line Options

| Option | Description |
|--------|-------------|
| `--config <path>` | Path to Spark config XML (default: `spark-config.xml` in current directory) |
| `--mode export\|import` | Execution mode (required) |
| `--table <name>` | YDB table path (e.g. `mydb.myschema.mytable`) |
| `--directory <path>` | Local directory: output for export, input for import |
| `--filter <sql>` | Optional WHERE clause (e.g. `id>100 AND status='active'`) |

### Examples

**Export YDB table to Parquet:**
```bash
mvn exec:java -Dexec.args="--config spark-config.xml --mode export --table mydb.myschema.mytable --directory /tmp/parquet-out"
```

**Export with filter:**
```bash
mvn exec:java -Dexec.args="--config spark-config.xml --mode export --table mydb.myschema.mytable --directory /tmp/parquet-out --filter 'id>100 AND status=\"active\"'"
```

**Import Parquet to YDB:**
```bash
mvn exec:java -Dexec.args="--config spark-config.xml --mode import --table mydb.myschema.mytable --directory /tmp/parquet-in"
```

**Import with filter:**
```bash
mvn exec:java -Dexec.args="--config spark-config.xml --mode import --table mydb.myschema.mytable --directory /tmp/parquet-in --filter 'year=2024'"
```

### spark-submit

```bash
spark-submit \
  --master "local[*]" \
  --packages tech.ydb.spark:ydb-spark-connector-shaded:2.1.0 \
  --conf spark.executor.memory=4g \
  --class copytab.Copier \
  target/ydb-copier-1.0-SNAPSHOT.jar \
  --config spark-config.xml --mode export --table mydb.mytable --directory /tmp/out
```

When using the assembled zip, run from the extracted directory so that `spark-config.xml` and scripts are available. The main JAR is in the `lib` folder.
