# YDB Copy Table Sample

Sample Java application to export and import YDB tables using Apache Spark with the YDB Spark Connector. Connects to YDB via the Catalog API and uses Parquet as the intermediate storage format.

## Principles of Operation

The application runs in two modes:

1. **Export** – Reads data from a YDB table via Spark SQL, optionally applying a `WHERE` filter, and writes the result to a Parquet directory on the local filesystem.
2. **Import** – Reads data from a Parquet directory, optionally applies a `WHERE` filter, and inserts the result into a YDB table.

### How It Works

- **Catalog integration**: YDB is exposed as a Spark SQL catalog named `src`. The catalog configuration (connection URL, auth, etc.) is loaded from an XML properties file.
- **Export flow**: `SELECT * FROM src.<table> [WHERE ...]` → Spark DataFrame → Parquet files (overwrite mode).
- **Import flow**: Parquet directory → Spark DataFrame → temp view → `SELECT * FROM parquet_src [WHERE ...]` → YDB table.
- **Table naming**: YDB table paths should be specified natively with slashes as schema delimited, but without the database name (e.g. `table_in_root` or `schema/table_in_schema`). Slashes in the table name are converted to dots automatically to be compatible with Spark SQL notation when needed.

## Requirements

- Java 17
- Maven 3.6+
- YDB instance (local Docker or remote)

## Build

```bash
mvn clean package
```

Produces `target/ydb-copier-1.0-SNAPSHOT.jar` and, with the release profile, `target/ydb-copier-1.0-SNAPSHOT-bin.zip` containing the JAR, dependencies, and scripts.

## Configuration

Edit `spark-config.xml` (or your custom config file) to set YDB connection parameters:

- **spark.sql.catalog.src** – YDB catalog implementation class
- **spark.sql.catalog.src.url** – YDB connection string (e.g. `grpc://localhost:2136/local` for local Docker)
- **spark.sql.catalog.src.auth.token.file** – Path to token file for cloud/remote YDB (uncomment if needed)
- **spark.sql.catalog.src.auth.ca.file** – Path to TLS certificate for secure connections (uncomment if needed)

See [YDB Spark documentation](https://ydb.tech/docs/en/integrations/ingestion/spark) for all options.

### Cores and Memory

- **spark.executor.cores** – Sets the number of working threads per executor. Configure this in `spark-config.xml` or via `--conf` option when using `spark-submit`.
- **spark.executor.memory** – Should be at least 1G per core (e.g. 4G for 4 cores). Set in `spark-config.xml` or via `--conf`.
- **-Xmx** (JVM heap) – When running via the startup script (e.g. `ydb-copier.sh`), set `-Xmx` to typically be twice the `spark.executor.memory` value. For example, with `spark.executor.memory=4g`, use `-Xmx8g` or `-Xmx8192m`.

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
./ydb-copier.sh --config spark-config.xml --mode export --table myschema/mytable --directory /tmp/parquet-out
```

**Export with filter:**
```bash
./ydb-copier.sh --config spark-config.xml --mode export --table myschema/mytable --directory /tmp/parquet-out --filter "id > 100 AND status='active'"
```

**Import Parquet to YDB:**
```bash
./ydb-copier.sh --config spark-config.xml --mode import --table myschema/mytable --directory /tmp/parquet-in
```

**Import with filter:**
```bash
./ydb-copier.sh --config spark-config.xml --mode import --table myschema/mytable --directory /tmp/parquet-in --filter "dt <= '2026-02-03T00:00:00.000000Z'"
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
