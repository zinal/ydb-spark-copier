package tech.ydb.samples.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Sample Spark application that connects to YDB via Catalog API.
 * <p>
 * Modes:
 * <ul>
 *   <li>Default (no mode): SHOW TABLES FROM src</li>
 *   <li>export: Read from YDB table, write to Parquet directory</li>
 *   <li>import: Read from Parquet directory, write to YDB table</li>
 * </ul>
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final String DEFAULT_CONFIG_PATH = "spark-config.xml";
    private static final String CATALOG_NAME = "src";

    public static void main(String[] args) throws Exception {
        Args parsed = parseArgs(args);
        String configPath = parsed.configPath;

        SparkSession.Builder builder = SparkSession.builder()
                .appName("YDB Spark Sample")
                .master("local[*]");

        Properties props = loadPropertiesFromXml(configPath);
        props.forEach((k, v) -> builder.config(k.toString(), v.toString()));

        try (SparkSession spark = builder.getOrCreate()) {
            if (parsed.mode == null) {
                runShowTables(spark);
            } else if ("export".equals(parsed.mode)) {
                runExport(spark, props, parsed.input, parsed.output, parsed.filter);
            } else if ("import".equals(parsed.mode)) {
                runImport(spark, parsed.input, parsed.output, parsed.filter);
            } else {
                throw new IllegalArgumentException("Unknown mode: " + parsed.mode);
            }
        }
    }

    private static void runShowTables(SparkSession spark) {
        log.info("Spark session created. Running: SHOW TABLES FROM {}", CATALOG_NAME);
        log.info("---");
        Dataset<Row> tables = spark.sql("SHOW TABLES FROM " + CATALOG_NAME);
        tables.show(Integer.MAX_VALUE, false);
        log.info("---");
        log.info("Done.");
    }

    private static void runExport(SparkSession spark, Properties props, String table, String outputDir, String filter) {
        String sql = buildSelectSql(table, filter);
        log.info("Export: {} -> {}", sql, outputDir);

        Dataset<Row> df = spark.sql(sql);
        df.write().mode("overwrite").parquet(outputDir);

        log.info("Done.");
    }

    private static void runImport(SparkSession spark, String inputDir, String table, String filter) {
        Dataset<Row> df = spark.read().parquet(inputDir);
        df.createOrReplaceTempView("parquet_src");

        String filterClause = (filter != null && !filter.isBlank()) ? " WHERE " + filter : "";
        String sql = "INSERT INTO " + table + " SELECT * FROM parquet_src" + filterClause;
        log.info("Import: {} -> {} (sql: {})", inputDir, table, sql);

        spark.sql(sql);

        log.info("Done.");
    }

    private static String buildSelectSql(String table, String filter) {
        String filterClause = (filter != null && !filter.isBlank()) ? " WHERE " + filter : "";
        return "SELECT * FROM " + table + filterClause;
    }

    private static Properties loadPropertiesFromXml(String configPath) throws Exception {
        Path path = Paths.get(configPath);
        if (!Files.exists(path)) {
            path = Paths.get("src/main/resources", configPath);
        }
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("Config file not found: " + configPath);
        }

        Properties props = new Properties();
        try (InputStream is = Files.newInputStream(path)) {
            props.loadFromXML(is);
        }
        return props;
    }

    private static Args parseArgs(String[] args) {
        Args result = new Args();
        result.configPath = DEFAULT_CONFIG_PATH;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--config" -> {
                    if (i + 1 < args.length) result.configPath = args[++i];
                }
                case "--mode" -> {
                    if (i + 1 < args.length) result.mode = args[++i];
                }
                case "--input" -> {
                    if (i + 1 < args.length) result.input = args[++i];
                }
                case "--output" -> {
                    if (i + 1 < args.length) result.output = args[++i];
                }
                case "--filter" -> {
                    if (i + 1 < args.length) result.filter = args[++i];
                }
                default -> {
                    if (i == 0 && !args[i].startsWith("--")) {
                        result.configPath = args[i];
                    }
                }
            }
        }

        if (result.mode != null) {
            if (result.input == null) throw new IllegalArgumentException("--input is required for mode=" + result.mode);
            if (result.output == null) throw new IllegalArgumentException("--output is required for mode=" + result.mode);
        }

        return result;
    }

    private static class Args {
        String configPath;
        String mode;
        String input;
        String output;
        String filter;
    }
}
