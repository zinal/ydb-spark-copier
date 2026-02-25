package copytab;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Sample Spark application that connects to YDB via Catalog API.
 * <p>
 * Modes:
 * <ul>
 * <li>Default (no mode): SHOW TABLES FROM src</li>
 * <li>export: Read from YDB table, write to Parquet directory</li>
 * <li>import: Read from Parquet directory, write to YDB table</li>
 * </ul>
 */
public class Copier {

    private static final Logger log = LoggerFactory.getLogger(Copier.class);

    private static final String DEFAULT_CONFIG_PATH = "spark-config.xml";
    private static final String CATALOG_NAME = "src";

    private final Properties sparkConfig;
    private final Config args;

    /**
     * Construct the copier instance.
     *
     * @param sparkConfig Set of Spark properties
     * @param args Execution settings
     */
    public Copier(Properties sparkConfig, Config args) {
        this.sparkConfig = sparkConfig;
        this.args = args;
    }

    public void run() throws Exception {
        SparkSession.Builder builder = SparkSession.builder()
                .appName("YDB Table Copier")
                .master("local[*]");

        sparkConfig.forEach((k, v) -> builder.config(k.toString(), v.toString()));

        try (SparkSession spark = builder.getOrCreate()) {
            if ("export".equals(args.mode)) {
                runExport(spark, args.input, args.output, args.filter);
            } else if ("import".equals(args.mode)) {
                runImport(spark, args.input, args.output, args.filter);
            } else {
                throw new IllegalArgumentException("Unknown mode: " + args.mode);
            }
        }
    }

    public static void main(String[] args) {
        try {
            Config parsed = parseArgs(args);
            Properties sparkConfig = loadPropertiesFromXml(parsed.configPath);
            Copier main = new Copier(sparkConfig, parsed);
            main.run();
        } catch (Exception ex) {
            log.error("FATAL", ex);
            System.exit(1);
        }
    }

    private void runExport(SparkSession spark, String table, String outputDir, String filter) {
        String sql = buildSelectSql(table, filter);
        log.info("Export: {} -> {}", sql, outputDir);

        Dataset<Row> df = spark.sql(sql);
        df.write().mode("overwrite").parquet(outputDir);

        log.info("Done.");
    }

    private void runImport(SparkSession spark, String inputDir, String table, String filter) {
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
        Properties props = new Properties();
        try (InputStream is = Files.newInputStream(Paths.get(configPath))) {
            props.loadFromXML(is);
        }
        return props;
    }

    private static Config parseArgs(String[] args) {
        Config result = new Config();
        result.configPath = DEFAULT_CONFIG_PATH;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--config" -> {
                    if (i + 1 < args.length) {
                        result.configPath = args[++i];
                    }
                }
                case "--mode" -> {
                    if (i + 1 < args.length) {
                        result.mode = args[++i];
                    }
                }
                case "--input" -> {
                    if (i + 1 < args.length) {
                        result.input = args[++i];
                    }
                }
                case "--output" -> {
                    if (i + 1 < args.length) {
                        result.output = args[++i];
                    }
                }
                case "--filter" -> {
                    if (i + 1 < args.length) {
                        result.filter = args[++i];
                    }
                }
                default -> {
                    if (i == 0 && !args[i].startsWith("--")) {
                        result.configPath = args[i];
                    }
                }
            }
        }

        if (result.mode != null) {
            if (result.input == null) {
                throw new IllegalArgumentException("--input is required for mode=" + result.mode);
            }
            if (result.output == null) {
                throw new IllegalArgumentException("--output is required for mode=" + result.mode);
            }
        }

        return result;
    }

    public static class Config {

        String configPath;
        String mode;
        String input;
        String output;
        String filter;
    }
}
