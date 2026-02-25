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
    private static final String CATALOG_PROPERTY = "spark.sql.catalog.src";

    private final Properties sparkConfig;
    private final Config config;

    /**
     * Construct the copier instance.
     *
     * @param sparkConfig Set of Spark properties
     * @param config Execution settings
     */
    public Copier(Properties sparkConfig, Config config) {
        this.sparkConfig = sparkConfig;
        this.config = config;
    }

    public void run() throws Exception {
        String url = sparkConfig.getProperty(CATALOG_PROPERTY);
        if (url == null) {
            throw new IllegalArgumentException("Missing configuration: " + CATALOG_PROPERTY);
        }
        log.info("YDB connection URL: {}", url);

        SparkSession.Builder builder = SparkSession.builder()
                .appName("YDB Table Copier")
                .master("local[*]");
        sparkConfig.forEach((k, v) -> builder.config(k.toString(), v.toString()));

        try (SparkSession spark = builder.getOrCreate()) {
            if ("export".equals(config.mode)) {
                runExport(spark);
            } else if ("import".equals(config.mode)) {
                runImport(spark);
            } else {
                throw new IllegalArgumentException("Unknown mode: " + config.mode);
            }
        }
    }

    /**
     * Program entry point.
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        try {
            Config parsed = parseArgs(args);
            if (parsed.mode == null || parsed.tableName == null || parsed.directory == null) {
                System.out.println("USAGE: Copier --mode export|import "
                        + "--table YDB-TABLE --directory LOCAL-DIRECTORY "
                        + "[--filter FILTER-CONDITION]");
                System.exit(2);
            }
            Properties sparkConfig = loadPropertiesFromXml(parsed.configPath);
            Copier main = new Copier(sparkConfig, parsed);
            main.run();
        } catch (Exception ex) {
            log.error("FATAL", ex);
            System.exit(1);
        }
    }

    private void runExport(SparkSession spark) {
        String sql = buildSelectSql(config.tableName, config.filter);
        log.info("Export: {} -> {}", sql, config.directory);

        Dataset<Row> df = spark.sql(sql);
        df.write().mode("overwrite").parquet(config.directory);

        log.info("Done.");
    }

    private void runImport(SparkSession spark) {
        Dataset<Row> df = spark.read().parquet(config.directory);
        df.createOrReplaceTempView("parquet_src");

        String table = config.tableName;
        if (table.contains("/")) {
            table = table.replace('/', '.');
        }
        String filterClause = (config.filter != null && !config.filter.isBlank())
                ? " WHERE " + config.filter : "";
        String sql = "INSERT INTO " + CATALOG_NAME + "." + table + " SELECT * FROM parquet_src" + filterClause;
        log.info("Import: {} -> {} (sql: {})", config.directory, table, sql);

        spark.sql(sql);

        log.info("Done.");
    }

    private static String buildSelectSql(String table, String filter) {
        if (table.contains("/")) {
            table = table.replace('/', '.');
        }
        String filterClause = (filter != null && !filter.isBlank()) ? " WHERE " + filter : "";
        return "SELECT * FROM " + CATALOG_NAME + "." + table + filterClause;
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
                case "--table" -> {
                    if (i + 1 < args.length) {
                        result.tableName = args[++i];
                    }
                }
                case "--directory" -> {
                    if (i + 1 < args.length) {
                        result.directory = args[++i];
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

        return result;
    }

    /**
     * Configuration settings for Copier.
     */
    public static class Config {

        private String configPath;
        private String mode;
        private String tableName;
        private String directory;
        private String filter;

        public String getConfigPath() {
            return configPath;
        }

        public void setConfigPath(String configPath) {
            this.configPath = configPath;
        }

        public String getMode() {
            return mode;
        }

        public void setMode(String mode) {
            this.mode = mode;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getDirectory() {
            return directory;
        }

        public void setDirectory(String directory) {
            this.directory = directory;
        }

        public String getFilter() {
            return filter;
        }

        public void setFilter(String filter) {
            this.filter = filter;
        }

    }
}
