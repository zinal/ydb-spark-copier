package copytab;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
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
    private static final String CATALOG_PREFIX = "spark.sql.catalog.src.";
    private static final String CATALOG_PROPERTY = CATALOG_PREFIX + "url";
    private static final String CORES_PROPERTY = "spark.executor.cores";

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

        String master = "local[*]";
        String coresCount = sparkConfig.getProperty(CORES_PROPERTY);
        if (coresCount != null && coresCount.length() > 0) {
            master = "local[" + coresCount + "]";
        }

        SparkSession.Builder builder = SparkSession.builder()
                .appName("YDB Table Copier")
                .config("spark.ui.enabled", false)
                .master(master);
        for (var me : sparkConfig.entrySet()) {
            builder = builder.config(me.getKey().toString(), me.getValue().toString());
        }

        try (SparkSession spark = builder.getOrCreate()) {
            RowsWrittenListener rowsListener = new RowsWrittenListener();
            spark.sparkContext().addSparkListener(rowsListener);
            try {
                if ("export".equals(config.mode)) {
                    runExport(spark);
                } else if ("import".equals(config.mode)) {
                    runImport(spark);
                } else {
                    throw new IllegalArgumentException("Unknown mode: " + config.mode);
                }
            } finally {
                rowsListener.stop();
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
        var df1 = spark.read().format("ydb")
                .options(extractYdbConfig())
                .option("dbtable", config.tableName)
                .load();
        df1.createOrReplaceTempView("ydb_src");

        String filterClause = (config.filter != null && !config.filter.isBlank())
                ? " WHERE " + config.filter : "";
        String sql = "SELECT * FROM ydb_src" + filterClause;

        var df2 = spark.sql(sql);

        log.info("Export: {} -> {} (sql: {})", config.tableName, config.directory, sql);

        df2.write().mode("overwrite").parquet(config.directory);

        log.info("Done.");
    }

    private void runImport(SparkSession spark) {
        var df1 = spark.read().parquet(config.directory);
        df1.createOrReplaceTempView("parquet_src");

        String filterClause = (config.filter != null && !config.filter.isBlank())
                ? " WHERE " + config.filter : "";
        String sql = "SELECT * FROM parquet_src" + filterClause;

        var df2 = spark.sql(sql);

        log.info("Import: {} -> {} (sql: {})", config.directory, config.tableName, sql);

        df2.write().format("ydb")
                .mode("append")
                .option("method", "UPSERT")
                .option("batch.rows", "50000")
                .option("batch.sizelimit", "20971520")
                .option("write.retry.count", "100")
                .options(extractYdbConfig())
                .option("dbtable", config.tableName)
                .save();

        log.info("Done.");
    }

    private HashMap<String, String> extractYdbConfig() {
        var ret = new HashMap<String, String>();
        for (var me : sparkConfig.entrySet()) {
            String key = me.getKey().toString();
            if (key.startsWith(CATALOG_PREFIX)) {
                ret.put(key.substring(CATALOG_PREFIX.length()), me.getValue().toString());
            }
        }
        return ret;
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
