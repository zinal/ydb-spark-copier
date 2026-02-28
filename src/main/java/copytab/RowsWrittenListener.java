package copytab;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Spark listener that tracks the number of rows written by tasks and reports
 * the count periodically (every 30 seconds).
 */
public class RowsWrittenListener extends SparkListener {

    private static final Logger log = LoggerFactory.getLogger(RowsWrittenListener.class);
    private static final int REPORT_INTERVAL_SECONDS = 30;

    private final AtomicLong rowsWritten = new AtomicLong(0);
    private final ScheduledExecutorService scheduler;
    private final ScheduledFuture<?> reportTask;

    public RowsWrittenListener() {
        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "rows-written-reporter");
            t.setDaemon(true);
            return t;
        });
        this.reportTask = scheduler.scheduleAtFixedRate(
                this::reportRowsWritten,
                REPORT_INTERVAL_SECONDS,
                REPORT_INTERVAL_SECONDS,
                TimeUnit.SECONDS
        );
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        if (taskEnd.taskMetrics() == null) {
            return;
        }
        var metrics = taskEnd.taskMetrics();

        // Output metrics (records written to destination)
        if (metrics.outputMetrics() != null) {
            long records = metrics.outputMetrics().recordsWritten();
            rowsWritten.addAndGet(records);
        }
    }

    private void reportRowsWritten() {
        long count = rowsWritten.get();
        log.info("Rows written so far: {}", count);
    }

    /**
     * Stops the periodic reporting and logs the final row count.
     */
    public void stop() {
        reportTask.cancel(false);
        log.info("Total rows written: {}", rowsWritten.get());
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
    }

    /**
     * Returns the total number of rows written so far.
     */
    public long getRowsWritten() {
        return rowsWritten.get();
    }
}
