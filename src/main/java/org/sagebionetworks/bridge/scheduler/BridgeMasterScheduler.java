package org.sagebionetworks.bridge.scheduler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.sqs.AmazonSQS;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.quartz.CronScheduleBuilder;
import org.quartz.spi.MutableTrigger;

/** Bridge Master Scheduler */
public class BridgeMasterScheduler {
    // To prevent infinite loops, specify a max number of scheduled executions per scheduler run. (60 per hour.)
    private static final int MAX_EXECUTIONS = 60;

    // Config keys
    private static final String CONFIG_KEY_CRON_SCHEDULE = "cronSchedule";
    private static final String CONFIG_KEY_HASH_KEY = "hashKey";
    private static final String CONFIG_KEY_LAST_PROCESSED_TIME = "lastProcessedTime";
    private static final String CONFIG_KEY_REQUEST_TEMPLATE = "requestTemplate";
    private static final String CONFIG_KEY_SCHEDULE_ID = "scheduleId";
    private static final String CONFIG_KEY_QUEUE_URL = "sqsQueueUrl";
    private static final String HASH_KEY_MASTER_SCHEDULER = "BridgeMasterScheduler";

    private Table ddbConfigTable;
    private Table ddbStatusTable;
    private AmazonSQS sqsClient;

    /**
     * DDB table for Scheduler configs. Storing configs in DDB allows us to change them dynamically without
     * re-deploying the scheduler.
     */
    public final void setDdbConfigTable(Table ddbConfigTable) {
        this.ddbConfigTable = ddbConfigTable;
    }

    // todo doc
    public final void setDdbStatusTable(Table ddbStatusTable) {
        this.ddbStatusTable = ddbStatusTable;
    }

    /** SQS client, used to send the request. */
    public final void setSqsClient(AmazonSQS sqsClient) {
        this.sqsClient = sqsClient;
    }

    /**
     * Sends the request for the given scheduler. This is called by AWS Lambda on an hourly basis (5 minutes after the
     * hour to avoid weird log rotation issues). This method then checks to see the last run time of the scheduler, and
     * kicks off all scheduled events that have occurred since then.
     */
    public void schedule() {
        // Fix the current processed time, so that we have consistent scheduling.
        DateTime nowUtc = DateTime.now(DateTimeZone.UTC);

        // Get last processed time from the status table. Hash key is "hashKey" with arbitrary value
        // "BridgeMasterScheduler" since this is a singleton row in the config table.
        DateTime lastProcessedTimeUtc;
        Item statusItem = ddbStatusTable.getItem(CONFIG_KEY_HASH_KEY, HASH_KEY_MASTER_SCHEDULER);
        if (statusItem == null || !statusItem.hasAttribute(CONFIG_KEY_LAST_PROCESSED_TIME)) {
            // Scheduler has not been bootstrapped. Default to an hour ago.
            lastProcessedTimeUtc = nowUtc.minusHours(1);
        } else {
            long lastProcessedTimeMillis = statusItem.getLong(CONFIG_KEY_LAST_PROCESSED_TIME);
            lastProcessedTimeUtc = new DateTime(lastProcessedTimeMillis, DateTimeZone.UTC);
        }

        // Log times, to help with debugging.
        System.out.println("Last processed time: " + lastProcessedTimeUtc.toString());
        System.out.println("Now: " + nowUtc.toString());

        // Get scheduler configs from DDB.
        Iterable<Item> configIter = scanDdbTable(ddbConfigTable);
        for (Item oneConfig : configIter) {
            try {
                // todo wrap in try catch finally
                List<DateTime> processTimeList = getProcessingTimes(oneConfig, lastProcessedTimeUtc, nowUtc);
                for (DateTime oneProcessTime : processTimeList) {
                    process(oneConfig, oneProcessTime);
                }
            } catch (IOException ex) {
                // todo
                ex.printStackTrace();
            }
        }

        // Update last processed time in the status table.
        ddbStatusTable.putItem(new Item().withString(CONFIG_KEY_HASH_KEY, HASH_KEY_MASTER_SCHEDULER)
                .withLong(CONFIG_KEY_LAST_PROCESSED_TIME, lastProcessedTimeUtc.getMillis()));
    }

    private List<DateTime> getProcessingTimes(Item scheduleConfig, DateTime startTimeUtc, DateTime endTimeUtc) {
        String scheduleId = scheduleConfig.getString(CONFIG_KEY_SCHEDULE_ID);

        // todo test to see if this is inclusive or exclusive, and adjust the code accordingly
        // Use quartz to parse and fire cron triggers.
        String cronSchedule = scheduleConfig.getString(CONFIG_KEY_CRON_SCHEDULE);
        MutableTrigger mutableTrigger = CronScheduleBuilder.cronSchedule(cronSchedule)
                .inTimeZone(DateTimeZone.UTC.toTimeZone()).build();
        mutableTrigger.setStartTime(startTimeUtc.toDate());
        mutableTrigger.setEndTime(endTimeUtc.toDate());

        // Get processing times.
        List<DateTime> processTimeList = new ArrayList<>();
        int numExecutions = 0;
        Date lastProcessDate = startTimeUtc.toDate();
        while ((lastProcessDate = mutableTrigger.getFireTimeAfter(lastProcessDate)) != null) {
            numExecutions++;
            if (numExecutions > MAX_EXECUTIONS) {
                // todo different exception type?
                throw new IllegalArgumentException("Max executions exceeded for schedule " + scheduleId);
            }

            processTimeList.add(new DateTime(lastProcessDate, DateTimeZone.UTC));
        }

        return processTimeList;
    }

    private void process(Item scheduleConfig, DateTime processTimeUtc) throws IOException {
        // Get schedule config.
        String scheduleId = scheduleConfig.getString(CONFIG_KEY_SCHEDULE_ID);
        String sqsQueueUrl = scheduleConfig.getString(CONFIG_KEY_QUEUE_URL);

        // Resolve template vars.
        String resolvedTemplate = scheduleConfig.getString(CONFIG_KEY_REQUEST_TEMPLATE);
        for (TemplateVariable oneTemplateVar : TemplateVariable.values()) {
            resolvedTemplate = oneTemplateVar.resolve(resolvedTemplate, processTimeUtc);
        }

        // Write request to SQS
        System.out.println("Sending request: scheduleId=" + scheduleId + ", sqsQueueUrl=" + sqsQueueUrl +
                ", processTime=" + processTimeUtc.toString() + ", request=" + resolvedTemplate);
        sqsClient.sendMessage(sqsQueueUrl, resolvedTemplate);
    }

    // Helper method, because DDB scan returns an ItemCollection, which overrides iterator() to return an
    // IteratorSupport, which is not publicly exposed. THis makes it nearly impossible to mock. So we abstract it away
    // into a method that we can mock.
    Iterable<Item> scanDdbTable(Table table) {
        return table.scan();
    }
}
