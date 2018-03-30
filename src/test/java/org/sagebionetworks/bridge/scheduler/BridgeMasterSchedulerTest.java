package org.sagebionetworks.bridge.scheduler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.sqs.AmazonSQS;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BridgeMasterSchedulerTest {
    private static final long MOCK_NOW_MILLIS = DateTime.parse("2018-03-27T19:00-07:00").getMillis();
    private static final long LAST_PROCESS_TIME_MILLIS = DateTime.parse("2018-03-27T18:30-07:00").getMillis();

    private AmazonSQS mockSqsClient;
    private BridgeMasterScheduler scheduler;
    private Table mockConfigTable;
    private Table mockStatusTable;

    @BeforeClass
    public void mockNow() {
        DateTimeUtils.setCurrentMillisFixed(MOCK_NOW_MILLIS);
    }

    @BeforeMethod
    public void before() {
        mockSqsClient = mock(AmazonSQS.class);
        mockConfigTable = mock(Table.class);
        mockStatusTable = mock(Table.class);

        scheduler = spy(new BridgeMasterScheduler());
        scheduler.setDdbConfigTable(mockConfigTable);
        scheduler.setDdbStatusTable(mockStatusTable);
        scheduler.setSqsClient(mockSqsClient);
    }

    @AfterClass
    public void unmockNow() {
        DateTimeUtils.setCurrentMillisSystem();
    }

    @Test
    public void basicCase() {
        // Mock status table.
        mockStatusWithLastProcessedTime(LAST_PROCESS_TIME_MILLIS);

        // Mock config table.
        String requestTemplate = "${startOfPreviousDay}-${endOfPreviousDay}";
        Item configItem = new Item().withString(BridgeMasterScheduler.CONFIG_KEY_SCHEDULE_ID, "schedule-1")
                .withString(BridgeMasterScheduler.CONFIG_KEY_CRON_SCHEDULE, "0 0,30 * * * ?")
                .withString(BridgeMasterScheduler.CONFIG_KEY_REQUEST_TEMPLATE, requestTemplate)
                .withString(BridgeMasterScheduler.CONFIG_KEY_QUEUE_URL, "dummy-q-url-1");
        doReturn(ImmutableList.of(configItem)).when(scheduler).scanDdbTable(mockConfigTable);

        // Execute
        scheduler.schedule();

        // Verify 1 call to SQS.
        verify(mockSqsClient).sendMessage("dummy-q-url-1",
                "2018-03-26T00:00:00.000-07:00-2018-03-26T23:59:59.999-07:00");
        verifyNoMoreInteractions(mockSqsClient);

        // Verify we wrote process time back to the status table.
        ArgumentCaptor<Item> statusItemCaptor = ArgumentCaptor.forClass(Item.class);
        verify(mockStatusTable).putItem(statusItemCaptor.capture());

        Item statusItem = statusItemCaptor.getValue();
        assertEquals(statusItem.getString(BridgeMasterScheduler.CONFIG_KEY_HASH_KEY),
                BridgeMasterScheduler.HASH_KEY_MASTER_SCHEDULER);
        assertEquals(statusItem.getLong(BridgeMasterScheduler.CONFIG_KEY_LAST_PROCESSED_TIME), MOCK_NOW_MILLIS);
    }

    @Test
    public void initialRun() {
        // Mock config table.
        String requestTemplate = "request-1 ${processTime}";
        Item configItem = new Item().withString(BridgeMasterScheduler.CONFIG_KEY_SCHEDULE_ID, "schedule-1")
                .withString(BridgeMasterScheduler.CONFIG_KEY_CRON_SCHEDULE, "0 15 * * * ?")
                .withString(BridgeMasterScheduler.CONFIG_KEY_REQUEST_TEMPLATE, requestTemplate)
                .withString(BridgeMasterScheduler.CONFIG_KEY_QUEUE_URL, "dummy-q-url-1");
        doReturn(ImmutableList.of(configItem)).when(scheduler).scanDdbTable(mockConfigTable);

        // Execute
        scheduler.schedule();

        // Verify 1 call to SQS.
        verify(mockSqsClient).sendMessage("dummy-q-url-1", "request-1 2018-03-27T18:15:00.000-07:00");
        verifyNoMoreInteractions(mockSqsClient);
    }

    @Test
    public void multipleSchedules() {
        // Mock status table.
        mockStatusWithLastProcessedTime(LAST_PROCESS_TIME_MILLIS);

        // Mock config table.
        String requestTemplate1 = "request-1 ${processTime}";
        Item configItem1 = new Item().withString(BridgeMasterScheduler.CONFIG_KEY_SCHEDULE_ID, "schedule-1")
                .withString(BridgeMasterScheduler.CONFIG_KEY_CRON_SCHEDULE, "0 0,30 * * * ?")
                .withString(BridgeMasterScheduler.CONFIG_KEY_REQUEST_TEMPLATE, requestTemplate1)
                .withString(BridgeMasterScheduler.CONFIG_KEY_QUEUE_URL, "dummy-q-url-1");

        String requestTemplate2 = "request-2 ${processTime}";
        Item configItem2 = new Item().withString(BridgeMasterScheduler.CONFIG_KEY_SCHEDULE_ID, "schedule-2")
                .withString(BridgeMasterScheduler.CONFIG_KEY_CRON_SCHEDULE, "0 15,45 * * * ?")
                .withString(BridgeMasterScheduler.CONFIG_KEY_REQUEST_TEMPLATE, requestTemplate2)
                .withString(BridgeMasterScheduler.CONFIG_KEY_QUEUE_URL, "dummy-q-url-2");

        doReturn(ImmutableList.of(configItem1, configItem2)).when(scheduler).scanDdbTable(mockConfigTable);

        // Execute
        scheduler.schedule();

        // Verify 2 calls to SQS.
        verify(mockSqsClient).sendMessage("dummy-q-url-1", "request-1 2018-03-27T19:00:00.000-07:00");
        verify(mockSqsClient).sendMessage("dummy-q-url-2", "request-2 2018-03-27T18:45:00.000-07:00");
        verifyNoMoreInteractions(mockSqsClient);
    }

    @Test
    public void singleScheduleWithMultipleEvents() {
        // Mock status table.
        mockStatusWithLastProcessedTime(LAST_PROCESS_TIME_MILLIS);

        // Mock config table.
        String requestTemplate1 = "request-1 ${processTime}";
        Item configItem1 = new Item().withString(BridgeMasterScheduler.CONFIG_KEY_SCHEDULE_ID, "schedule-1")
                .withString(BridgeMasterScheduler.CONFIG_KEY_CRON_SCHEDULE, "0 0,15,30,45 * * * ?")
                .withString(BridgeMasterScheduler.CONFIG_KEY_REQUEST_TEMPLATE, requestTemplate1)
                .withString(BridgeMasterScheduler.CONFIG_KEY_QUEUE_URL, "dummy-q-url-1");
        doReturn(ImmutableList.of(configItem1)).when(scheduler).scanDdbTable(mockConfigTable);

        // Execute
        scheduler.schedule();

        // Verify 2 calls to SQS.
        verify(mockSqsClient).sendMessage("dummy-q-url-1", "request-1 2018-03-27T18:45:00.000-07:00");
        verify(mockSqsClient).sendMessage("dummy-q-url-1", "request-1 2018-03-27T19:00:00.000-07:00");
        verifyNoMoreInteractions(mockSqsClient);
    }

    @Test
    public void noEvents() {
        // Mock status table.
        mockStatusWithLastProcessedTime(LAST_PROCESS_TIME_MILLIS);

        // Mock config table.
        String requestTemplate1 = "request-1 ${processTime}";
        Item configItem1 = new Item().withString(BridgeMasterScheduler.CONFIG_KEY_SCHEDULE_ID, "schedule-1")
                .withString(BridgeMasterScheduler.CONFIG_KEY_CRON_SCHEDULE, "0 15 * * * ?")
                .withString(BridgeMasterScheduler.CONFIG_KEY_REQUEST_TEMPLATE, requestTemplate1)
                .withString(BridgeMasterScheduler.CONFIG_KEY_QUEUE_URL, "dummy-q-url-1");
        doReturn(ImmutableList.of(configItem1)).when(scheduler).scanDdbTable(mockConfigTable);

        // Execute
        scheduler.schedule();

        // Verify 2 calls to SQS.
        verifyZeroInteractions(mockSqsClient);
    }

    @Test
    public void errorHandling() {
        // Mock status table.
        mockStatusWithLastProcessedTime(LAST_PROCESS_TIME_MILLIS);

        // Mock config table.
        String requestTemplate1 = "request-1 ${processTime}";
        Item configItem1 = new Item().withString(BridgeMasterScheduler.CONFIG_KEY_SCHEDULE_ID, "schedule-1")
                .withString(BridgeMasterScheduler.CONFIG_KEY_CRON_SCHEDULE, "0 0,30 * * * ?")
                .withString(BridgeMasterScheduler.CONFIG_KEY_REQUEST_TEMPLATE, requestTemplate1)
                .withString(BridgeMasterScheduler.CONFIG_KEY_QUEUE_URL, "dummy-q-url-1");

        String requestTemplate2 = "request-2 ${processTime}";
        Item configItem2 = new Item().withString(BridgeMasterScheduler.CONFIG_KEY_SCHEDULE_ID, "schedule-2")
                .withString(BridgeMasterScheduler.CONFIG_KEY_CRON_SCHEDULE, "0 15,45 * * * ?")
                .withString(BridgeMasterScheduler.CONFIG_KEY_REQUEST_TEMPLATE, requestTemplate2)
                .withString(BridgeMasterScheduler.CONFIG_KEY_QUEUE_URL, "dummy-q-url-2");

        doReturn(ImmutableList.of(configItem1, configItem2)).when(scheduler).scanDdbTable(mockConfigTable);

        // For this test, calls to dummy-q-url-1 will throw.
        when(mockSqsClient.sendMessage(eq("dummy-q-url-1"), any())).thenThrow(RuntimeException.class);

        // Execute
        scheduler.schedule();

        // Verify 2 calls to SQS.
        verify(mockSqsClient).sendMessage("dummy-q-url-1", "request-1 2018-03-27T19:00:00.000-07:00");
        verify(mockSqsClient).sendMessage("dummy-q-url-2", "request-2 2018-03-27T18:45:00.000-07:00");
        verifyNoMoreInteractions(mockSqsClient);
    }

    private void mockStatusWithLastProcessedTime(long lastProcessTimeMillis) {
        Item statusItem = new Item()
                .withString(BridgeMasterScheduler.CONFIG_KEY_HASH_KEY, BridgeMasterScheduler.HASH_KEY_MASTER_SCHEDULER)
                .withLong(BridgeMasterScheduler.CONFIG_KEY_LAST_PROCESSED_TIME, lastProcessTimeMillis);
        when(mockStatusTable.getItem(BridgeMasterScheduler.CONFIG_KEY_HASH_KEY,
                BridgeMasterScheduler.HASH_KEY_MASTER_SCHEDULER)).thenReturn(statusItem);
    }
}