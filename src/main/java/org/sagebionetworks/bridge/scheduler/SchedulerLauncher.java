package org.sagebionetworks.bridge.scheduler;

import java.io.IOException;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

/**
 * <p>
 * Scheduler launcher. Main function is used for local testing during development. The public launch() method is called
 * by AWS Lambda. The launcher initializes the DDB and SQS clients, creates the scheduler, launches the scheduler,
 * then cleans up afterwards.
 * </p>
 * <p>
 * We don't use Spring here because the scheduler is very simple and Lambda apps are intended to be very lightweight.
 * The overhead of launching a Spring context with every invocation of the scheduler is fairly large given how simple
 * it is to manually wire it up.
 * </p>
 */
public class SchedulerLauncher {
    /** Main method, used for local testing during development. See README for more instructions on how to invoke. */
    public static void main(String[] args) throws IOException {
        launch(args[0]);
    }

    /**
     * Called by AWS Lambda.
     *
     * @param input
     *         required by AWS Lambda, but ignored because it doesn't include any useful information
     * @param context
     *         AWS Lambda context, primarily needed for the function name (scheduler name)
     */
    @SuppressWarnings("unused")
    public static void launch(Object input, Context context) {
        // Lambda function name is scheduler name
        String schedulerName = context.getFunctionName();
        launch(schedulerName);
    }

    /**
     * Internal launch() method to abstract away main method details and AWS Lambda details.
     *
     * @param schedulerName
     *         scheduler name, used as a config key
     */
    private static void launch(String schedulerName) {
        System.out.println("Initializing " + schedulerName + "...");

        // Set up DDB client and tables. Table names are "[schedulerName]-config" and "[schedulerName]-status".
        DynamoDB ddbClient = new DynamoDB(AmazonDynamoDBClientBuilder.defaultClient());
        Table ddbConfigTable = ddbClient.getTable(schedulerName + "-config");
        Table ddbStatusTable = ddbClient.getTable(schedulerName + "-status");

        // set up SQS client
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();

        // set up scheduler
        BridgeMasterScheduler scheduler = new BridgeMasterScheduler();
        scheduler.setDdbConfigTable(ddbConfigTable);
        scheduler.setDdbStatusTable(ddbStatusTable);
        scheduler.setSqsClient(sqsClient);

        // launch scheduler
        System.out.println("Launching " + schedulerName + "...");
        try {
            scheduler.schedule();
        } finally {
            // shut down AWS clients
            ddbClient.shutdown();
            sqsClient.shutdown();
        }
    }
}
