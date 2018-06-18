package org.sagebionetworks.bridge.scheduler;

import static org.testng.Assert.assertEquals;

import org.joda.time.DateTime;
import org.testng.annotations.Test;

public class TemplateVariableTest {
    private static final DateTime PROCESS_TIME_UTC = DateTime.parse("2018-03-28T04:21:45.862Z");

    @Test
    public void endOfPreviousDay() {
        String output = TemplateVariable.END_OF_PREVIOUS_DAY.resolve("${endOfPreviousDay}", PROCESS_TIME_UTC);
        assertEquals(output, "2018-03-26T23:59:59.999-07:00");
    }

    @Test
    public void processTime() {
        String output = TemplateVariable.PROCESS_TIME.resolve("${processTime}", PROCESS_TIME_UTC);
        assertEquals(output, "2018-03-27T21:21:45.862-07:00");
    }

    @Test
    public void startOfDay() {
        String output = TemplateVariable.START_OF_DAY.resolve("${startOfDay}", PROCESS_TIME_UTC);
        assertEquals(output, "2018-03-27T00:00:00.000-07:00");
    }

    @Test
    public void startOfDayOneWeekAgo() {
        String output = TemplateVariable.START_OF_DAY_ONE_WEEK_AGO.resolve("${startOfDayOneWeekAgo}",
                PROCESS_TIME_UTC);
        assertEquals(output, "2018-03-20T00:00:00.000-07:00");
    }

    @Test
    public void startOfHour() {
        String output = TemplateVariable.START_OF_HOUR.resolve("${startOfHour}", PROCESS_TIME_UTC);
        assertEquals(output, "2018-03-27T21:00:00.000-07:00");
    }

    @Test
    public void startOfPreviousDay() {
        String output = TemplateVariable.START_OF_PREVIOUS_DAY.resolve("${startOfPreviousDay}",
                PROCESS_TIME_UTC);
        assertEquals(output, "2018-03-26T00:00:00.000-07:00");
    }

    @Test
    public void todaysDate() {
        String output = TemplateVariable.TODAYS_DATE.resolve("${todaysDate}", PROCESS_TIME_UTC);
        assertEquals(output, "2018-03-27");
    }

    @Test
    public void yesterdaysDate() {
        String output = TemplateVariable.YESTERDAYS_DATE.resolve("${yesterdaysDate}", PROCESS_TIME_UTC);
        assertEquals(output, "2018-03-26");
    }

    @Test
    public void templateWithText() {
        String output = TemplateVariable.YESTERDAYS_DATE.resolve("before ${yesterdaysDate} after",
                PROCESS_TIME_UTC);
        assertEquals(output, "before 2018-03-26 after");
    }

    @Test
    public void multipleInstances() {
        String output = TemplateVariable.YESTERDAYS_DATE.resolve("${yesterdaysDate} ${yesterdaysDate}",
                PROCESS_TIME_UTC);
        assertEquals(output, "2018-03-26 2018-03-26");
    }

    @Test
    public void ignoresOtherVariables() {
        String output = TemplateVariable.YESTERDAYS_DATE.resolve("${badVar}", PROCESS_TIME_UTC);
        assertEquals(output, "${badVar}");
    }
}
