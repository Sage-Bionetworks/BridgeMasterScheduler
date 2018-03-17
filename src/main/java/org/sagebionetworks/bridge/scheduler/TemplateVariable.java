package org.sagebionetworks.bridge.scheduler;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public enum TemplateVariable {
    END_OF_PREVIOUS_DAY("endOfPreviousDay") {
        @Override
        protected String getReplacementValue(DateTime processTimeLocal) {
            return processTimeLocal.minusDays(1).withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59)
                    .withMillisOfSecond(999).toString();
        }
    },

    START_OF_DAY("startOfDay") {
        @Override
        protected String getReplacementValue(DateTime processTimeLocal) {
            return processTimeLocal.withTimeAtStartOfDay().toString();
        }
    },

    START_OF_DAY_ONE_WEEK_AGO("startOfDayOneWeekAgo") {
        @Override
        protected String getReplacementValue(DateTime processTimeLocal) {
            return processTimeLocal.minusDays(7).withTimeAtStartOfDay().toString();
        }
    },

    START_OF_HOUR("startOfHour") {
        @Override
        protected String getReplacementValue(DateTime processTimeLocal) {
            return processTimeLocal.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0).toString();
        }
    },

    START_OF_PREVIOUS_DAY("startOfPreviousDay") {
        @Override
        protected String getReplacementValue(DateTime processTimeLocal) {
            return processTimeLocal.minusDays(1).withTimeAtStartOfDay().toString();
        }
    },

    YESTERDAYS_DATE("yesterdaysDate") {
        @Override
        protected String getReplacementValue(DateTime processTimeLocal) {
            return processTimeLocal.minusDays(1).toLocalDate().toString();
        }
    };

    // For now, scheduler will assume Seattle time for calculating date.
    private static final DateTimeZone LOCAL_TIME_ZONE = DateTimeZone.forID("America/Los_Angeles");

    private String varPattern;

    TemplateVariable(String varName) {
        varPattern = "${" + varName + "}";
    }

    protected abstract String getReplacementValue(DateTime processTimeLocal);

    public String resolve(String template, DateTime processTimeUtc) {
        // Convert process time to local timezone, since all of the date calculations are based on local time zone.
        DateTime processTimeLocal = processTimeUtc.withZone(LOCAL_TIME_ZONE);

        // Resolve the variable and replace the variable in the template.
        String replacement = getReplacementValue(processTimeLocal);
        return template.replace(varPattern, replacement);
    }
}
