package com.googlecloud.utils;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.windowing.CalendarWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.googlecloud.windowduration.ProcessWindowType;
import org.joda.time.Duration;

/**
 * Created by ekene on 5/29/16.
 */
public class ProcessWindowHelper {


    private static final String timeFormat = "yyyyMMdd_Hmm";
    private static final String dayFormat = "yyyyMMdd";
    private static final String monthFormat = "yyyyMM";
    private static final String yearFormat = "yyyy";


    public static String getWindowTimeFormat(ProcessWindowType processWindow) {
        String format = "";

        switch (processWindow) {
            case FIVE_MINUTE:
            case TEN_MINUTE:
            case FIFTEEN_MINUTE:
            case THIRTY_MINUTE:
            case ONE_HOUR:
            case TWO_HOUR:
            case THREE_HOUR:
            case SIX_HOUR:
                format = timeFormat;
                break;

            case ONE_DAY:
            case THREE_DAY:
            case ONE_WEEK:
            case TWO_WEEK:
                format = dayFormat;
                break;
            case ONE_MONTH:
                format = monthFormat;
                break;
        }


        return format;
    }

    public static PCollection<TableRow> applyWindowingStrategy(PCollection<TableRow> pCollection, ProcessWindowType processWindow) {


        PCollection<TableRow> p = null;


        switch (processWindow) {
            case FIVE_MINUTE:
            case TEN_MINUTE:
            case FIFTEEN_MINUTE:
            case THIRTY_MINUTE:
            case ONE_HOUR:
            case TWO_HOUR:
            case THREE_HOUR:
            case SIX_HOUR:
                p = pCollection.apply(Window.into(FixedWindows.of(Duration.standardMinutes(processWindow.getValue()))));
                break;

            case ONE_DAY:
            case THREE_DAY:
            case ONE_WEEK:
            case TWO_WEEK:
                p = pCollection.apply(Window.into(CalendarWindows.days(processWindow.getValue())));
                break;
            case ONE_MONTH:
                p = pCollection.apply(Window.into(CalendarWindows.months(processWindow.getValue())));
                break;
        }


        return p;

    }

}
