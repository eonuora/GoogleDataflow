package com.googlecloud.windowduration;

import java.io.Serializable;

/**
 * Created by ekene on 5/29/16.
 */
public enum ProcessWindowType implements Serializable {

    FIVE_MINUTE(5),
    TEN_MINUTE(10),
    FIFTEEN_MINUTE(15),
    THIRTY_MINUTE(30),

    ONE_HOUR(1),
    TWO_HOUR(2),
    THREE_HOUR(3),
    SIX_HOUR(6),

    ONE_DAY(1),
    THREE_DAY(3),

    ONE_WEEK(1),
    TWO_WEEK(2),
    ONE_MONTH(1);


    private final Integer value;

    private ProcessWindowType(Integer value) {
        this.value = value;
    }

    public Integer getValue() {
        return value;
    }
}
