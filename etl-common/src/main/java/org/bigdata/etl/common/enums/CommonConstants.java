package org.bigdata.etl.common.enums;

/**
 * 常用变量
 * <p>
 * Author: GL
 * Date: 2022-03-26
 */
public enum CommonConstants {
    ;

    public static final String SOURCE = "source";
    public static final String MIDDLE = "middle";
    public static final String SINK = "sink";
    public static final String PROCESS_TYPE = "processType";

    public static final String EXECUTOR_CHECK = "check";
    public static final String EXECUTOR_INIT = "init";
    public static final String EXECUTOR_PROCESS = "process";
    public static final String EXECUTOR_CLOSE = "close";

    public static final String EMPTY_STRING = "";
    public static final String SPLIT_STRING = ",";

    public static final long MILLISECOND = 1000;
    public static final long HEARTBEAT_INTERVAL = 30 * 1000;
    public static final long MIN_HEARTBEAT_INTERVAL = 10 * 1000;
    public static final int EXPIRATION_COUNT = 6;
    public static final int MIN_EXPIRATION_COUNT = 3;
    public static final int FIRST = 1;
    public static final int SECOND = 2;
    public static final int THIRD = 3;
    public static final int FOURTH = 4;
    public static final int FIFTH = 5;
    public static final int SIXTH = 6;
    public static final int SEVENTH = 7;
    public static final int TENTH = 10;
    public static final int ZERO = 0;
    public static final int END_INDEX = -1;

    public static final boolean FALSE = false;
    public static final boolean TRUE = true;
}
