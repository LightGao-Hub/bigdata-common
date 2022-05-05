package org.bigdata.etl.common.enums;

import lombok.Getter;

/**
 * Author: GL
 * Date: 2022-04-21
 */
@Getter
public enum ExecutorType {
    CHECK(CommonConstants.EXECUTOR_CHECK),
    INIT(CommonConstants.EXECUTOR_INIT),
    PROCESS(CommonConstants.EXECUTOR_PROCESS),
    CLOSE(CommonConstants.EXECUTOR_CLOSE);

    private final String executorType;

    ExecutorType(String executorType) {
        this.executorType = executorType;
    }
}
