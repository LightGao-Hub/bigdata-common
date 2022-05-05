package org.bigdata.etl.common.enums;

import lombok.Getter;

/**
 * Author: GL
 * Date: 2022-04-21
 */
@Getter
public enum ProcessType {
    SOURCE(CommonConstants.SOURCE),
    MIDDLE(CommonConstants.MIDDLE),
    SINK(CommonConstants.SINK);

    private final String processType;

    ProcessType(String processType) {
        this.processType = processType;
    }
}
