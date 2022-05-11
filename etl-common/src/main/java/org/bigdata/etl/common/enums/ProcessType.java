package org.bigdata.etl.common.enums;

import lombok.Getter;

/**
 * Author: GL
 * Date: 2022-04-21
 */
@Getter
public enum ProcessType {
    SOURCE(CommonConstants.SOURCE),
    TRANSFORM(CommonConstants.TRANSFORM),
    SINK(CommonConstants.SINK);

    private final String processType;

    ProcessType(String processType) {
        this.processType = processType;
    }
}
