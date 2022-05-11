package org.bigdata.etl.common.model;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 *  所有配置类需实现此接口
 *
 * Author: GL
 * Date: 2022-04-24
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ETLJSONNode implements Serializable {
    private String processType;
    private Object config;
}
