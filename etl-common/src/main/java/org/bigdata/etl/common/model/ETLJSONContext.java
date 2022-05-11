package org.bigdata.etl.common.model;

import java.io.Serializable;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 *  etlJson字符串转换类
 *
 * Author: GL
 * Date: 2022-04-24
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ETLJSONContext implements Serializable {
    private ETLJSONNode source;
    private List<ETLJSONNode> transforms;
    private List<ETLJSONNode> sinks;
}
