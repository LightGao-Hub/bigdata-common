package org.bigdata.etl.common.configs;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author: GL
 * Date: 2022-04-22
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DirtyConfig implements ExecutorConfig {
    private String dirtyPath;
}
