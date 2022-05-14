package org.bigdata.etl.common.java.test.configs;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Author: GL
 * Date: 2022-04-22
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FileConfig implements Serializable {
    private String path;
}
