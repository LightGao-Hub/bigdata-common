package org.bigdata.common.clickhouse;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

/**
 *  测试clickhouse-jdbc连接驱动
 * Author: GL
 * Date: 2022-01-31
 */
public class ClickHouseDemo {

    private static Connection connection = null;

    static {
        try {
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");// 驱动包
            String url = "jdbc:clickhouse://192.168.1.5:8123/default";// url路径
            String user = "default";// 账号
            String password = "";// 密码
            connection = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreate() throws Exception {
        final String sql = "CREATE TABLE IF NOT EXISTS default.test_json"
                         + "("
                         + "    `id` Int8,"
                         + "    `name` String,"
                         + "    `age` Int8"
                         + ")ENGINE = Memory";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.execute();
        ps.close();
    }

    @Test
    public void testJsonInsert() throws Exception {
        final String json1 = "{\"id\":1,\"name\":\"a\",\"age\":27}";
        final String sql = "insert into default.test_json FORMAT JSONEachRow ";
        final StringBuffer stringBuffer = new StringBuffer(sql)
                .append(json1).append(",")
                .append(json1).append(",")
                .append(json1);
        PreparedStatement ps = connection.prepareStatement(stringBuffer.toString());
        ps.execute();
        ps.close();
    }

    @Test
    public void testSelect() throws Exception {
        final String sql = "select * from default.test_json";
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(metaData.getColumnName(i) + ":" + resultSet.getString(i) + "\t");
            }
            System.out.println();
        }
    }
}

