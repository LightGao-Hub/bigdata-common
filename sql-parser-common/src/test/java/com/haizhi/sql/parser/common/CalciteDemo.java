package com.haizhi.sql.parser.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.junit.Test;


/**
 * Author: GL
 * Date: 2022-03-07
 */
public class CalciteDemo {
    public static SqlParserPos zero = SqlParserPos.ZERO;

    public static SqlSyntax syntax = SqlSyntax.FUNCTION;

    public static final SqlParser.Config parserConfig = SqlParser.config()
            .withQuoting(Quoting.DOUBLE_QUOTE)
            .withConformance(SqlConformanceEnum.MYSQL_5)
            .withQuotedCasing(Casing.UNCHANGED)
            .withUnquotedCasing(Casing.UNCHANGED);

    // 根据不同方言进行解析
    @Test
    public void demo1() {
        String sql = "select id, substring('name', 2, 2) as name from person where age > 10 order by id desc ";
        SqlParser parser = SqlParser.create(sql, parserConfig);
        SqlNode sqlNode;
        try {
            sqlNode = parser.parseStmt(); // sql语法解析成AST语法树，此Node节点即是根节点
            SqlString sqlString = sqlNode.toSqlString(new OracleSqlDialect(SqlDialect.EMPTY_CONTEXT)); // 通过toSqlString函数传递不同的SqlDialect方言，即可实现一套sql，到处运行
            System.out.println(sqlString);
        } catch (SqlParseException e) {
            throw new RuntimeException("parse failed: " + e.getMessage(), e);
        }
    }


    // 自定义方言需要重写unparseCall函数
    @Test
    public void demo2() {
        String sql = "select mydefind(f_varchar) from person where age > 10 order by id desc";
        SqlParser parser = SqlParser.create(sql, parserConfig);
        SqlNode sqlNode;
        try {
            sqlNode = parser.parseStmt(); // sql语法解析成AST语法树，此Node节点即是根节点
            SqlString sqlString = sqlNode.toSqlString(new OracleSqlDialect(SqlDialect.EMPTY_CONTEXT) { // 对于自定义函数需要自己重写对应方言解析，同样可实现一套sql，到处运行
                @Override
                public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
                    final String name = call.getOperator().getName();
                    switch (name) {
                        case "mydefind":
                            SqlCall newSqlCall = buildSqlCall("char_length", SqlLibraryEx.ORACLE.toString(), call.operand(0));
                            super.unparseCall(writer, newSqlCall, leftPrec, rightPrec);
                            break;
                        default:
                            super.unparseCall(writer, call, leftPrec, rightPrec);
                    }
                }
            });
            System.out.println(sqlString);
        } catch (SqlParseException e) {
            throw new RuntimeException("parse failed: ", e);
        }
    }

    private static SqlCall buildSqlCall(String opName, String type, SqlNode...nodes) {
        return findSysOp(opName, type).createCall(zero, nodes);
    }

    /**
     * 查找系统函数
     * 先查找标准库SqlStdOperatorTable
     * 然后是 扩展库 SqlLibraryOperatorTableFactory
     * 在然后是补充的扩展库SqlLibraryOperatorTableFactoryEx
     * @param opName
     * @param type
     * @return
     */
    private static SqlOperator findSysOp(String opName, String type) {
        List<SqlOperator> result = new ArrayList<>(4);

        SqlOperatorTable ops = SqlStdOperatorTable.instance();

        SqlNameMatcher matcher = SqlNameMatchers.withCaseSensitive(false);

        // 先查找标准库SqlStdOperatorTable
        ops.lookupOperatorOverloads(new SqlIdentifier(opName, zero), null, SqlSyntax.FUNCTION, result, matcher);
        ops.lookupOperatorOverloads(new SqlIdentifier(opName, zero), null, SqlSyntax.BINARY, result, matcher);
        if (!result.isEmpty()) {
            return result.get(0);
        }

        if (type != null) {
            // 查找扩展库 SqlLibraryOperatorTableFactory
            SqlLibrary lib = null;
            try {
                lib = SqlLibrary.valueOf(type);
            } catch (Exception e) {
            }

            if (lib != null) {
                ops = SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(lib);
                if (ops != null) {
                    ops.lookupOperatorOverloads(new SqlIdentifier(opName, zero), null, SqlSyntax.FUNCTION, result,
                            matcher);
                    ops.lookupOperatorOverloads(new SqlIdentifier(opName, zero), null, SqlSyntax.BINARY, result,
                            matcher);
                    if (!result.isEmpty()) {
                        return result.get(0);
                    }
                }
            }
            // 查找扩展库SqlLibraryOperatorTableFactoryEx
            ops = SqlLibraryOperatorTableFactoryEx.INSTANCE.getOperatorTable(SqlLibraryEx.valueOf(type));
            if (ops != null) {
                ops.lookupOperatorOverloads(new SqlIdentifier(opName, zero), null, syntax, result, matcher);
                if (!result.isEmpty()) {
                    return result.get(0);
                }
            }
        }
        throw new RuntimeException("not found sys func:[" + opName + "] " + " in [" + type + "]");
    }
}
