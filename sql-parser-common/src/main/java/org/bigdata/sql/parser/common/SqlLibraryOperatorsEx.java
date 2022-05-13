package org.bigdata.sql.parser.common;
// CHECKSTYLE:OFF

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SameOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeFamily;

/**
 * Defines functions and operators that are not part of standard SQL but belong
 * to one or more other dialects of SQL.
 *
 * <p>
 * They are read by {@link SqlLibraryOperatorTableFactory} into instances of
 * {@link SqlOperatorTable} that contain functions and operators for particular
 * libraries.
 * 两种SqlFunction的构造参数使用   用SqlIdentifier参数的是绑定大小写的  其他的是大小写不敏感的
 */
public abstract class SqlLibraryOperatorsEx {


    private SqlLibraryOperatorsEx() {
    }


    @LibraryOperatorEx(libraries = {SqlLibraryEx.MYSQL})
    public static final SqlFunction SUBSTRING_INDEX = new SqlFunction("SUBSTRING_INDEX", SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_2000, null, OperandTypes.STRING, SqlFunctionCategory.SYSTEM);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.MYSQL})
    public static final SqlFunction NOW = new SqlFunction("NOW", SqlKind.OTHER_FUNCTION, ReturnTypes.DATE, null,
            OperandTypes.family(), SqlFunctionCategory.SYSTEM);


    @LibraryOperatorEx(libraries = {SqlLibraryEx.MYSQL, SqlLibraryEx.ANALYTICDB, SqlLibraryEx.HIVE})
    public static final SqlFunction UNIX_TIMESTAMP = new SqlFunction("UNIX_TIMESTAMP", SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER, null, OperandTypes.TIMESTAMP, SqlFunctionCategory.TIMEDATE);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.MYSQL, SqlLibraryEx.ANALYTICDB, SqlLibraryEx.HUBBLE})
    public static final SqlFunction WEEKDAY = new SqlFunction("WEEKDAY", SqlKind.OTHER_FUNCTION, ReturnTypes.INTEGER,
            null, OperandTypes.ANY, SqlFunctionCategory.TIMEDATE);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.MYSQL, SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction DATE = new SqlFunction("DATE", SqlKind.OTHER_FUNCTION, ReturnTypes.DATE_NULLABLE,
            null, OperandTypes.STRING, SqlFunctionCategory.TIMEDATE);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.MYSQL, SqlLibraryEx.GREENPLUM})
    public static final SqlFunction GREATEST = new SqlFunction("GREATEST", SqlKind.OTHER_FUNCTION, ReturnTypes.ARG0,
            null, OperandTypes.ANY, SqlFunctionCategory.SYSTEM);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.MYSQL, SqlLibraryEx.GREENPLUM})
    public static final SqlFunction LEAST = new SqlFunction("LEAST", SqlKind.OTHER_FUNCTION, ReturnTypes.ARG0, null,
            OperandTypes.ANY, SqlFunctionCategory.SYSTEM);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.MYSQL})
    public static final SqlFunction LPAD = new SqlFunction("LPAD", SqlKind.OTHER_FUNCTION, ReturnTypes.ARG0, null,
            OperandTypes.ANY, SqlFunctionCategory.SYSTEM);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.MYSQL, SqlLibraryEx.ANALYTICDB, SqlLibraryEx.GREENPLUM})
    public static final SqlFunction IF = new SqlFunction("IF", SqlKind.IF, null, null,
            OperandTypes.and(OperandTypes.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.ANY, SqlTypeFamily.ANY),
                    new SameOperandTypeChecker(3) {
        @Override
        protected List<Integer> getOperandList(int operandCount) {
            return ImmutableList.of(1, 2);
        }
    }), SqlFunctionCategory.SYSTEM);


    @LibraryOperatorEx(libraries = {SqlLibraryEx.ORACLE, SqlLibraryEx.POSTGRESQL, SqlLibraryEx.GREENPLUM})
    public static final SqlFunction TO_CHAR = new SqlFunction("TO_CHAR", SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_2000_NULLABLE, null, OperandTypes.ANY, SqlFunctionCategory.TIMEDATE);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.ORACLE})
    public static final SqlFunction NUMTODSINTERVAL = new SqlFunction("NUMTODSINTERVAL", SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER, null, OperandTypes.family(SqlTypeFamily.INTEGER, SqlTypeFamily.ANY),
            SqlFunctionCategory.TIMEDATE);


    @LibraryOperatorEx(libraries = {SqlLibraryEx.POSTGRESQL, SqlLibraryEx.GREENPLUM})
    public static final SqlFunction DATE_PG = new SqlFunction("DATE", SqlKind.OTHER_FUNCTION, ReturnTypes.DATE, null,
            OperandTypes.family(SqlTypeFamily.INTEGER, SqlTypeFamily.ANY), SqlFunctionCategory.TIMEDATE);

    //系统库的||限制了参数类型
    @LibraryOperatorEx(libraries = {SqlLibraryEx.POSTGRESQL, SqlLibraryEx.GREENPLUM})
    public static final SqlFunction CONCAT_FUNCTION = new SqlFunction("CONCAT", SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_2000, null, OperandTypes.ANY, SqlFunctionCategory.STRING);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.POSTGRESQL, SqlLibraryEx.GREENPLUM})
    public static final SqlFunction ENCODE = new SqlFunction("ENCODE", SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_2000, null, OperandTypes.STRING_STRING, SqlFunctionCategory.STRING);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.POSTGRESQL, SqlLibraryEx.GREENPLUM})
    public static final SqlFunction DECODE = new SqlFunction("DECODE", SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_2000, null, OperandTypes.STRING_STRING, SqlFunctionCategory.STRING);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.POSTGRESQL, SqlLibraryEx.GREENPLUM, SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction LOG = new SqlFunction("LOG", SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE, null,
            OperandTypes.NUMERIC, SqlFunctionCategory.NUMERIC);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.POSTGRESQL, SqlLibraryEx.GREENPLUM})
    public static final SqlFunction REGEXP_REPLACE = new SqlFunction("REGEXP_REPLACE", SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_2000, InferTypes.RETURN_TYPE, OperandTypes.family(SqlTypeFamily.STRING,
            SqlTypeFamily.STRING, SqlTypeFamily.INTEGER), SqlFunctionCategory.STRING);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.POSTGRESQL, SqlLibraryEx.GREENPLUM})

    public static final SqlFunction REGEXP_SPLIT_TO_ARRAY = new SqlFunction("REGEXP_SPLIT_TO_ARRAY",
            SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_2000, InferTypes.RETURN_TYPE,
            OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.INTEGER),
            SqlFunctionCategory.STRING);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.POSTGRESQL, SqlLibraryEx.GREENPLUM})
    public static final SqlFunction REGEXP_MATCH = new SqlFunction("REGEXP_MATCH", SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_2000, InferTypes.RETURN_TYPE, OperandTypes.family(SqlTypeFamily.STRING,
            SqlTypeFamily.STRING, SqlTypeFamily.INTEGER), SqlFunctionCategory.STRING);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.POSTGRESQL, SqlLibraryEx.GREENPLUM})
    public static final SqlFunction REGEXP_MATCHES = new SqlFunction("REGEXP_MATCHES", SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_2000, InferTypes.RETURN_TYPE, OperandTypes.family(SqlTypeFamily.STRING,
            SqlTypeFamily.STRING, SqlTypeFamily.INTEGER), SqlFunctionCategory.STRING);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction MAX = new SqlFunction("MAX", SqlKind.OTHER_FUNCTION, ReturnTypes.ARG0, null,
            OperandTypes.ANY, SqlFunctionCategory.SYSTEM);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction TO_BASE64 = new SqlFunction("TO_BASE64", SqlKind.OTHER_FUNCTION, ReturnTypes.ARG0
            , null, OperandTypes.ANY, SqlFunctionCategory.SYSTEM);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction FROM_BASE64 = new SqlFunction("FROM_BASE64", SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0, null, OperandTypes.ANY, SqlFunctionCategory.SYSTEM);


////////////////////////////////////////////////////////////////////////////////////////////////////    


    @LibraryOperatorEx(libraries = {SqlLibraryEx.HIVE})
    public static final SqlFunction WEEKOFYEAR = new SqlFunction("WEEKOFYEAR", SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER, null, OperandTypes.ANY, SqlFunctionCategory.TIMEDATE);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.HIVE, SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction DATE_FORMAT = new SqlFunction("DATE_FORMAT", SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER, null, OperandTypes.ANY, SqlFunctionCategory.TIMEDATE);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.HIVE, SqlLibraryEx.ANALYTICDB, SqlLibraryEx.HUBBLE})
    public static final SqlFunction DATEDIFF = new SqlFunction("DATEDIFF", SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER, null, OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME),
            SqlFunctionCategory.TIMEDATE);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.HIVE})
    public static final SqlFunction PMOD = new SqlFunction("PMOD", SqlKind.OTHER_FUNCTION, ReturnTypes.INTEGER, null,
            OperandTypes.ANY, SqlFunctionCategory.STRING);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.HIVE})
    public static final SqlFunction DATE_SUB = new SqlFunction("DATE_SUB", SqlKind.OTHER_FUNCTION, ReturnTypes.DATE,
            null, OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER), SqlFunctionCategory.TIMEDATE);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.HIVE})
    public static final SqlFunction SPLIT = new SqlFunction("SPLIT", SqlKind.OTHER_FUNCTION, ReturnTypes.TO_ARRAY,
            null, OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING), SqlFunctionCategory.STRING);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.HIVE})
    public static final SqlFunction INDEX = new SqlFunction("INDEX", SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0_NULLABLE, null, OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.INTEGER),
            SqlFunctionCategory.SYSTEM);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.HIVE})
    public static final SqlFunction ARRAY = new SqlFunction("ARRAY", SqlKind.OTHER_FUNCTION, ReturnTypes.TO_ARRAY,
            null, OperandTypes.ANY, SqlFunctionCategory.SYSTEM);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.HIVE})
    public static final SqlFunction DAYOFWEEK = new SqlFunction("dayofweek", SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER, null, OperandTypes.family(SqlTypeFamily.DATETIME), SqlFunctionCategory.TIMEDATE);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.HIVE, SqlLibraryEx.ORACLE})
    public static final SqlFunction ADD_MONTHS = new SqlFunction("ADD_MONTHS", SqlKind.OTHER_FUNCTION,
            ReturnTypes.DATE, null, OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.INTEGER),
            SqlFunctionCategory.TIMEDATE);


    @LibraryOperatorEx(libraries = {SqlLibraryEx.HIVE})
    public static final SqlFunction DATE_ADD = new SqlFunction("DATE_ADD", SqlKind.OTHER_FUNCTION, ReturnTypes.DATE,
            null, OperandTypes.DATE, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.HIVE})
    public static final SqlFunction DATE_HIVE = new SqlFunction("DATE", SqlKind.OTHER_FUNCTION, ReturnTypes.DATE,
            null, OperandTypes.CHARACTER, SqlFunctionCategory.TIMEDATE);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.HIVE, SqlLibraryEx.MYSQL})
    public static final SqlFunction DAY = new SqlFunction("DAY", SqlKind.OTHER_FUNCTION, ReturnTypes.INTEGER, null,
            OperandTypes.DATE, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.HIVE, SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction MONTH = new SqlFunction("MONTH", SqlKind.OTHER_FUNCTION, ReturnTypes.INTEGER,
            null, OperandTypes.DATE, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.HIVE, SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction YEAR = new SqlFunction("YEAR", SqlKind.OTHER_FUNCTION, ReturnTypes.INTEGER, null,
            OperandTypes.DATE, SqlFunctionCategory.TIMEDATE);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.HIVE, SqlLibraryEx.MSSQL})
    public static final SqlFunction CONCAT = new SqlFunction("CONCAT", SqlKind.OTHER_FUNCTION,
            ReturnTypes.MULTIVALENT_STRING_SUM_PRECISION_NULLABLE, InferTypes.RETURN_TYPE,
            OperandTypes.repeat(SqlOperandCountRanges.from(2), OperandTypes.STRING), SqlFunctionCategory.STRING);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.HIVE})
    public static final SqlFunction REGEXP_EXTRACT = new SqlFunction("REGEXP_EXTRACT", SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_2000, InferTypes.RETURN_TYPE, OperandTypes.family(SqlTypeFamily.STRING,
            SqlTypeFamily.STRING, SqlTypeFamily.INTEGER), SqlFunctionCategory.STRING);

    ////////////////////////////////////////////////////////////////////////////////////////
    @LibraryOperatorEx(libraries = {SqlLibraryEx.MSSQL})
    public static final SqlFunction DATEPART = new SqlFunction("DATEPART", SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER, InferTypes.RETURN_TYPE, OperandTypes.family(SqlTypeFamily.STRING,
            SqlTypeFamily.DATETIME), SqlFunctionCategory.STRING);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.MSSQL})
    public static final SqlFunction DATEADD = new SqlFunction("DATEADD", SqlKind.OTHER_FUNCTION, ReturnTypes.INTEGER,
            InferTypes.RETURN_TYPE, OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER,
            SqlTypeFamily.DATETIME), SqlFunctionCategory.STRING);


////////////////////////////////////////////////////////////////////////////////////////


    ////////////////////////////////////////////////////////////////////////////////////////////////

    @LibraryOperatorEx(libraries = {SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction SAFE_CHECK_STRING = new SqlFunction("SAFE_CHECK_STRING", SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0, InferTypes.RETURN_TYPE, OperandTypes.STRING, SqlFunctionCategory.STRING);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction CONCAT_AN = new SqlFunction("CONCAT", SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_2000, InferTypes.RETURN_TYPE, OperandTypes.ANY, SqlFunctionCategory.STRING);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction STR_TO_DATE = new SqlFunction("STR_TO_DATE", SqlKind.OTHER_FUNCTION,
            ReturnTypes.DATE_NULLABLE, null, OperandTypes.STRING_STRING, SqlFunctionCategory.TIMEDATE);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction TO_DATE_AN = new SqlFunction("TO_DATE", SqlKind.OTHER_FUNCTION,
            ReturnTypes.DATE_NULLABLE, null, OperandTypes.STRING, SqlFunctionCategory.TIMEDATE);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.ORACLE, SqlLibraryEx.GREENPLUM})
    public static final SqlFunction TO_DATE = new SqlFunction("TO_DATE", SqlKind.OTHER_FUNCTION,
            ReturnTypes.DATE_NULLABLE, null, OperandTypes.STRING_STRING, SqlFunctionCategory.TIMEDATE);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.GREENPLUM})
    public static final SqlFunction TO_TIMESTAMP = new SqlFunction("TO_TIMESTAMP", SqlKind.OTHER_FUNCTION,
            ReturnTypes.DATE_NULLABLE, null, OperandTypes.STRING_STRING, SqlFunctionCategory.TIMEDATE);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.GREENPLUM, SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction LENGTH = new SqlFunction("LENGTH", SqlKind.OTHER_FUNCTION,
            ReturnTypes.DATE_NULLABLE, null, OperandTypes.STRING_STRING, SqlFunctionCategory.STRING);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.GREENPLUM})
    public static final SqlFunction TRUNC = new SqlFunction("TRUNC", SqlKind.OTHER_FUNCTION,
            ReturnTypes.DATE_NULLABLE, null, OperandTypes.NUMERIC, SqlFunctionCategory.NUMERIC);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.GREENPLUM})
    public static final SqlFunction RANDOM = new SqlFunction("RANDOM", SqlKind.OTHER_FUNCTION,
            ReturnTypes.DATE_NULLABLE, null, OperandTypes.ANY_ANY, SqlFunctionCategory.NUMERIC);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.GREENPLUM})
    public static final SqlFunction CONVERT_FROM = new SqlFunction("CONVERT_FROM", SqlKind.OTHER_FUNCTION,
            ReturnTypes.DATE_NULLABLE, null, OperandTypes.ANY_ANY, SqlFunctionCategory.STRING);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.GREENPLUM})
    public static final SqlFunction PERCENTILE_CONT = new SqlFunction("PERCENTILE_CONT", SqlKind.OTHER_FUNCTION,
            ReturnTypes.DATE_NULLABLE, null, OperandTypes.ANY_ANY, SqlFunctionCategory.NUMERIC);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction ADDDATE = new SqlFunction("ADDDATE", SqlKind.OTHER_FUNCTION,
            ReturnTypes.DATE_NULLABLE, null, OperandTypes.ANY, SqlFunctionCategory.TIMEDATE);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction DATE_ADD_AN = new SqlFunction("DATE_ADD", SqlKind.OTHER_FUNCTION,
            ReturnTypes.DATE_NULLABLE, null, OperandTypes.ANY, SqlFunctionCategory.TIMEDATE);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction DAY_OF_WEEK = new SqlFunction("DAY_OF_WEEK", SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER, null, OperandTypes.DATE, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction DAY_OF_MONTH = new SqlFunction("DAY_OF_MONTH", SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER, null, OperandTypes.DATE, SqlFunctionCategory.TIMEDATE);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction DAY_OF_YEAR = new SqlFunction("DAY_OF_YEAR", SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER, null, OperandTypes.DATE, SqlFunctionCategory.TIMEDATE);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction DATE_DIFF = new SqlFunction("DATE_DIFF", SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER, null, OperandTypes.family(SqlTypeFamily.DATE, SqlTypeFamily.DATE),
            SqlFunctionCategory.TIMEDATE);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.ANALYTICDB, SqlLibraryEx.POSTGRESQL, SqlLibraryEx.GREENPLUM})
    public static final SqlFunction SPLIT_PART = new SqlFunction("SPLIT_PART", SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_2000, null, OperandTypes.ANY, SqlFunctionCategory.STRING);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.ANALYTICDB, SqlLibraryEx.MYSQL, SqlLibraryEx.HIVE, SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction FROM_UNIXTIME = new SqlFunction("FROM_UNIXTIME", SqlKind.OTHER_FUNCTION,
            ReturnTypes.DATE, null, OperandTypes.ANY, SqlFunctionCategory.TIMEDATE);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction ARRAY_JOIN = new SqlFunction("ARRAY_JOIN", SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_2000, null, OperandTypes.ANY, SqlFunctionCategory.STRING);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction ADS_ARRAY = new SqlFunction("ADS_ARRAY", SqlKind.OTHER_FUNCTION,
            ReturnTypes.TO_ARRAY, null, OperandTypes.ANY, SqlFunctionCategory.SYSTEM);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.ANALYTICDB, SqlLibraryEx.HIVE})
    public static final SqlFunction INSTR = new SqlFunction("INSTR", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_2000
            , null, OperandTypes.ANY, SqlFunctionCategory.STRING);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction ADS_WORK_DAY_DIFF = new SqlFunction("ADS_WORK_DAY_DIFF", SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER, null, OperandTypes.ANY, SqlFunctionCategory.STRING);


    @LibraryOperatorEx(libraries = {SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction ARRAY_MAX = new SqlFunction("ARRAY_MAX", SqlKind.OTHER_FUNCTION, ReturnTypes.ARG0
            , null, OperandTypes.ANY, SqlFunctionCategory.STRING);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction ARRAY_MIN = new SqlFunction("ARRAY_MIN", SqlKind.OTHER_FUNCTION, ReturnTypes.ARG0
            , null, OperandTypes.ANY, SqlFunctionCategory.STRING);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.ANALYTICDB})
    public static final SqlFunction REGEXP_EXTRACT_ALL = new SqlFunction("REGEXP_EXTRACT_ALL", SqlKind.OTHER_FUNCTION
            , ReturnTypes.VARCHAR_2000, null, OperandTypes.ANY, SqlFunctionCategory.STRING);


    //////////////////////////////////////////////////////////////////////////////////////
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction CONCAT_CK = new SqlFunction("CONCAT", SqlKind.OTHER_FUNCTION,
            ReturnTypes.MULTIVALENT_STRING_SUM_PRECISION_NULLABLE, InferTypes.RETURN_TYPE,
            OperandTypes.repeat(SqlOperandCountRanges.from(2), OperandTypes.STRING), SqlFunctionCategory.STRING);


    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toString = new SqlFunction(new SqlIdentifier(Arrays.asList("toString"),
            SqlParserPos.ZERO), ReturnTypes.VARCHAR_2000, InferTypes.RETURN_TYPE, OperandTypes.ANY, null,
            SqlFunctionCategory.STRING);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toFixedString = new SqlFunction(new SqlIdentifier(Arrays.asList("toFixedString"),
            SqlParserPos.ZERO), ReturnTypes.VARCHAR_2000, InferTypes.RETURN_TYPE,
            OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER), null, SqlFunctionCategory.STRING);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction parseDateTimeBestEffort = new SqlFunction(new SqlIdentifier(Arrays.asList(
            "parseDateTimeBestEffort"), SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE,
            OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER), null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction TO_DATE_CK = new SqlFunction(new SqlIdentifier(Arrays.asList("toDate"),
            SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE, OperandTypes.ANY, null,
            SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction TO_DATETIME_CK = new SqlFunction(new SqlIdentifier(Arrays.asList("toDateTime"),
            SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE, OperandTypes.ANY, null,
            SqlFunctionCategory.TIMEDATE);


    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toYear = new SqlFunction(new SqlIdentifier(Arrays.asList("toYear"),
            SqlParserPos.ZERO), ReturnTypes.INTEGER, InferTypes.RETURN_TYPE, OperandTypes.or(OperandTypes.DATE,
            OperandTypes.DATETIME), null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toQuarter = new SqlFunction(new SqlIdentifier(Arrays.asList("toQuarter"),
            SqlParserPos.ZERO), ReturnTypes.INTEGER, InferTypes.RETURN_TYPE, OperandTypes.or(OperandTypes.DATE,
            OperandTypes.DATETIME), null, SqlFunctionCategory.STRING);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toMonth = new SqlFunction(new SqlIdentifier(Arrays.asList("toMonth"),
            SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE, OperandTypes.or(OperandTypes.DATE,
            OperandTypes.DATETIME), null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toDayOfYear = new SqlFunction(new SqlIdentifier(Arrays.asList("toDayOfYear"),
            SqlParserPos.ZERO), ReturnTypes.INTEGER, InferTypes.RETURN_TYPE, OperandTypes.or(OperandTypes.DATE,
            OperandTypes.DATETIME), null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toDayOfMonth = new SqlFunction(new SqlIdentifier(Arrays.asList("toDayOfMonth"),
            SqlParserPos.ZERO), ReturnTypes.INTEGER, InferTypes.RETURN_TYPE, OperandTypes.or(OperandTypes.DATE,
            OperandTypes.DATETIME), null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toDayOfWeek = new SqlFunction(new SqlIdentifier(Arrays.asList("toDayOfWeek"),
            SqlParserPos.ZERO), ReturnTypes.INTEGER, InferTypes.RETURN_TYPE, OperandTypes.or(OperandTypes.DATE,
            OperandTypes.DATETIME), null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toHour = new SqlFunction(new SqlIdentifier(Arrays.asList("toHour"),
            SqlParserPos.ZERO), ReturnTypes.INTEGER, InferTypes.RETURN_TYPE, OperandTypes.or(OperandTypes.DATE,
            OperandTypes.DATETIME), null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toMinute = new SqlFunction(new SqlIdentifier(Arrays.asList("toMinute"),
            SqlParserPos.ZERO), ReturnTypes.INTEGER, InferTypes.RETURN_TYPE, OperandTypes.or(OperandTypes.DATE,
            OperandTypes.DATETIME), null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toSecond = new SqlFunction(new SqlIdentifier(Arrays.asList("toSecond"),
            SqlParserPos.ZERO), ReturnTypes.INTEGER, InferTypes.RETURN_TYPE, OperandTypes.or(OperandTypes.DATE,
            OperandTypes.DATETIME), null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toUnixTimestamp = new SqlFunction(new SqlIdentifier(Arrays.asList(
            "toUnixTimestamp"), SqlParserPos.ZERO), ReturnTypes.BIGINT, InferTypes.RETURN_TYPE,
            OperandTypes.or(OperandTypes.STRING, OperandTypes.DATETIME), null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toStartOfYear = new SqlFunction(new SqlIdentifier(Arrays.asList("toStartOfYear"),
            SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE, OperandTypes.or(OperandTypes.DATE,
            OperandTypes.DATETIME), null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toStartOfISOYear = new SqlFunction(new SqlIdentifier(Arrays.asList(
            "toStartOfISOYear"), SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE,
            OperandTypes.or(OperandTypes.DATE, OperandTypes.DATETIME), null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toStartOfQuarter = new SqlFunction(new SqlIdentifier(Arrays.asList(
            "toStartOfQuarter"), SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE,
            OperandTypes.or(OperandTypes.DATE, OperandTypes.DATETIME), null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toStartOfMonth =
            new SqlFunction(new SqlIdentifier(Arrays.asList("toStartOfMonth"), SqlParserPos.ZERO), ReturnTypes.DATE,
                    InferTypes.RETURN_TYPE, OperandTypes.or(OperandTypes.DATE, OperandTypes.DATETIME), null,
                    SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toStartOfWeek = new SqlFunction(new SqlIdentifier(Arrays.asList("toStartOfWeek"),
            SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE, OperandTypes.or(OperandTypes.DATE,
            OperandTypes.INTEGER), null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toStartOfDay = new SqlFunction(new SqlIdentifier(Arrays.asList("toStartOfDay"),
            SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE, OperandTypes.or(OperandTypes.DATETIME),
            null, SqlFunctionCategory.STRING);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toStartOfHour = new SqlFunction(new SqlIdentifier(Arrays.asList("toStartOfHour"),
            SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE, OperandTypes.or(OperandTypes.DATETIME),
            null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toStartOfMinute = new SqlFunction(new SqlIdentifier(Arrays.asList(
            "toStartOfMinute"), SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE,
            OperandTypes.or(OperandTypes.DATETIME), null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toStartOfSecond = new SqlFunction(new SqlIdentifier(Arrays.asList(
            "toStartOfSecond"), SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE,
            OperandTypes.or(OperandTypes.DATE, OperandTypes.DATETIME), null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toStartOfFiveMinute = new SqlFunction(new SqlIdentifier(Arrays.asList(
            "toStartOfFiveMinute"), SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE,
            OperandTypes.or(OperandTypes.DATE, OperandTypes.DATETIME), null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toStartOfInterval = new SqlFunction(new SqlIdentifier(Arrays.asList(
            "toStartOfInterval"), SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE,
            OperandTypes.or(OperandTypes.DATE, OperandTypes.DATETIME), null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toTime = new SqlFunction(new SqlIdentifier(Arrays.asList("toTime"),
            SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE, OperandTypes.or(OperandTypes.DATETIME),
            null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toWeek = new SqlFunction(new SqlIdentifier(Arrays.asList("toWeek"),
            SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE, OperandTypes.or(OperandTypes.DATE,
            OperandTypes.DATETIME), null, SqlFunctionCategory.STRING);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toYearWeek = new SqlFunction(new SqlIdentifier(Arrays.asList("toYearWeek"),
            SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE, OperandTypes.family(SqlTypeFamily.DATE,
            SqlTypeFamily.INTEGER), null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction date_trunc = new SqlFunction(new SqlIdentifier(Arrays.asList("date_trunc"),
            SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE, OperandTypes.or(OperandTypes.DATE,
            OperandTypes.DATETIME), null, SqlFunctionCategory.STRING);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction addYears = new SqlFunction(new SqlIdentifier(Arrays.asList("addYears"),
            SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE,
            OperandTypes.or(OperandTypes.family(SqlTypeFamily.DATE, SqlTypeFamily.INTEGER),
                    OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.INTEGER)), null,
            SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction addMonths = new SqlFunction(new SqlIdentifier(Arrays.asList("addMonths"),
            SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE,
            OperandTypes.or(OperandTypes.family(SqlTypeFamily.DATE, SqlTypeFamily.INTEGER),
                    OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.INTEGER)), null,
            SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction addWeeks = new SqlFunction(new SqlIdentifier(Arrays.asList("addWeeks"),
            SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE,
            OperandTypes.or(OperandTypes.family(SqlTypeFamily.DATE, SqlTypeFamily.INTEGER),
                    OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.INTEGER)), null,
            SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction addDays = new SqlFunction(new SqlIdentifier(Arrays.asList("addDays"),
            SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE,
            OperandTypes.or(OperandTypes.family(SqlTypeFamily.DATE, SqlTypeFamily.INTEGER),
                    OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.INTEGER)), null,
            SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toDateTime = new SqlFunction(new SqlIdentifier(Arrays.asList("toDateTime"),
            SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE,
            OperandTypes.or(OperandTypes.family(SqlTypeFamily.DATE, SqlTypeFamily.ANY),
                    OperandTypes.family(SqlTypeFamily.DATETIME)), null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toDate = new SqlFunction(new SqlIdentifier(Arrays.asList("toDate"),
            SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE,
            OperandTypes.or(OperandTypes.family(SqlTypeFamily.DATE, SqlTypeFamily.ANY),
                    OperandTypes.family(SqlTypeFamily.DATE)), null, SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction addHours = new SqlFunction(new SqlIdentifier(Arrays.asList("addHours"),
            SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE,
            OperandTypes.or(OperandTypes.family(SqlTypeFamily.DATE, SqlTypeFamily.INTEGER),
                    OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.INTEGER)), null,
            SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction addMinutes = new SqlFunction(new SqlIdentifier(Arrays.asList("addMinutes"),
            SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE,
            OperandTypes.or(OperandTypes.family(SqlTypeFamily.DATE, SqlTypeFamily.INTEGER),
                    OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.INTEGER)), null,
            SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction addSeconds = new SqlFunction(new SqlIdentifier(Arrays.asList("addSeconds"),
            SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE,
            OperandTypes.or(OperandTypes.family(SqlTypeFamily.DATE, SqlTypeFamily.INTEGER),
                    OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.INTEGER)), null,
            SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction addQuarters = new SqlFunction(new SqlIdentifier(Arrays.asList("addQuarters"),
            SqlParserPos.ZERO), ReturnTypes.DATE, InferTypes.RETURN_TYPE,
            OperandTypes.or(OperandTypes.family(SqlTypeFamily.DATE, SqlTypeFamily.INTEGER),
                    OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.INTEGER)), null,
            SqlFunctionCategory.TIMEDATE);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction splitByString = new SqlFunction(new SqlIdentifier(Arrays.asList("splitByString"),
            SqlParserPos.ZERO), ReturnTypes.TO_ARRAY, InferTypes.RETURN_TYPE,
            OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING), null, SqlFunctionCategory.STRING);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction arrayElement = new SqlFunction(new SqlIdentifier(Arrays.asList("arrayElement"),
            SqlParserPos.ZERO), ReturnTypes.VARCHAR_2000, InferTypes.RETURN_TYPE,
            OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.INTEGER), null, SqlFunctionCategory.STRING);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction dateDiff = new SqlFunction(new SqlIdentifier(Arrays.asList("dateDiff"),
            SqlParserPos.ZERO), ReturnTypes.INTEGER, InferTypes.RETURN_TYPE, OperandTypes.family(SqlTypeFamily.ANY,
            SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME), null, SqlFunctionCategory.STRING);

    //系统标准库中有一个extract 抽取时间的  这里有冲突 
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction extract = new SqlFunction(new SqlIdentifier(Arrays.asList("extract"),
            SqlParserPos.ZERO), ReturnTypes.VARCHAR_2000, InferTypes.RETURN_TYPE,
            OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING), null, SqlFunctionCategory.STRING);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction extractAll = new SqlFunction(new SqlIdentifier(Arrays.asList("extractAll"),
            SqlParserPos.ZERO), ReturnTypes.TO_ARRAY, InferTypes.RETURN_TYPE,
            OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.INTEGER), null,
            SqlFunctionCategory.STRING);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction IF_CK = new SqlFunction("IF", SqlKind.OTHER_FUNCTION, ReturnTypes.ARG1_NULLABLE,
            null, OperandTypes.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.ANY, SqlTypeFamily.ANY),
            SqlFunctionCategory.STRING);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction CASE_CK = new SqlFunction("CASE", SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG1_NULLABLE, null, OperandTypes.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.ANY,
            SqlTypeFamily.ANY), SqlFunctionCategory.STRING);


    //大小写铭感
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction array = new SqlFunction(new SqlIdentifier(Arrays.asList("array"),
            SqlParserPos.ZERO), ReturnTypes.ARG1_NULLABLE, InferTypes.RETURN_TYPE, OperandTypes.ANY, null,
            SqlFunctionCategory.SYSTEM);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction arrayReduce = new SqlFunction(new SqlIdentifier(Arrays.asList("arrayReduce"),
            SqlParserPos.ZERO), ReturnTypes.ARG1_NULLABLE, InferTypes.RETURN_TYPE,
            OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.ARRAY), null, SqlFunctionCategory.SYSTEM);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction base64Encode = new SqlFunction(new SqlIdentifier(Arrays.asList("base64Encode"),
            SqlParserPos.ZERO), ReturnTypes.VARCHAR_2000, InferTypes.RETURN_TYPE, OperandTypes.ANY, null,
            SqlFunctionCategory.SYSTEM);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction base64Decode = new SqlFunction(new SqlIdentifier(Arrays.asList("base64Decode"),
            SqlParserPos.ZERO), ReturnTypes.VARCHAR_2000, InferTypes.RETURN_TYPE, OperandTypes.ANY, null,
            SqlFunctionCategory.SYSTEM);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction position = new SqlFunction(new SqlIdentifier(Arrays.asList("position"),
            SqlParserPos.ZERO), ReturnTypes.ARG1_NULLABLE, InferTypes.RETURN_TYPE, OperandTypes.ANY_ANY, null,
            SqlFunctionCategory.SYSTEM);


    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction SUBSTRING = new SqlFunction("SUBSTRING", SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG1_NULLABLE, null, OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY,
            SqlTypeFamily.ANY), SqlFunctionCategory.STRING);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction SUBSTR = new SqlFunction("SUBSTR", SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG1_NULLABLE, null, OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY,
            SqlTypeFamily.ANY), SqlFunctionCategory.STRING);


    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction replaceAll = new SqlFunction(new SqlIdentifier(Arrays.asList("replaceAll"),
            SqlParserPos.ZERO), ReturnTypes.VARCHAR_2000, InferTypes.RETURN_TYPE,
            OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING), null,
            SqlFunctionCategory.SYSTEM);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction replaceRegexpAll = new SqlFunction(new SqlIdentifier(Arrays.asList(
            "replaceRegexpAll"), SqlParserPos.ZERO), ReturnTypes.VARCHAR_2000, InferTypes.RETURN_TYPE,
            OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING), null,
            SqlFunctionCategory.SYSTEM);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toInt32 = new SqlFunction(new SqlIdentifier(Arrays.asList("toInt32"),
            SqlParserPos.ZERO), ReturnTypes.VARCHAR_2000, InferTypes.RETURN_TYPE, OperandTypes.ANY, null,
            SqlFunctionCategory.SYSTEM);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toInt64 = new SqlFunction(new SqlIdentifier(Arrays.asList("toInt64"),
            SqlParserPos.ZERO), ReturnTypes.VARCHAR_2000, InferTypes.RETURN_TYPE, OperandTypes.ANY, null,
            SqlFunctionCategory.SYSTEM);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toInt16 = new SqlFunction(new SqlIdentifier(Arrays.asList("toInt16"),
            SqlParserPos.ZERO), ReturnTypes.VARCHAR_2000, InferTypes.RETURN_TYPE, OperandTypes.ANY, null,
            SqlFunctionCategory.SYSTEM);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toInt8 = new SqlFunction(new SqlIdentifier(Arrays.asList("toInt8"),
            SqlParserPos.ZERO), ReturnTypes.VARCHAR_2000, InferTypes.RETURN_TYPE, OperandTypes.ANY, null,
            SqlFunctionCategory.SYSTEM);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toDecimal32 = new SqlFunction(new SqlIdentifier(Arrays.asList("toDecimal32"),
            SqlParserPos.ZERO), ReturnTypes.VARCHAR_2000, InferTypes.RETURN_TYPE, OperandTypes.ANY, null,
            SqlFunctionCategory.SYSTEM);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toFloat64 = new SqlFunction(new SqlIdentifier(Arrays.asList("toFloat64"),
            SqlParserPos.ZERO), ReturnTypes.VARCHAR_2000, InferTypes.RETURN_TYPE, OperandTypes.ANY, null,
            SqlFunctionCategory.SYSTEM);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction toDecimal64 = new SqlFunction(new SqlIdentifier(Arrays.asList("toDecimal64"),
            SqlParserPos.ZERO), ReturnTypes.VARCHAR_2000, InferTypes.RETURN_TYPE, OperandTypes.ANY, null,
            SqlFunctionCategory.SYSTEM);


    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE, SqlLibraryEx.MYSQL, SqlLibraryEx.POSTGRESQL, SqlLibraryEx.GREENPLUM, SqlLibraryEx.HIVE})
    public static final SqlFunction length = new SqlFunction("length", SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG1_NULLABLE, null, OperandTypes.STRING, SqlFunctionCategory.STRING);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction rand = new SqlFunction("RAND", SqlKind.OTHER_FUNCTION, ReturnTypes.ARG1_NULLABLE,
            null, OperandTypes.ANY, SqlFunctionCategory.STRING);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction randConstant = new SqlFunction(new SqlIdentifier(Arrays.asList("randConstant"),
            SqlParserPos.ZERO), ReturnTypes.VARCHAR_2000, InferTypes.RETURN_TYPE, OperandTypes.ANY, null,
            SqlFunctionCategory.SYSTEM);
    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction bitAnd = new SqlFunction(new SqlIdentifier(Arrays.asList("bitAnd"),
            SqlParserPos.ZERO), ReturnTypes.ARG0, InferTypes.RETURN_TYPE, OperandTypes.family(SqlTypeFamily.ANY,
            SqlTypeFamily.ANY), null, SqlFunctionCategory.SYSTEM);

    @LibraryOperatorEx(libraries = {SqlLibraryEx.CLICKHOUSE})
    public static final SqlFunction JSONExtractRaw =
            new SqlFunction(new SqlIdentifier(Arrays.asList("JSONExtractRaw"), SqlParserPos.ZERO), ReturnTypes.ARG0,
                    InferTypes.RETURN_TYPE, OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY), null,
                    SqlFunctionCategory.SYSTEM);

}
