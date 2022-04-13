package com.haizhi.sql.parser.common;
// CHECKSTYLE:OFF
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.checkerframework.checker.nullness.qual.Nullable;

public enum SqlLibraryEx {
    /**
     * The standard operators.
     */
    STANDARD("", "standard"),
    /**
     * Geospatial operators.
     */
    SPATIAL("s", "spatial"),
    /**
     * A collection of operators that are in Google BigQuery but not in standard
     * SQL.
     */
    BIG_QUERY("b", "bigquery"),
    /**
     * A collection of operators that are in Apache Hive but not in standard
     * SQL.
     */
    HIVE("h", "hive"),
    /**
     * A collection of operators that are in MySQL but not in standard SQL.
     */
    MYSQL("m", "mysql"),
    /**
     * A collection of operators that are in Oracle but not in standard SQL.
     */
    ORACLE("o", "oracle"),
    /**
     * A collection of operators that are in PostgreSQL but not in standard
     * SQL.
     */
    POSTGRESQL("p", "postgresql"), GREENPLUM("g", "greenplum"),
    /**
     * A collection of operators that are in Apache Spark but not in standard
     * SQL.
     */
    SPARK("s", "spark"), ANALYTICDB("a", "analyticdb"), HUBBLE("u", "hubble"), MSSQL("x", "mssql"), CLICKHOUSE("c",
            "clickhouse"), HANA("n", "hana");

    /**
     * Abbreviation for the library used in SQL reference.
     */
    public final String abbrev;

    /**
     * Name of this library when it appears in the connect string;
     * see {@link CalciteConnectionProperty#FUN}.
     */
    public final String fun;

    SqlLibraryEx(String abbrev, String fun) {
        this.abbrev = Objects.requireNonNull(abbrev, "abbrev");
        this.fun = Objects.requireNonNull(fun, "fun");
        Preconditions.checkArgument(fun.equals(name().toLowerCase(Locale.ROOT).replace("_", "")));
    }

    /**
     * Looks up a value.
     * Returns null if not found.
     * You can use upper- or lower-case name.
     */
    public static @Nullable SqlLibraryEx of(String name) {
        return MAP.get(name);
    }

    /**
     * Parses a comma-separated string such as "standard,oracle".
     */
    public static List<SqlLibraryEx> parse(String libraryNameList) {
        final ImmutableList.Builder<SqlLibraryEx> list = ImmutableList.builder();
        for (String libraryName : libraryNameList.split(",")) {
            SqlLibraryEx library = Objects.requireNonNull(SqlLibraryEx.of(libraryName), () -> "library does not "
                    + "exist: " + libraryName);
            list.add(library);
        }
        return list.build();
    }

    public static final Map<String, SqlLibraryEx> MAP;

    static {
        final ImmutableMap.Builder<String, SqlLibraryEx> builder = ImmutableMap.builder();
        for (SqlLibraryEx value : values()) {
            builder.put(value.name(), value);
            builder.put(value.fun, value);
        }
        MAP = builder.build();
    }
}
