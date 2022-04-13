package com.haizhi.sql.parser.common;
// CHECKSTYLE:OFF

import static java.util.Objects.requireNonNull;

import java.lang.reflect.Field;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.util.Util;


/**
 * 原始的SqlLibraryOperatorTableFactory处理的SqlLibraryOperators 实际上 补充的function并不全面
 * 这里单独补充下 系统有的
 */
public class SqlLibraryOperatorTableFactoryEx {

    /**
     * List of classes to scan for operators.
     */
    private final ImmutableList<Class<?>> classes;

    /**
     * The singleton instance.
     */
    public static final SqlLibraryOperatorTableFactoryEx INSTANCE =
            new SqlLibraryOperatorTableFactoryEx(SqlLibraryOperatorsEx.class);

    private SqlLibraryOperatorTableFactoryEx(Class<?>... classes) {
        this.classes = ImmutableList.copyOf(classes);
    }

    // ~ Instance fields --------------------------------------------------------

    /**
     * A cache that returns an operator table for a given library (or set of
     * libraries).
     */
    private final LoadingCache<ImmutableSet<SqlLibraryEx>, SqlOperatorTable> cache =
            CacheBuilder.newBuilder().build(CacheLoader.from(this::create));

    // ~ Methods ----------------------------------------------------------------

    /**
     * Creates an operator table that contains operators in the given set of
     * libraries.
     */
    private SqlOperatorTable create(ImmutableSet<SqlLibraryEx> librarySet) {
        final ImmutableList.Builder<SqlOperator> list = ImmutableList.builder();
        boolean custom = false;
        boolean standard = false;
        for (SqlLibraryEx library : librarySet) {
            switch (library) {
                case STANDARD:
                    standard = true;
                    break;
                case SPATIAL:
                    list.addAll(SqlOperatorTables.spatialInstance().getOperatorList());
                    break;
                default:
                    custom = true;
            }
        }

        // Use reflection to register the expressions stored in public fields.
        // Skip if the only libraries asked for are "standard" or "spatial".
        if (custom) {
            for (Class<?> aClass : classes) {
                for (Field field : aClass.getFields()) {
                    try {
                        if (SqlOperator.class.isAssignableFrom(field.getType())) {
                            final SqlOperator op = (SqlOperator) requireNonNull(field.get(this), () -> "null value "
                                    + "of" + " " + field + " for " + this);
                            if (operatorIsInLibrary(op.getName(), field, librarySet)) {
                                list.add(op);
                            }
                        }
                    } catch (IllegalArgumentException | IllegalAccessException e) {
                        throw Util.throwAsRuntime(Util.causeOrSelf(e));
                    }
                }
            }
        }
        SqlOperatorTable operatorTable = new ListSqlOperatorTable(list.build());
        if (standard) {
            operatorTable = SqlOperatorTables.chain(SqlStdOperatorTable.instance(), operatorTable);
        }
        return operatorTable;
    }

    /**
     * Returns whether an operator is in one or more of the given libraries.
     */
    private static boolean operatorIsInLibrary(String operatorName, Field field, Set<SqlLibraryEx> seekLibrarySet) {
        LibraryOperatorEx libraryOperator = field.getAnnotation(LibraryOperatorEx.class);
        if (libraryOperator == null) {
            throw new AssertionError("Operator must have annotation: " + operatorName);
        }
        SqlLibraryEx[] librarySet = libraryOperator.libraries();
        if (librarySet.length <= 0) {
            throw new AssertionError("Operator must belong to at least one library: " + operatorName);
        }
        for (SqlLibraryEx library : librarySet) {
            if (seekLibrarySet.contains(library)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns a SQL operator table that contains operators in the given library or
     * libraries.
     */
    public SqlOperatorTable getOperatorTable(SqlLibraryEx... libraries) {
        return getOperatorTable(ImmutableSet.copyOf(libraries));
    }

    /**
     * Returns a SQL operator table that contains operators in the given set of
     * libraries.
     */
    public SqlOperatorTable getOperatorTable(Iterable<SqlLibraryEx> librarySet) {
        try {
            return cache.get(ImmutableSet.copyOf(librarySet));
        } catch (ExecutionException e) {
            throw Util.throwAsRuntime("populating SqlOperatorTable for library " + librarySet, Util.causeOrSelf(e));
        }
    }
}
