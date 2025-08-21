package io.dazzleduck.sql.commons.util;

public class TestConstants {
    public static String SUPPORTED_HIVE_PATH_QUERY = "FROM (FROM (VALUES(NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR)) t( dt, p, key, value) \n" +
            "WHERE false\n" +
            "UNION ALL BY NAME\n" +
            "FROM read_parquet('example/hive_table/*/*/*.parquet', hive_partitioning = true, hive_types = {'dt': DATE, 'p': VARCHAR}))";

    public static String SUPPORTED_DELTA_PATH_QUERY = "FROM (FROM (VALUES(NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR)) t( dt, p, key, value) \n" +
            "WHERE false\n" +
            "UNION ALL BY NAME\n" +
            "FROM read_delta('example/delta_table'))";
}
