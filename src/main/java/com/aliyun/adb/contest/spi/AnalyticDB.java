package com.aliyun.adb.contest.spi;

/**
 *
 * DO NOT modify this file.
 *
 */
public interface AnalyticDB {

    /**
     *
     * Load test data.
     *
     * @param tpchDataFileDir A read-only directory
     * @param workspaceDir A directory for players to use
     */
    void load(String tpchDataFileDir, String workspaceDir) throws Exception;


    /**
     *
     * Quantile definition:
     *
     * Consider a table T which contains a column C in it. C contains five values {1, 2, 3, 4, 5}.
     * quantile(T, C, 0.2) = values[5*0.2-1] = values[0] = 1
     *
     * We make sure that the result (percentile*values.length) is an integer.
     *
     * @param table TPC-H table
     * @param column A column that belong to this table.
     * @param percentile [0, 1.0], such as 0.25, 0.125
     * @return A string representing the answer
     */
    String quantile(String table, String column, double percentile) throws Exception;

}
