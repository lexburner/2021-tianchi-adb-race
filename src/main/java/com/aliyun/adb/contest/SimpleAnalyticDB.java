package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleAnalyticDB implements AnalyticDB {

    private final Map<String, List<Long>> data = new HashMap<String, List<Long>>();

    /**
     *
     * The implementation must contain a public no-argument constructor.
     *
     */
    public SimpleAnalyticDB() {
    }

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        File dir = new File(tpchDataFileDir);

        for (File dataFile : dir.listFiles()) {
            System.out.println("Start loading table " + dataFile.getName());

            // You can write data to workspaceDir
            File yourDataFile = new File(workspaceDir, dataFile.getName());
            yourDataFile.createNewFile();

            loadInMemroy(dataFile);
        }

    }

    @Override
    public String quantile(String table, String column, double percentile) throws Exception {

        List<Long> values = data.get(tableColumnKey(table, column));

        if (values == null) {
            throw new IllegalArgumentException();
        }

        int rank = (int) Math.round(values.size() * percentile);
        String ans = values.get(rank-1).toString();

        System.out.println("Query:" + table + ", " + column + ", " + percentile + " Answer:" + rank + ", " + ans);

        return ans;
    }

    private void loadInMemroy(File dataFile) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(dataFile));
        String table = dataFile.getName();
        String[] columns = reader.readLine().split(",");

        for (String column : columns) {
            data.put(tableColumnKey(table, column), new ArrayList<Long>());
        }

        String rawRow;
        while ((rawRow = reader.readLine()) != null) {
            String[] row = rawRow.split(",");

            for (int i = 0; i < columns.length; i++) {
                data.get(tableColumnKey(table, columns[i])).add(Long.parseLong(row[i]));
            }
        }

        data.forEach((tableColumn, values) -> {
            values.sort(Long::compareTo);
            System.out.println("Finish loading column " + tableColumn);
        });

    }

    private String tableColumnKey(String table, String column) {
        return (table + "." + column).toLowerCase();
    }

}
