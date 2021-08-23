package com.aliyun.adb.contest;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.aliyun.adb.contest.spi.AnalyticDB;

public class RaceAnalyticDB implements AnalyticDB {

    private final Map<String, DiskRaceEngine> columnName2EngineMap = new HashMap<>();

    public static final int THREAD_NUM = 12;

    public static final int PARTITION_OVER_PARTITION = 8;

    public static final int PARTITION = 64;

    public static final int OFFSET = 7;

    public static final int WRITE_BUFFER_SIZE = 1024 * 128;

    public static ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);



    /**
     * The implementation must contain a public no-argument constructor.
     */
    public RaceAnalyticDB() {
    }

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        File dir = new File(tpchDataFileDir);
        for (File dataFile : dir.listFiles()) {
            if ("lineitem".equals(dataFile.getName())) {
                final int threadNum = THREAD_NUM;

                // 初始化引擎
                long createEngineStart = System.currentTimeMillis();
                String orderKey = tableColumnKey("lineitem", "L_ORDERKEY");
                DiskRaceEngine orderRaceEngine = new DiskRaceEngine(workspaceDir, orderKey, threadNum);
                columnName2EngineMap.put(orderKey, orderRaceEngine);
                String partKey = tableColumnKey("lineitem", "L_PARTKEY");
                DiskRaceEngine partRaceEngine = new DiskRaceEngine(workspaceDir, partKey, threadNum);
                columnName2EngineMap.put(partKey, partRaceEngine);
                System.out.println(
                    "create engine cost " + (System.currentTimeMillis() - createEngineStart) + " ms");

                long start = System.currentTimeMillis();
                RandomAccessFile randomAccessFile = new RandomAccessFile(dataFile, "r");
                FileChannel fileChannel = randomAccessFile.getChannel();
                long totalSize = fileChannel.size();
                long[] readThreadPosition = new long[threadNum];
                // 设置对齐的 n 个片
                readThreadPosition[0] = 21;
                for (int i = 1; i < threadNum; i++) {
                    long paddingPosition = totalSize / threadNum * i;
                    MappedByteBuffer mappedByteBuffer = fileChannel.map(MapMode.READ_ONLY, paddingPosition, 100);
                    for (int j = 0; j < 100; j++) {
                        byte aByte = mappedByteBuffer.get();
                        if (aByte == (byte)'\n') {
                            paddingPosition += j + 1;
                            break;
                        }
                    }
                    readThreadPosition[i] = paddingPosition;
                }

                CountDownLatch countDownLatch = new CountDownLatch(threadNum);
                for (int k = 0; k < threadNum; k++) {
                    final int threadNo = k;
                    new Thread(() -> {
                        //long threadStart = System.currentTimeMillis();
                        orderRaceEngine.init(threadNo);
                        partRaceEngine.init(threadNo);
                        try {
                            int readBufferSize = 1024 * 1024 * 2;
                            ByteBuffer byteBuffer = ByteBuffer.allocate(readBufferSize);
                            byte[] readBufferArray;
                            long readPosition = readThreadPosition[threadNo];
                            long partitionTotalSize;
                            if (threadNo == threadNum - 1) {
                                partitionTotalSize = totalSize;
                            } else {
                                partitionTotalSize = readThreadPosition[threadNo + 1];
                            }
                            while (readPosition < partitionTotalSize - 1) {
                                int size = (int)Math.min(readBufferSize, partitionTotalSize - readPosition);
                                byteBuffer.clear();
                                fileChannel.read(byteBuffer, readPosition);
                                readBufferArray = byteBuffer.array();
                                long val = 0;
                                while (size > 0) {
                                    if (readBufferArray[size - 1] == '\n') {
                                        break;
                                    }
                                    size--;
                                }
                                for (int i = 0; i < size; i++) {
                                    //if (readBufferArray[i] == '\n') {
                                    //    partRaceEngine.add(threadNo, val);
                                    //    val = 0;
                                    //    blockReadPosition = i + 1;
                                    //} else if(readBufferArray[i] == ',') {
                                    //    orderRaceEngine.add(threadNo, val);
                                    //    val = 0;
                                    //    blockReadPosition = i + 1;
                                    //} else {
                                    //    val = val * 10 + (readBufferArray[i] - '0');
                                    //}
                                    byte temp = readBufferArray[i];
                                    do {
                                        val = val * 10 + (temp - '0');
                                        temp = readBufferArray[++i];
                                    } while (temp != ',');
                                    orderRaceEngine.add(threadNo, val);
                                    val = 0;
                                    // skip ，
                                    i++;
                                    temp = readBufferArray[i];
                                    do {
                                        val = val * 10 + (temp - '0');
                                        temp = readBufferArray[++i];
                                    } while (temp != '\n');
                                    partRaceEngine.add(threadNo, val);
                                    val = 0;
                                    // skip \n
                                }
                                readPosition += size;
                            }

                            orderRaceEngine.flush(threadNo);
                            partRaceEngine.flush(threadNo);

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        countDownLatch.countDown();
                        //System.out.println("thread cost " + (System.currentTimeMillis() - threadStart) + " ms");
                    }).start();
                }

                countDownLatch.await();
                //System.out.println("read + analysis cost " + (System.currentTimeMillis() - start) + " ms");

                //for (int j = 0; j < 128; j++) {
                //    long dataNum = 0;
                //    for (int i = 0; i < threadNum; i++) {
                //        dataNum += orderRaceEngine.bucketFiles[i][j].getDataNum();
                //    }
                //    System.out.println("partition " + j + " has " + dataNum + " nums");
                //}
                //
                //for (int i = 0; i < threadNum; i++) {
                //    for (int j = 0; j < 128; j++) {
                //        System.out.println(" partRaceEngine | thread " + i + " partition " + j + " nums " +
                //            partRaceEngine.bucketFiles[i][j].getDataNum());
                //    }
                //}

            }
        }
    }

    //AtomicInteger atomicInteger = new AtomicInteger();

    @Override
    public String quantile(String table, String column, double percentile) throws Exception {
        //long start = System.currentTimeMillis();
        DiskRaceEngine diskRaceEngine = columnName2EngineMap.get(tableColumnKey(table, column));
        long ans = diskRaceEngine.quantile(percentile);
        //long cost = System.currentTimeMillis() - start;
        //System.out.println(
        //    "Query:" + table + ", " + column + ", " + percentile + " Answer:" + ans + ", Cost " + cost + " ms");
        //if (atomicInteger.incrementAndGet() == 9) {
        //    return "12345";
        //}
        return ans + "";
    }

    private String tableColumnKey(String table, String column) {
        return (table + "." + column).toLowerCase();
    }

}
