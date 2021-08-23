package com.aliyun.adb.contest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import com.aliyun.adb.contest.spi.AnalyticDB;

/**
 * @author jingfeng.xjf
 * @date 2021-06-24
 */
public class AnalysisDB implements AnalyticDB {

    private final Map<String, List<Integer>> data = new HashMap<>();

    private Map<Byte, Integer> byte2IntMap;

    /**
     * The implementation must contain a public no-argument constructor.
     */
    public AnalysisDB() {
        byte2IntMap = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            byte2IntMap.put((byte)(i + '0'), i);
        }
    }

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {

        File dir = new File(tpchDataFileDir);
        for (File dataFile : dir.listFiles()) {
            if ("lineitem".equals(dataFile.getName())) {
                //testRead(dataFile, 12, true);
                //testRead(dataFile, 12, true);
                //testRead(dataFile, 12, false);
                //testRead(dataFile, 12, false);
            }
        }

        testWrite(workspaceDir, 12, 128, 1);
        testWrite(workspaceDir, 12, 64, 2);
        //testWriteWithCpuOp(workspaceDir, 12, 128, 2);
        //testWritex3(workspaceDir, 12, 128, 3);

    }

    @Override
    public String quantile(String table, String column, double percentile) throws Exception {
        return "12345";
    }

    private void loadInMemroy(File dataFile) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(dataFile));
        String table = dataFile.getName();
        String[] columns = reader.readLine().split(",");

        for (String column : columns) {
            ArrayList<Integer> countArray = new ArrayList<>();
            for (int i = 0; i < 101; i++) {
                countArray.add(0);
            }
            data.put(tableColumnKey(table, column), countArray);
        }

        String rawRow;
        while ((rawRow = reader.readLine()) != null) {
            String[] row = rawRow.split(",");

            for (int i = 0; i < columns.length; i++) {
                List<Integer> countArray = data.get(tableColumnKey(table, columns[i]));
                int partition = head2BitPartition(row[i]);
                countArray.set(partition, countArray.get(partition) + 1);
            }
        }
        for (Entry<String, List<Integer>> entries : data.entrySet()) {
            String key = entries.getKey();
            List<Integer> value = entries.getValue();
            System.out.println("tableColumn : " + key);
            for (int i = 0; i < 101; i++) {
                System.out.println("partition " + i + " nums: " + value.get(i));
            }
        }

    }

    private String tableColumnKey(String table, String column) {
        return (table + "." + column).toLowerCase();
    }

    private int head2BitPartition(String val) {
        if (val.length() <= 17) {
            return 0;
        } else if (val.length() == 18) {
            return val.charAt(0) - '0';
        } else if (val.length() == 19) {
            return (val.charAt(0) - '0') * 10 + (val.charAt(1) - '0');
        }
        return 100;
    }

    private void testWrite(String workDir, final int threadNum, final int fileNum, final int testRound)
        throws Exception {
        long totalWriteNum = 6_0000_0000;
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        long start = System.currentTimeMillis();
        for (int i = 0; i < threadNum; i++) {
            final long threadWriteNum = totalWriteNum / threadNum;
            final int threadNo = i;
            new Thread(() -> {
                try {
                    File file = new File(
                        workDir + File.separator + testRound + "_" + threadNum + "_" + threadNo + "_" + testRound);
                    RandomAccessFile rw = new RandomAccessFile(file, "rw");
                    FileChannel fileChannel = rw.getChannel();
                    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(128 * 1024);
                    long position = 0;
                    for (long val = 0; val < threadWriteNum; val++) {
                        byteBuffer.putLong(val);
                        if (byteBuffer.remaining() < 8) {
                            byteBuffer.flip();
                            fileChannel.write(byteBuffer, position);
                            position += 128 * 1024;
                            byteBuffer.clear();
                        }
                    }
                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
        countDownLatch.await();
        System.out.println(
            "threadNum " + threadNum + " fileNum " + fileNum + " write 6_0000_0000 cost " + (
                System.currentTimeMillis()
                    - start) + " ms");
    }

    private void testWriteWithCpuOp(String workDir, final int threadNum, final int fileNum, final int testRound)
        throws Exception {
        long totalWriteNum = 6_0000_0000;
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        long start = System.currentTimeMillis();
        for (int i = 0; i < threadNum; i++) {
            String valStr = "600000000000000000";
            final long threadWriteNum = totalWriteNum / threadNum;
            final int threadNo = i;
            new Thread(() -> {
                try {
                    FileChannel[] fileChannels = new FileChannel[fileNum];
                    for (int j = 0; j < fileNum; j++) {
                        File file = new File(workDir + File.separator + testRound + "_" + threadNo + "_" + j);
                        RandomAccessFile rw = new RandomAccessFile(file, "rw");
                        FileChannel fileChannel = rw.getChannel();
                        fileChannels[j] = fileChannel;
                    }
                    ByteBuffer byteBuffer = ByteBuffer.allocate(128 * 1024);
                    int round = 0;
                    for (long val = 0; val < threadWriteNum; val++) {
                        long temp = 0;
                        for (int j = 0; j < valStr.length(); j++) {
                            temp += temp * 10 + valStr.charAt(j) - '0';
                        }
                        //int offset = 64 - 8 + 1;
                        //byte t = (byte)((temp >> offset) & 0xff);
                        byteBuffer.putLong(val);
                        if (byteBuffer.remaining() < 8) {
                            byteBuffer.flip();
                            fileChannels[round % fileNum].write(byteBuffer, fileChannels[round % fileNum].position());
                            round++;
                            byteBuffer.clear();
                        }
                    }
                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
        countDownLatch.await();
        System.out.println(
            "threadNum " + threadNum + " fileNum " + fileNum + " write 6_0000_0000 cost " + (
                System.currentTimeMillis()
                    - start) + " ms");
    }

    private void testWritex3(String workDir, final int threadNum, final int fileNum, final int testRound)
        throws Exception {
        long totalWriteNum = 6_0000_0000;
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        long start = System.currentTimeMillis();
        for (int i = 0; i < threadNum; i++) {
            final long threadWriteNum = totalWriteNum / threadNum;
            final int threadNo = i;
            new Thread(() -> {
                try {
                    FileChannel[] fileChannels = new FileChannel[fileNum];
                    for (int j = 0; j < fileNum; j++) {
                        File file = new File(workDir + File.separator + testRound + "_" + threadNo + "_" + j);
                        RandomAccessFile rw = new RandomAccessFile(file, "rw");
                        FileChannel fileChannel = rw.getChannel();
                        fileChannels[j] = fileChannel;
                    }
                    ByteBuffer byteBuffer = ByteBuffer.allocate(128 * 1024);
                    int round = 0;
                    for (long val = 0; val < threadWriteNum; val++) {
                        byteBuffer.putLong(val);
                        byteBuffer.putLong(val);
                        byteBuffer.putLong(val);
                        if (byteBuffer.remaining() < 8 * 3) {
                            byteBuffer.flip();
                            fileChannels[round % fileNum].write(byteBuffer, fileChannels[round % fileNum].position());
                            round++;
                            byteBuffer.clear();
                        }
                    }
                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
        countDownLatch.await();
        System.out.println(
            "threadNum " + threadNum + " fileNum " + fileNum + " write 6_0000_0000 cost " + (
                System.currentTimeMillis()
                    - start) + " ms");
    }

    private void testRead(File dataFile, int threadNum, boolean flag) throws Exception {
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
                long threadStart = System.currentTimeMillis();
                try {
                    long readBufferSize = 1024 * 64;
                    ByteBuffer byteBuffer = ByteBuffer.allocate((int)readBufferSize);
                    byte[] readBufferArray = new byte[(int)readBufferSize];
                    long readPosition = readThreadPosition[threadNo];
                    long partitionTotalSize;
                    if (threadNo == threadNum - 1) {
                        partitionTotalSize = totalSize;
                    } else {
                        partitionTotalSize = readThreadPosition[threadNo + 1];
                    }
                    long blockReadPosition = 0;
                    while (readPosition < partitionTotalSize - 1) {
                        long size = Math.min(readBufferSize, partitionTotalSize - readPosition);
                        if (size < readBufferSize) {
                            readBufferArray = new byte[(int)size];
                        }
                        fileChannel.read(byteBuffer, readPosition);
                        byteBuffer.flip();
                        byteBuffer.get(readBufferArray);
                        byteBuffer.clear();
                        long val = 0;
                        for (int i = 0; i < size; i++) {
                            if (readBufferArray[i] != '\n' && readBufferArray[i] != ',') {
                                if (flag) {
                                    val = val * 10 + convertByte(readBufferArray[i]);
                                } else {
                                    val = val * 10 + readBufferArray[i] - '0';
                                }

                            } else if (readBufferArray[i] == ',') {
                                //orderRaceEngine.add(threadNo, val);
                                val = 0;
                                blockReadPosition = i + 1;
                            } else if (readBufferArray[i] == '\n') {
                                //partRaceEngine.add(threadNo, val);
                                val = 0;
                                blockReadPosition = i + 1;
                            }
                        }
                        readPosition += blockReadPosition;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                countDownLatch.countDown();
                System.out.println("thread cost " + (System.currentTimeMillis() - threadStart) + " ms");
            }).start();
        }
        countDownLatch.await();
        System.out.println(
            "threadNum " + threadNum + " read + analysis cost " + (System.currentTimeMillis() - start) + " ms");
    }

    private void testMmapWrite(String workDir, final int threadNum) throws Exception {
        long totalWriteNum = 6_0000_0000;
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        long start = System.currentTimeMillis();
        for (int i = 0; i < threadNum; i++) {
            final long threadWriteNum = totalWriteNum / threadNum;
            final int threadNo = i;
            new Thread(() -> {
                try {
                    File file = new File(workDir + File.separator + threadNum + "_" + threadNo);
                    RandomAccessFile rw = new RandomAccessFile(file, "rw");
                    FileChannel fileChannel = rw.getChannel();
                    MappedByteBuffer mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, 800 * 1024 * 1024);
                    for (long val = 0; val < threadWriteNum; val++) {
                        mappedByteBuffer.putLong(val);
                    }
                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
        countDownLatch.await();
        System.out.println(
            "threadNum " + threadNum + " write 6_0000_0000 cost " + (System.currentTimeMillis() - start) + " ms");
    }

    public int convertByte(byte b) {
        return byte2IntMap.get(b);
    }

}
