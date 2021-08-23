package com.aliyun.adb.contest;

import java.io.File;
import java.util.concurrent.Future;

import static com.aliyun.adb.contest.RaceAnalyticDB.OFFSET;
import static com.aliyun.adb.contest.RaceAnalyticDB.PARTITION;
import static com.aliyun.adb.contest.RaceAnalyticDB.THREAD_NUM;

/**
 * @author jingfeng.xjf
 * @date 2021-06-21
 */
public class DiskRaceEngine {

    private String tableColumnName;
    public BucketFile[][] bucketFiles;
    private int threadNum;
    private String workDir;
    private String filePrefix;

    public DiskRaceEngine(String workDir, String filePrefix, int threadNum) {
        this.workDir = workDir;
        this.filePrefix = filePrefix;
        this.threadNum = threadNum;
        bucketFiles = new BucketFile[threadNum][PARTITION];
    }

    public void init(int threadNo) {
        for (int j = 0; j < PARTITION; j++) {
            bucketFiles[threadNo][j] = new BucketFile(workDir + File.separator + filePrefix + "_" + threadNo + "_" + j);
        }
    }

    public void add(int threadNo, long longVal) throws Exception {
        // 根据 key 选择到对应分区的文件
        BucketFile bucket = chooseBucketByKey(threadNo, longVal);
        bucket.add(longVal);
    }

    private BucketFile chooseBucketByKey(int threadNo, long longVal) {
        int partition = getPartition(longVal);
        return bucketFiles[threadNo][partition];
    }

    public static int getPartition(long values) {
        int offset = 64 - OFFSET;
        return (byte)(values >> offset);
    }

    public long quantile(double percentile) throws Exception {
        long ioStart = System.currentTimeMillis();
        //int totalNumer = getTotalNumer();
        int totalNumer = 3_0000_0000;
        int rank = (int)Math.round(totalNumer * percentile);
        // rank 代表第 n 个数，从 1 开始；index 代表数组的下标，从 0 开始
        int currentBucketBeginRank = 1;
        int currentBucketEndRank;
        int blockIndex = 0;
        int hitPartition = -1;
        for (int i = 0; i < PARTITION; i++) {
            int dataNum = 0;
            for (int j = 0; j < threadNum; j++) {
                dataNum += bucketFiles[j][i].getDataNum();
            }
            currentBucketEndRank = currentBucketBeginRank + dataNum;
            if (currentBucketBeginRank <= rank && rank < currentBucketEndRank) {
                blockIndex = rank - currentBucketBeginRank;
                hitPartition = i;
                break;
            }
            currentBucketBeginRank = currentBucketEndRank;
        }

        long[][][] nums = Util.sharedBuffers;
        int[][] sharedIndex = Util.sharedIndex;
        for (int i = 0; i < THREAD_NUM; i++) {
            for (int j = 0; j < 8; j++) {
                sharedIndex[i][j] = 0;
            }
        }
        Future[] futures = new Future[threadNum];
        for (int i = 0; i < threadNum; i++) {
            bucketFiles[i][hitPartition].flush();
            futures[i] = bucketFiles[i][hitPartition].loadAsync(nums[i], sharedIndex[i]);
        }
        for (Future future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                // 数据不均匀会导致这里溢出
                return 0;
            }
        }

        int currentBlockBeginIndex = 0;
        int currentBlockEndIndex;
        int resultIndex = 0;
        int hitmBlock = -1;
        for (int i = 0; i < 8; i++) {
            int dataNum = 0;
            for (int j = 0; j < threadNum; j++) {
                dataNum += sharedIndex[j][i];
            }
            currentBlockEndIndex = currentBlockBeginIndex + dataNum;
            if (currentBlockBeginIndex <= blockIndex && blockIndex < currentBlockEndIndex) {
                resultIndex = blockIndex - currentBlockBeginIndex;
                hitmBlock = i;
                break;
            }
            currentBlockBeginIndex = currentBlockEndIndex;
        }

        System.out.println("io cost " + (System.currentTimeMillis() - ioStart) + " ms");

        long[] sharedBuffer = Util.sharedBuffer;
        int sharedBufferIndex = 0;
        for (int i = 0; i < threadNum; i++) {
            for (int j = 0; j < sharedIndex[i][hitmBlock]; j++) {
                sharedBuffer[sharedBufferIndex++] = nums[i][hitmBlock][j];
            }
        }

        //long copyStart = System.currentTimeMillis();
        //long[][][] pnums = Util.sharedBuffers;
        //int[] sharedIndex = Util.sharedIndex;
        //Arrays.fill(sharedIndex, 0);
        //for (int i = 0; i < partitionDataNum; i++) {
        //    int p = (int)((nums[i] >> 54) & 0x07);
        //    pnums[p][sharedIndex[p]++] = nums[i];
        //}
        //System.out.println("copy cost " + (System.currentTimeMillis() - copyStart) + " ms");

        //int[] a = new int[10];
        //Arrays.fill(a, 0);
        //for (int i = 0; i < partitionDataNum; i++) {
        //    a[(int)((nums[i] >> 56) & 0x01)]++;
        //}
        //for (int i = 0; i < 2; i++) {
        //    System.out.printf("%d ", a[i]);
        //}
        //System.out.println();
        //Arrays.fill(a, 0);
        //for (int i = 0; i < partitionDataNum; i++) {
        //    a[(int)((nums[i] >> 55) & 0x03)]++;
        //}
        //for (int i = 0; i < 4; i++) {
        //    System.out.printf("%d ", a[i]);
        //}
        //System.out.println();
        //Arrays.fill(a, 0);
        //for (int i = 0; i < partitionDataNum; i++) {
        //    a[(int)((nums[i] >> 54) & 0x07)]++;
        //}
        //for (int i = 0; i < 8; i++) {
        //    System.out.printf("%d ", a[i]);
        //}
        //System.out.println();

        long start = System.currentTimeMillis();
        long result = Util.quickSelect(sharedBuffer, 0, sharedBufferIndex - 1, sharedBufferIndex - resultIndex);
        System.out.println("sort cost " + (System.currentTimeMillis() - start) + " ms");
        return result;
    }

    private int partition(long a[], int i, int j) {
        long tmp = a[j];
        int index = i;
        if (i < j) {
            for (int k = i; k < j; k++) {
                if (a[k] >= tmp) {
                    swap(a, index++, k);
                }
            }
            swap(a, index, j);
            return index;
        }
        return index;
    }

    private long search(long a[], int i, int j, int k) {
        int m = partition(a, i, j);
        if (k == m - i + 1) { return a[m]; } else if (k < m - i + 1) {
            return search(a, i, m - 1, k);
        }
        //后半段
        else {
            //核心后半段：再找第 k-(m-i+1)大的数就行
            return search(a, m + 1, j, k - (m - i + 1));
        }
    }

    //交换数组array中的下标为index1和index2的两个数组元素
    private void swap(long[] array, int index1, int index2) {
        long temp = array[index1];
        array[index1] = array[index2];
        array[index2] = temp;
    }

    private int getTotalNumer() {
        int sum = 0;
        for (int i = 0; i < threadNum; i++) {
            for (int j = 0; j < PARTITION; j++) {
                sum += bucketFiles[i][j].getDataNum();
            }
        }
        return sum;
    }

    public void flush(int threadNum) throws Exception {
        for (int j = 0; j < PARTITION; j++) {
            bucketFiles[threadNum][j].flush();
        }
    }

    public void flushPartition(int partition) throws Exception {
        for (int j = 0; j < 12; j++) {
            bucketFiles[j][partition].flush();
        }
    }
}
