package com.aliyun.adb.contest;

import static com.aliyun.adb.contest.RaceAnalyticDB.PARTITION_OVER_PARTITION;
import static com.aliyun.adb.contest.RaceAnalyticDB.THREAD_NUM;

/**
 * @author jingfeng.xjf
 * @date 2021-06-28
 */
public class Util {

    //public static long[] sharedBuffer = new long[498_3700];
    //public static long[] sharedBuffer = new long[260_0000];
    //public static long[] sharedBuffer = new long[260_0000];

    public static long[] sharedBuffer = new long[60_0000];
    public static long[][][] sharedBuffers = new long[THREAD_NUM][PARTITION_OVER_PARTITION][5_0000];
    public static int[][] sharedIndex = new int[THREAD_NUM][PARTITION_OVER_PARTITION];

    public static long quickSelect(long[] nums, int start, int end, int k) {
        if (start == end) {
            return nums[start];
        }
        int left = start;
        int right = end;
        long pivot = nums[(start + end) / 2];
        while (left <= right) {
            while (left <= right && nums[left] > pivot) {
                left++;
            }
            while (left <= right && nums[right] < pivot) {
                right--;
            }
            if (left <= right) {
                long temp = nums[left];
                nums[left] = nums[right];
                nums[right] = temp;
                left++;
                right--;
            }
        }
        if (start + k - 1 <= right) {
            return quickSelect(nums, start, right, k);
        }
        if (start + k - 1 >= left) {
            return quickSelect(nums, left, end, k - (left - start));
        }
        return nums[right + 1];
    }
}
