package sort;

/**
 * 依据归并排序计算倒序对
 */
class ReversePairs {
    public static int reversePairs(int[] nums) {
        int len = nums.length;
        if (len < 2) {
            return 0;
        }
        return reversePairs(nums, 0, len - 1);
    }

    /**
     * @param nums
     * @param left
     * @param right
     * @return
     */
    private static int reversePairs(int[] nums, int left, int right) {
        if (left >= right) {
            return 0;
        }
        int mid = left + (right - left) / 2;
        int leftSum = reversePairs(nums, left, mid);
        int rightSum = reversePairs(nums, mid + 1, right);
        int mergeSum = mergeAndSum(nums, left, mid, right);
        return mergeSum + leftSum + rightSum;
    }

    // 14 23
    private static int mergeAndSum(int[] nums, int left, int mid, int right) {
        int count = 0;
        int[] help = new int[right - left + 1];
        int lp = left;
        int rp = mid + 1;
        int hi = 0;
        for (int k = left; k <= right; k++) {

            // 左边届溢出
            if (lp == mid + 1) {
                help[hi] = nums[rp];
                rp++;

            } else if (rp == right + 1) {
                //右边界溢出
                help[hi] = nums[lp];
                lp++;
            } else if (nums[lp] <= nums[rp]) {
                help[hi] = nums[lp];
                lp++;
            } else {
                help[hi] = nums[rp];
                rp++;
                count += (mid - lp + 1);
            }
            hi++;
        }
        for (int j = 0; j < help.length; j++) {
            nums[left + j] = help[j];
        }
        return count;
    }

    public static void main(String[] args) {
        int[] test = {1, 2, 2, 7, 5, 4, 3, 4, 32, 32, 4, 4, 2, 2, 9};
        System.out.println(reversePairs(test));
    }
}