package sort;

/**
 * @author zhangxh
 * @version 1.0
 * @date 2021/3/3 8:21
 */
public class MergeSortTest {

    public static void main(String[] args) {
        int[] nums = {1, 34, 5, 23, 2, 45, 2, 34, 45, 4,323,323,23,2,45,5,4,2,2,43,32,0,45,45,};
        new MergeSortTest().process(nums,0,nums.length-1);
        System.out.println("");
    }

    public void process(int[] nums,int left, int right){
        // 条件终止
        if(left>=right){
            return;
        }
        int mid = left + (right-left)/2;
        // 分
        process(nums,left,mid);
        process(nums,mid+1,right);
        // 治
        merge(nums,left,mid,right);
    }

    public void merge(int[] nums, int left, int mid, int right) {
        int lp = left;
        int rp = mid + 1;
        int hp = 0;
        int[] helpNums = new int[right - left + 1];

        while (lp <= mid && rp <= right) {
            if (nums[lp] >= nums[rp]) {
                helpNums[hp++] = nums[rp++];
            }else {
                helpNums[hp++] = nums[lp++];
            }
        }
        while (lp <= mid){
            helpNums[hp++] = nums[lp++];
        }
        while (rp <= right){
            helpNums[hp++] = nums[rp++];
        }
        for (int i = 0;i<helpNums.length;i++){
            nums[left+i] = helpNums[i];
        }
    }
}
