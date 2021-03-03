package sort;

/**
 * @author zhangxh
 * @version 1.0
 * @date 2021/3/3 9:26
 */
public class QuickSortTest {

    public static void main(String[] args) {
        int[] nums = {1, 34, 5, 23, 2, 45, 2, 34, 45, 4,323,323,23,2,45,5,4,2,2,43,32,0,45,45,};
        new QuickSortTest().process(nums,0,nums.length-1);
        System.out.println("");
    }


    public void swap(int[] nums, int src, int dest) {
        int temp = nums[src];
        nums[src] = nums[dest];
        nums[dest] = temp;
    }

    public int[] partition(int[] nums, int left, int right) {
        int less = left - 1;
        int more = right;
        int index = left;
//
        int mark = nums[right];
        while (index<more){
            if(nums[index]<mark){
                less++;
                swap(nums,index,less);
                index++;
            }else if (nums[index]==mark){
                index ++;
            }else {
                more--;
                swap(nums,index,more);
            }
        }
        swap(nums,more,right);
        return new int[]{less + 1, more};
    }

    public  void process(int[] nums,int left,int right){
        if (left>=right){
            return;
        }
        swap(nums, left + (int) (Math.random() * (right - left)) + 1, right);
        int[] partition = partition(nums, left, right);
        process(nums,left,partition[0]-1);
        process(nums,partition[1]+1,right);
    }
}
