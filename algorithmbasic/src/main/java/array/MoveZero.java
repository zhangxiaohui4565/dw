package array;

/**
 * 移动0
 * @author zhangxh
 * @version 1.0
 * @date 2021/1/26 14:40
 */
public class MoveZero {
    public static void main(String[] args) {
        int[] ints = {1,2,2,0,0,4,2,0};
        moveZeroes(ints);
        System.out.println(ints.toString());
    }
    
    public  static void moveZeroes(int[] nums) {
        int left = 0;
        int right = 0;
        for (;right<nums.length;right++){
            if (nums[right] != 0){

                swap(left,right,nums);
                left++;

            }
        }

    }

    public static int[] swap(int left,int right,int [] nums){
        int temp = nums[left];
        nums[left] = nums[right];
        nums[right] = temp;
        System.out.println(1);
        return nums;
    }
}
