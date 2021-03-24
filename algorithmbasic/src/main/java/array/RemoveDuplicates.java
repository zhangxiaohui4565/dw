package array;

/**
 * @author zhangxh
 * @version 1.0
 * @date 2021/3/15 8:26
 */
public class RemoveDuplicates {


    public void removeDuplicates (Integer[] nums){
        Integer left = 0;
        Integer right = 1;
        Integer temp = nums[left];
        if (nums == null || nums.length <= 1){
            return ;
        }
        for (;right<nums.length;right++){
            if(nums[right] == temp){
                nums[right] = null;
            }else {
                temp = nums[right];
            }
        }
    }


    public static void main(String[] args) {
        Integer[] integers = {1, 1, 2, 2, 3, 4, 4, 5, 5, 6, 9};
        new RemoveDuplicates().removeDuplicates(integers);
        System.out.println("");
    }
}
