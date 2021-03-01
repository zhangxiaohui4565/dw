package sort;

/**
 * study quick sort
 * @author zhangxh
 * @version 1.0
 * @date 2021/3/1 9:46
 */
public class QuickSort {


    public static void main(String[] args) {
        int[] ints = {1, 2, 3, 4, 32, 2, 1, 2, 3, 5};
        new QuickSort().process(ints,0,ints.length-1);
        System.out.println(ints.toString());
    }

    public void process(int[] nums,int left,int right ){
        if (nums != null && nums.length <= 1){
            return;
        }
        if (left>=right){
            return;
        }
        swap(nums,left+(int)(Math.random()*(right-left)+1),right);
        int[] partitions = partitions(nums, left, right);
        process(nums,left,partitions[0]-1);
        process(nums,partitions[1]+1,right);
    }

    public int[] partitions(int[] nums,int left,int right){
        int mark = nums[right];
        int less = left-1;
        int more = right;
        int index = left;
        while(index<more){
            if(nums[index] < mark){
                less++;
                swap(nums,index,less);
                index++;
            }else if(nums[index] == mark){
                index++;
            }else {
                more --;
                swap(nums,index,more);
            }
        }
        swap(nums,right,more);
        return new int[]{less+1,more};
    }

    public void swap(int[] nums,int left,int right){
        int temp = nums[left];
        nums[left] = nums[right];
        nums[right] = temp;
    }
}
