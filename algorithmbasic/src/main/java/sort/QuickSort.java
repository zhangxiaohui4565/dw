package sort;

/**
 * @author zhangxh
 * @version 1.0
 * @date 2020/11/25 13:17
 */
public class QuickSort {
    public static void quickSort3(int[] arr) {
        if (arr == null || arr.length < 2) {
            return;
        }
        process3(arr, 0, arr.length - 1);
    }

    public static void process3(int[] arr, int L, int R) {
        if (L >= R) {
            return;
        }
        swap(arr, L + (int) (Math.random() * (R - L + 1)), R);
        // 获取相等部分的开始和结束位置
        int[] equalArea = netherlandsFlag(arr, L, R);
        // 将相等两边分为两部分进行递归调用
        process3(arr, L, equalArea[0] - 1);
        process3(arr, equalArea[1] + 1, R);
    }

    /**
     * 返回中间相等部分的起始位置和结束位置的数组值
     * @param arr
     * @param l
     * @param r
     * @return
     */
    private static int[] netherlandsFlag(int[] arr, int l, int r) {
        int max_start = r;
        int less_end = l-1;
        int index = l;
        while (index < max_start) {
            if (arr[index] == arr[r]){
                index++;
            }else if(arr[index] > arr[r]){
                swap(arr,index,--max_start);
            }else {
                swap(arr,index++,++less_end);
            }
        }
        swap(arr,max_start,r);
        return new int[]{less_end+1,max_start};
    }

    private static void swap(int[] arr, int i, int r) {
        int temp = arr[i];
        arr[i] = arr[r];
        arr[r] = temp;
    }

    public static void main(String[] args) {
        int[] test = {1, 2, 2, 7, 5, 4, 3, 4,32,32,4,4,2,2,9};
        quickSort3(test);
        System.out.println();
    }
}
