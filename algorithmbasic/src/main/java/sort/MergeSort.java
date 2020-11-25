package sort;

/**
 * @author zhangxh
 * @version 1.0
 * @date 2020/11/25 11:21
 * 归并排序
 */
public class MergeSort {

    public static void mergeSort1(int[] arr) {
        if (arr == null || arr.length < 2) {
            return;
        }
        process1(arr, 0, arr.length - 1);
    }

    private static void process1(int[] arr, int l, int r) {
        if (l >= r) {
            return;
        }
        //
        int m = l + ((r - l) >> 1);
        process1(arr, l, m);
        process1(arr, m + 1, r);
        merge1(arr, l, m, r);
    }

    public static void merge1(int[] arr, int l, int m, int r) {
        int[] help = new int[r - l + 1];
        int lp = l;
        int rp = m + 1;
        int i = 0;
        while (lp <= m && rp <= r) {
            help[i++] = arr[lp] < arr[rp] ? arr[lp++] : arr[rp++];
        }
        while (lp <= m) {
            help[i++] = arr[lp++];
        }
        while (rp <= r) {
            help[i++] = arr[rp++];
        }
        for (int j = 0; j < help.length; j++) {
            arr[l + j] = help[j];
        }
    }
    public static void mergeSort2(int[] arr) {
        /**
         * 从长度为1 开始 ，逐个进行左组右组归并。
         */
        if (arr == null || arr.length < 2) {
            return;
        }
        int allLength = arr.length;
        int mergeSize = 1;// 当前有序的，左组长度
        while (mergeSize < allLength) { // log N
            int L = 0;
            // 0....
            while (L < allLength) {
                // L...M  左组（mergeSize）
                int M = L + mergeSize - 1;
                if (M >= allLength) {
                    break;
                }
                //  L...M   M+1...R(mergeSize)
                int R = Math.min(M + mergeSize, allLength - 1);
                merge1(arr, L, M, R);
                L = R + 1;
            }
            if (mergeSize > allLength / 2) {
                break;
            }
            // 每次组内数组个数×2
            mergeSize <<= 1;
        }
    }
    public static void main(String[] args) {
        int[] test = {1, 2, 2, 7, 5, 4, 3, 4,32,32,4,4,2,2,9};
        System.out.println(test.toString());
//        mergeSort1(test);
        mergeSort2(test);
        System.out.println(test.toString());
    }

}
