package sort;

/**
 * @author zhangxh
 * @version 1.0
 * @date 2020/12/3 8:30
 */
public class InsertSort {
    public static void insertSort(int arr[]) {
        for (int i = 1; i < arr.length; i++) {
            for (int j = i; j > 0; j--) {
                if (arr[j-1] > arr[j]){
                    swap(arr,j-1,j);
                }
            }
        }
    }

    private static void swap(int arr[], int i, int j) {
        int temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
    }

    public static void main(String[] args) {
        int[] test = {1, 2, 2, 7, 5, 4, 3, 4,32,32,4,4,2,2,9};
        System.out.println(test.toString());
//        mergeSort1(test);
//        insertSort(test);
        insertSort(test);
        System.out.println(test.toString());
    }
}
