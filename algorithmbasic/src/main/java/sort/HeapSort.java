package sort;

import java.util.Comparator;
import java.util.PriorityQueue;


/**
 * @author zhangxh
 * @version 1.0
 * @date 2020/12/1 8:29
 */
public class HeapSort {
    public static void main(String[] args) {
        PriorityQueue heap = new PriorityQueue();
        heap.add(1);
        heap.add(5);
        heap.add(7);
        heap.add(0);
        heap.add(10);
        heap.add(12);
        while (true){
            Object poll = heap.poll();
            if (poll == null){

            }

            System.out.println( );
        }
    }
}
