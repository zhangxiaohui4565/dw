package likou;

/**
 * 合并两个有序列表
 * @author zhangxh
 * @version 1.0
 * @date 2021/3/3 16:27
 */
public class Soulution_21 {
    class ListNode{
        int val;
        ListNode next;
        ListNode(int val, ListNode next) { this.val = val; this.next = next; }
    }
    public ListNode mergeTwoLists(ListNode l1, ListNode l2) {
        ListNode head = null;
        ListNode leftP = l1;
        ListNode rightP = l2;
        if(leftP.val<=rightP.val){
            head = leftP;
            leftP = leftP.next;
        }else{
            head = rightP;
            rightP =rightP.next;
        }
        ListNode cur =  head;
        while(leftP != null && rightP!=null){
            if(leftP.val <= rightP.val ){
                cur.next = leftP;
                leftP = leftP.next;

            }else {
                cur.next = rightP;
                rightP =rightP.next;
            }
            cur = cur.next;
        }
        while (leftP != null){
            cur.next = leftP;
            leftP = leftP.next;
            cur = cur.next;
        }
        while (rightP!=null){
            cur.next = rightP;
            rightP =rightP.next;
            cur = cur.next;
        }
        return head;
    }
}
