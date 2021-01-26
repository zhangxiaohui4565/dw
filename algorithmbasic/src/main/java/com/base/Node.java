package com.base;

/**
 * @author zhangxh
 * @version 1.0
 * @date 2020/12/17 14:48
 */
public class Node {


    public String value ;
    private Node next ;

    public Node(String value){
        this.value = value;
        this.next = null;
    }

    public void setNextNode(Node node){
        this.next = node;
    }

    public Node getNextNode(){
        return  this.next;
    }
}
