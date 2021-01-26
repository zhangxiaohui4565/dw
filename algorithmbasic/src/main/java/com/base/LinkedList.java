package com.base;

public class LinkedList {

    public static void main(String[] args) {

        LinkedList seasons = new LinkedList();
        seasons.printList();

        seasons.addToHead("summer");
        seasons.addToHead("spring");
        seasons.printList();

        seasons.addToTail("fall");
        seasons.addToTail("winter");
        seasons.printList();

        seasons.removeHead();
        seasons.printList();
    }

    private Node removeHead() {
      if (this.head == null){
        return null;
      }else {
        this.head = this.head.getNextNode();
        return this.head;
      }
    }

    private void addToTail(String data) {

        Node tail = this.head;
        if (tail == null) {
            addToHead(data);
        } else {
            while (tail.getNextNode() != null) {
                tail = tail.getNextNode();
            }
            tail.setNextNode(new Node(data));
        }

    }

    private void addToHead(String data) {
        Node node = new Node(data);
        Node curHead = this.head;
        this.head = node;
        node.setNextNode(curHead);

    }

    public Node head;

    public LinkedList() {
        this.head = null;
    }


    public String printList() {
        String output = "<head> ";
        Node currentNode = this.head;
        while (currentNode != null) {
            output += currentNode.value + " ";
            currentNode = currentNode.getNextNode();
        }
        output += "<tail>";
        System.out.println(output);
        return output;
    }

}

