package edu.buffalo.cse.cse486586.simpledht;

public class Node implements Comparable<Node>{
    String port;
    String myHash;
    String mySuccessor;
    String myPredecessor;

    // Constructor initializes Node's Predecessor & Successor as its own hash.
    public Node(String myHash, String port) {
        this.myHash = myHash;
        this.mySuccessor = myHash;
        this.myPredecessor = myHash;
        this.port = port;
    }

    // Compares two nodes on the basis of their myHash values.
    @Override
    public int compareTo(Node another) {
        return this.myHash.compareTo(another.myHash);
    }
}
