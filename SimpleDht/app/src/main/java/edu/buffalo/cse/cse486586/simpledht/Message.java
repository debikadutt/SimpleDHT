package edu.buffalo.cse.cse486586.simpledht;

/**
 * Created by debika on 4/01/16.
 */
public class Message {

    private String portID;
    private String myHash;
    private Message predecessor;
    private Message successor;
    private String max_node;
    private String min_node;

    public String getPortID() {
        return portID;
    }

    public void setPortID(String portID) {
        this.portID = portID;
    }

    public String getHashedValue() {
        return myHash;
    }

    public void setHashVal(String myHash) {
        this.myHash = myHash;
    }

    public String getMaxNode() {
        return max_node;
    }

    public String getMinNode() {
        return min_node;
    }

    public void setMin_node(String min_node) {
        this.min_node = min_node;
    }

    public void setMax_node(String max_node) {
        this.max_node = max_node;
    }

    public Message getPredecessor() {
        return predecessor;
    }

    public void setPredecessor(Message predecessor) {
        this.predecessor = predecessor;
    }

    public Message getSuccessor() {
        return successor;
    }

    public void setSuccessor(Message successor) {
        this.successor = successor;
    }





}