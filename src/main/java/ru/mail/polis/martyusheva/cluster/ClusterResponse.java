package ru.mail.polis.martyusheva.cluster;


public class ClusterResponse {
    private int successAck;
    private int notFound;
    private int removed;
    private byte[] value;

    public ClusterResponse(){
        this.successAck = 0;
        this.notFound = 0;
        this.removed = 0;
        this.value = null;
    }

    public int getSuccessAck() {
        return successAck;
    }

    public int getNotFound() {
        return notFound;
    }

    public int getRemoved() {
        return removed;
    }

    public byte[] getValue() {
        return value;
    }

    public void addSuccessAck(){
        this.successAck++;
    }

    public void addNotFound(){
        this.notFound++;
    }

    public void addRemoved(){
        this.removed++;
    }

    public void addResponse(ClusterResponse response) {
        this.successAck = this.successAck + response.getSuccessAck();
        this.notFound = this.notFound + response.getNotFound();
        this.removed = this.removed + response.getRemoved();
        if(this.value == null){
            this.value = response.getValue();
        }
    }

    public ClusterResponse value(byte[] value) {
        this.value = value;
        return this;
    }
}
