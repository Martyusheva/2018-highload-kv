package ru.mail.polis.martyusheva.cluster;

public class ClusterRequest {
    private final int ack;
    private final int from;
    private final String id;
    private final byte[] value;
    private final boolean proxied;

    public ClusterRequest(String id, int ack, int from, byte[] value, boolean proxied) {
        this.id = id;
        this.ack = ack;
        this.from = from;
        this.value = value;
        this.proxied = proxied;
    }

    public int getAck() {
        return ack;
    }

    public int getFrom() {
        return from;
    }

    public String getId() {
        return id;
    }

    public byte[] getValue() {
        return value;
    }

    public boolean isProxied() {
        return proxied;
    }
}
