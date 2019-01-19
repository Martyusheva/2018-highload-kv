package ru.mail.polis.martyusheva.cluster;

import one.nio.http.HttpClient;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.util.Map;
import java.util.Set;

public class ClusterConfig {
    @NotNull
    private final KVDao dao;
    private final int nodeId;
    private final Map<Integer, HttpClient> nodes;
    private final Set<String> removedIds;

    public ClusterConfig(@NotNull KVDao dao, int nodeId, Map<Integer, HttpClient> nodes, Set<String> removedIds) {
        this.dao = dao;
        this.nodeId = nodeId;
        this.nodes = nodes;
        this.removedIds = removedIds;
    }

    @NotNull
    public KVDao dao() {
        return dao;
    }

    public int nodeId() {
        return nodeId;
    }

    public Map<Integer, HttpClient> nodes() {
        return nodes;
    }

    public Set<String> removedIds() {
        return removedIds;
    }
}
