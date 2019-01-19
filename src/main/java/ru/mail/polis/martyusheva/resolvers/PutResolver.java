package ru.mail.polis.martyusheva.resolvers;

import one.nio.http.HttpClient;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.martyusheva.cluster.ClusterConfig;
import ru.mail.polis.martyusheva.cluster.ClusterRequest;

import java.io.IOException;
import java.util.List;

import static ru.mail.polis.martyusheva.Utils.*;

public class PutResolver implements RequestResolver {
    private final ClusterConfig cluster;

    public PutResolver(ClusterConfig clusterConfig) {
        this.cluster = clusterConfig;
    }

    public void resolve(@NotNull final HttpSession session, @NotNull final ClusterRequest query) throws IOException {
        if(query.isProxied()){
            cluster.dao().upsert(query.getId().getBytes(), query.getValue());
            session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
        } else {
            List<Integer> nodesForId = getNodesById(query.getId(), query.getFrom());

            int ack = 0;
            for (Integer node: nodesForId){
                try {
                    if (node == cluster.nodeId()) {
                        cluster.dao().upsert(query.getId().getBytes(), query.getValue());
                        ack++;
                    } else if (sendProxied(node, query.getId(), query.getValue()).getStatus() == 201) {
                        ack++;
                    }
                } catch (Exception e){
                    //LOGGER?
                }
            }

            if (ack >= query.getAck())
                session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
            else
                session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
        }
    }

    private Response sendProxied(Integer node, String id, byte[] body) throws Exception{
        String request = new StringBuilder().append(ENTITY_PATH).append(PARAMS_SYMBOL).append("id=").append(id).toString();

        HttpClient client = cluster.nodes().get(node);
        return client.put(request, body, proxyHeaders);
    }

}
