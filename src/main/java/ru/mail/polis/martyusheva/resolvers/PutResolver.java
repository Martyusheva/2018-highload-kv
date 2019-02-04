package ru.mail.polis.martyusheva.resolvers;

import one.nio.http.HttpClient;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.martyusheva.cluster.ClusterConfig;
import ru.mail.polis.martyusheva.cluster.ClusterRequest;
import ru.mail.polis.martyusheva.cluster.ClusterResponse;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import static ru.mail.polis.martyusheva.Utils.*;

public class PutResolver implements RequestResolver {
    private final Logger logger;
    private final ClusterConfig cluster;

    public PutResolver(ClusterConfig clusterConfig) {
        this.cluster = clusterConfig;
        this.logger = Logger.getLogger(PutResolver.class.getName());
    }

    public void resolve(@NotNull final HttpSession session, @NotNull final ClusterRequest query) throws IOException {
        if(query.isProxied()) {
            cluster.dao().upsert(query.getId().getBytes(), query.getValue());
            session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
        } else {
            List<Integer> nodesForId = getNodesById(query.getId(), query.getFrom());
            ClusterResponse clusterResponse = new ClusterResponse();
            for (Integer node: nodesForId) {
                try {
                    if (node == cluster.nodeId()) {
                        cluster.dao().upsert(query.getId().getBytes(), query.getValue());
                        clusterResponse.addSuccessAck();
                    } else if (sendProxied(node, query.getId(), query.getValue()).getStatus() == 201) {
                        clusterResponse.addSuccessAck();
                    }
                } catch (Exception e) {
                    logger.info(PutResolver.class.getName() + e.getMessage());
                }
            }

            sendResponse(session, query, clusterResponse);
        }
    }

    private Response sendProxied(Integer node, String id, byte[] body) throws Exception {
        String request = new StringBuilder().append(ENTITY_PATH).append(PARAMS_SYMBOL).append("id=").append(id).toString();

        HttpClient client = cluster.nodes().get(node);
        return client.put(request, body, proxyHeaders);
    }

    private void sendResponse(@NotNull HttpSession session, @NotNull ClusterRequest query, ClusterResponse response) throws IOException {
        if (response.getSuccessAck() >= query.getAck())
            session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
        else
            session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
    }

}
