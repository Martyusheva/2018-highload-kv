package ru.mail.polis.martyusheva.resolvers;

import one.nio.http.HttpClient;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.martyusheva.cluster.ClusterResponse;
import ru.mail.polis.martyusheva.cluster.ClusterRequest;
import ru.mail.polis.martyusheva.cluster.ClusterConfig;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;
import static ru.mail.polis.martyusheva.Utils.*;


public class DeleteResolver implements RequestResolver {
    private final Logger logger;
    private final ClusterConfig cluster;

    public DeleteResolver(ClusterConfig clusterConfig) {
        this.cluster = clusterConfig;
        this.logger = Logger.getLogger(DeleteResolver.class.getName());
    }

    public void resolve(@NotNull final HttpSession session, @NotNull final ClusterRequest query)throws IOException {
        cluster.removedIds().add(query.getId());
        if (query.isProxied()){
            cluster.dao().remove(query.getId().getBytes());
            session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
        } else {
            List<Integer> nodesForId = getNodesById(query.getId(), query.getFrom());
            ClusterResponse clusterResponse = new ClusterResponse();
            for (Integer node: nodesForId){
                try {
                    if (node == cluster.nodeId()) {
                        cluster.dao().remove(query.getId().getBytes());
                        clusterResponse.addSuccessAck();
                    } else if (sendProxied(node, query.getId()).getStatus() == 202){
                        clusterResponse.addSuccessAck();
                    }

                } catch (Exception e) {
                    logger.info(DeleteResolver.class.getName() + e.getMessage());
                }
            }

            sendResponse(session, query, clusterResponse);
        }
    }

    private Response sendProxied(Integer node, String id) throws Exception{
        String request = new StringBuilder().append(ENTITY_PATH).append(PARAMS_SYMBOL).append("id=").append(id).toString();

        HttpClient client = cluster.nodes().get(node);
        return client.delete(request, proxyHeaders);
    }

    private void sendResponse(@NotNull HttpSession session, @NotNull ClusterRequest query, ClusterResponse response) throws IOException {
        if (response.getSuccessAck() >= query.getAck())
            session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
        else
            session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
    }

}
