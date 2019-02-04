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
import java.util.NoSuchElementException;
import java.util.logging.Logger;

import static ru.mail.polis.martyusheva.Utils.*;


public class GetResolver implements RequestResolver {
    private final Logger logger;
    private final ClusterConfig cluster;

    public GetResolver(ClusterConfig clusterConfig) {
        this.cluster = clusterConfig;
        this.logger = Logger.getLogger(GetResolver.class.getName());
    }

    public void resolve(@NotNull final HttpSession session, @NotNull final ClusterRequest query) throws IOException {
        if (query.isProxied()) {
            localGet(session, query);
        } else {
            List<Integer> nodesForId = getNodesById(query.getId(), query.getFrom());

            ClusterResponse clusterResponse = new ClusterResponse();
            for (Integer node : nodesForId) {
                try {
                    if (node == cluster.nodeId()) {
                        clusterResponse.addResponse(localGetClusterResponse(query));
                    } else
                        clusterResponse.addResponse(proxiedGetClusterResponse(node, query.getId()));
                } catch(Exception e){
                    logger.info(GetResolver.class.getName() + e.getMessage());

                }
            }
            sendResponse(session, query, clusterResponse);
        }
    }


    private void sendResponse(@NotNull HttpSession session, @NotNull ClusterRequest query, ClusterResponse response) throws IOException {
        if (response.getSuccessAck() >= query.getAck()) {
            if (response.getNotFound() == response.getSuccessAck() || response.getRemoved() > 0)
                session.sendResponse(new Response(Response.NOT_FOUND, Response.EMPTY));
            else if (response.getValue() != null)
                session.sendResponse(new Response(Response.OK, response.getValue()));
            else
                session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
        } else {
            session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
        }
    }

    private ClusterResponse proxiedGetClusterResponse(int node, String id) throws Exception {
        ClusterResponse response = new ClusterResponse();
        String request = new StringBuilder().append(ENTITY_PATH).append(PARAMS_SYMBOL).append("id=").append(id).toString();
        HttpClient client = cluster.nodes().get(node);
        Response httpResponse = client.get(request, proxyHeaders);
        if (httpResponse.getStatus() == 200) {
            response.value(httpResponse.getBody());
        } else if (httpResponse.getStatus() == 403) {
            response.addRemoved();
        } else if (httpResponse.getStatus() == 404) {
            response.addNotFound();
        }
        response.addSuccessAck();

        return response;
    }

    private ClusterResponse localGetClusterResponse(@NotNull ClusterRequest query) throws IOException {
        ClusterResponse response = new ClusterResponse();
        try {
            if (cluster.removedIds().contains(query.getId())) {
                response.addRemoved();
            } else {
                response.value(cluster.dao().get(query.getId().getBytes()));
            }
        } catch (NoSuchElementException e) {
            response.addNotFound();
        }
        response.addSuccessAck();
        return response;
    }

    private void localGet(@NotNull HttpSession session, @NotNull final ClusterRequest query) throws IOException {
        try {
            final String id = query.getId();

            if (cluster.removedIds().contains(id)) {
                session.sendResponse(new Response(Response.FORBIDDEN, Response.EMPTY));
            }

            final byte[] value;
            if (id != null) {
                value = cluster.dao().get(id.getBytes());
                session.sendResponse(new Response(Response.OK, value));
            }
        } catch (NoSuchElementException e) {
            session.sendResponse(new Response(Response.NOT_FOUND, Response.EMPTY));
        }
    }
}
