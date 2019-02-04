package ru.mail.polis.martyusheva;

import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;
import ru.mail.polis.martyusheva.cluster.ClusterRequest;
import ru.mail.polis.martyusheva.cluster.ClusterConfig;
import ru.mail.polis.martyusheva.resolvers.DeleteResolver;
import ru.mail.polis.martyusheva.resolvers.GetResolver;
import ru.mail.polis.martyusheva.resolvers.PutResolver;
import ru.mail.polis.martyusheva.resolvers.RequestResolver;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

import static ru.mail.polis.martyusheva.Utils.*;

public class KVServiceImpl extends HttpServer implements KVService {
    private final ClusterConfig clusterConfig;

    private final Map<Integer, RequestResolver> resolverMap;

    public KVServiceImpl(final int port, final KVDao dao,final Set<String> topology) throws IOException {
        super(getConfig(port));
        clusterConfig = getClusterSettings(port, dao, topology);
        resolverMap = new HashMap<>();
    }

    private static HttpServerConfig getConfig(final int port) {
        AcceptorConfig ac = new AcceptorConfig();
        HttpServerConfig config = new HttpServerConfig();
        ac.port = port;
        config.acceptors = new AcceptorConfig[]{ac};
        return config;
    }

    private ClusterConfig getClusterSettings(final int port, final KVDao dao, final Set<String> topology) {
        List<String> nodePaths = new ArrayList<>(topology);
        int nodeId = nodePaths.indexOf(NODE_PATH + port);
        Map<Integer, HttpClient> replicas = extractNodes(nodePaths);
        return new ClusterConfig(dao, nodeId, replicas, new ConcurrentSkipListSet<>());
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }

    @Path(STATUS_PATH)
    public void status(Request request, HttpSession session) throws IOException {
        session.sendResponse(Response.ok(Response.EMPTY));
    }


    @Path(ENTITY_PATH)
    public void entity(Request request, HttpSession session) throws IOException {
        try {
            ClusterRequest query = processRequest(request, clusterConfig.nodes().size());

            GetResolver getResolver = new GetResolver(clusterConfig);
            PutResolver putResolver = new PutResolver(clusterConfig);
            DeleteResolver deleteResolver = new DeleteResolver(clusterConfig);

            resolverMap.put(Request.METHOD_GET, getResolver);
            resolverMap.put(Request.METHOD_PUT, putResolver);
            resolverMap.put(Request.METHOD_DELETE, deleteResolver);

            final int method = request.getMethod();
            RequestResolver resolver = resolverMap.get(method);
            if (resolver == null) {
                session.sendError(Response.METHOD_NOT_ALLOWED, null);
            } else {
                resolver.resolve(session, query);
            }
        } catch (IllegalArgumentException iae) {
            session.sendError(Response.BAD_REQUEST, null);
        }
    }

}
