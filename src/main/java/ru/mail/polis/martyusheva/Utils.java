package ru.mail.polis.martyusheva;

import one.nio.http.HttpClient;
import one.nio.http.Request;
import one.nio.net.ConnectionString;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.martyusheva.cluster.ClusterRequest;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Math.abs;

public class Utils {
    public static final String ENTITY_PATH = "/v0/entity";
    public static final String STATUS_PATH = "/v0/status";
    public static final String NODE_PATH = "http://localhost:";

    public static final String PARAMS_SYMBOL = "?";
    private static final String ID = "id";
    private static final String AND = "&";
    private static final String EQUALS = "=";
    private static final String DELIMITER = "/";
    private static final String ENCODING = "UTF-8";
    private static final String REPLICAS = "replicas";
    private static final String INVALID_QUERY = "Invalid query";

    public static final String[] proxyHeaders;

    private static final String HEADER_PROXY = "Proxied: True";

    static {
        proxyHeaders = new String[1];
        proxyHeaders[0] = HEADER_PROXY;
    }

    public static Map<Integer, HttpClient> extractNodes(List<String> topology) {
        Map<Integer, HttpClient> nodes = new HashMap<>();
        topology.stream().forEach(node -> {
            HttpClient client = new HttpClient(new ConnectionString(node));
            nodes.put(topology.indexOf(node), client);
        });

        return nodes;
    }

    public static ClusterRequest processRequest(@NotNull Request request, int topologySize) {
        String key = request.getQueryString();
        if (key == null) {
            throw new IllegalArgumentException(INVALID_QUERY);
        }

        Map<String, String> params = getParams(key);
        String id = params.get(ID);

        int ack;
        int from ;
        if (params.containsKey(REPLICAS)) {
            String[] rp = params.get(REPLICAS).split(DELIMITER);
            ack = Integer.valueOf(rp[0]);
            from = Integer.valueOf(rp[1]);
        } else {
            ack = topologySize / 2 + 1;
            from = topologySize;
        }
        if (id == null || "".equals(id) || ack < 1 || ack > from) {
            throw new IllegalArgumentException(INVALID_QUERY);
        }

        boolean isProxied = request.getHeader(HEADER_PROXY) != null;

        return new ClusterRequest(id, ack, from, request.getBody(), isProxied);
    }

    private static Map<String, String> getParams(@NotNull String query) {
        try {
            Map<String, String> params = new HashMap<>();
            for (String param : query.split(AND)) {
                int index = param.indexOf(EQUALS);
                String param1 = URLDecoder.decode(param.substring(0, index), ENCODING);
                String param2 = URLDecoder.decode(param.substring(index + 1), ENCODING);
                params.put(param1, param2);
            }
            return params;
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException(INVALID_QUERY);
        }
    }

    public static ArrayList<Integer> getNodesById(@NotNull final String id, int from) {
        ArrayList<Integer> result = new ArrayList<>();

        int base = abs(id.hashCode()) % from;
        for (int i = 0; i < from; i++) {
            result.add(base);
            base = abs((base + 1)) % from;
        }

        return result;
    }


}
