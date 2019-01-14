package ru.mail.polis.martyusheva;

import one.nio.http.*;
import one.nio.net.ConnectionString;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Set;

import static java.lang.Math.abs;
import static org.iq80.leveldb.impl.Iq80DBFactory.asString;
import static ru.mail.polis.martyusheva.ReqRespProcessor.*;

/**
 * Created by moresmart on 13.12.18.
 */
public class KVServiceImpl extends HttpServer implements KVService{
    private final KVDaoImpl dao;

    private final ArrayList<HttpClient> nodes = new ArrayList<>();
    private HttpClient me = null;

    private RequestProcessor requestProcessor;
    private static final String SPLITTER = " ";

    public KVServiceImpl(HttpServerConfig config, KVDao dao, Set<String> topology) throws IOException{
        super(config);
        this.dao = (KVDaoImpl) dao;

        String port = Integer.toString(config.acceptors[0].port);
        topology.stream().forEach(node -> {
            HttpClient client = new HttpClient(new ConnectionString(node));
            nodes.add(client);
            if (me == null)
                if(node.split(":")[2].equals(port)){
                    me = client;
                }
        });
    }

    @Path(STATUS_PATH)
    public void status(Request request, HttpSession session) throws IOException{
        session.sendResponse(Response.ok(Response.EMPTY));
    }

    private RequestProcessor buildRequestProcessor (String replicas){
        return  replicas == null ?
                new RequestProcessor(nodes.size()) :
                (replicas.isEmpty() ?
                        new RequestProcessor(nodes.size()) :
                        new RequestProcessor(replicas, nodes.size()));
    }

    private String buildString (String splitter, String... chunks) {
        StringBuilder builder = new StringBuilder();
        for (String s: chunks) {
            builder.append(s).append(splitter);
        }
        return builder.toString();
    }

    @Path(ENTITY_PATH)
    public void entity(Request request, HttpSession session) throws IOException{
        String id = request.getParameter(ID_PARAM);

        try {
            if (id == null || id.isEmpty()) {
                session.sendError(Response.BAD_REQUEST, null);
                return;
            }

            requestProcessor = buildRequestProcessor(request.getParameter(REPLICAS_PARAM));

            switch (request.getMethod()) {
                case Request.METHOD_PUT: {
                    Response response = upsert(id, request.getBody(), request.getHeader(PROXY_HEADER) != null);
                    session.sendResponse(response);
                    break;
                }
                case Request.METHOD_GET: {
                    Response response = get(id, request.getHeader(PROXY_HEADER) != null);
                    session.sendResponse(response);
                    break;
                }
                case Request.METHOD_DELETE: {
                    Response response = remove(id, request.getHeader(PROXY_HEADER) != null);
                    session.sendResponse(response);
                    break;
                }
                default: {
                    session.sendError(Response.METHOD_NOT_ALLOWED, null);
                    break;
                }
            }
        } catch (IllegalArgumentException iae) {
            session.sendError(Response.BAD_REQUEST, null);
        }
    }

    private Response upsert(final String id, final  byte[] value, final boolean proxied){
        if(proxied){
            try {
                dao.upsert(id.getBytes(), value);
                return new Response(Response.CREATED, Response.EMPTY);
            } catch (IOException ioe){
                return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
            }
        } else {
            ArrayList<HttpClient> nodes = getNodes(id);

            int ack = 0;
            for (HttpClient node: nodes){
                try {
                    if (node == me) {
                        dao.upsert(id.getBytes(), value);
                        ack++;
                    } else if (sendProxied(HttpMethod.PUT, node, id, value).getStatus() == HTTP_CODE_CREATED) {
                        ack++;
                    }
                } catch (Exception e){
                    //LOGGER?
                }
            }

            return ack >= requestProcessor.getAck() ?
                    new Response(Response.CREATED, Response.EMPTY) :
                    new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response get(final String id, final boolean proxied) {
        if (proxied) {
            return localGet(id);
        } else {
            ArrayList<HttpClient> nodes = getNodes(id);

//            int ack = 0;
            ResponseProcessor responseProcessor = new ResponseProcessor(requestProcessor.getAck());
            for (HttpClient node : nodes) {
                try {
                    if (node == me) {
                        responseProcessor.put(localGet(id), node);
//                        if (localGet(id).getStatus() == HTTP_CODE_OK)
//                            ack++;
                    } else
                        responseProcessor.put(sendProxied(HttpMethod.GET, node, id, null), node);
//                        if (sendProxied(HttpMethod.GET, node, id, null).getStatus() == HTTP_CODE_OK){
//                        ack++;
//                    }
                } catch(Exception e){
                    //LOGGER?
                    responseProcessor.put(new Response(Response.INTERNAL_ERROR, Response.EMPTY), node);
                }
            }
//            return ack >= requestProcessor.getAck() ?
//                    new Response(Response.OK, ) :
//                    new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
            return responseProcessor.getResponse();
        }
    }

    private Response localGet(String id) {
        try {
            byte[] value = dao.get(id.getBytes());
//            if (value != null)
//                return new Response(Response.OK, value);
            if (value != null && !asString(value).equals("deleted"))
                return new Response(Response.OK, value);
            else if (asString(value).equals("deleted"))
                return new Response(Response.FORBIDDEN, Response.EMPTY);
            else
                return new Response(Response.NOT_FOUND, Response.EMPTY);
        } catch (IOException ioe){
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        } catch (NoSuchElementException nSEE) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private Response remove (final String id, final  boolean proxied){
        if (proxied){
            dao.remove(id.getBytes());
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } else {
            ArrayList<HttpClient> nodes = getNodes(id);
            int ack = 0;
            for (HttpClient node: nodes){
                try {
                    if (node == me) {
                        dao.remove(id.getBytes());
                        ack++;
                    } else if (sendProxied(HttpMethod.DELETE, node, id, null).getStatus() == HTTP_CODE_ACCEPTED)
                        ack++;
                } catch (Exception e) {
                    //LOGGER?
                }
            }

            return ack >= requestProcessor.getAck() ?
                    new Response(Response.ACCEPTED, Response.EMPTY) :
                    new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response sendProxied(HttpMethod method, HttpClient node, String id, byte[] body) throws Exception{
        String request = new StringBuilder().append(ENTITY_PATH).append(PARAMS_SYMBOL).append(ID_PARAM).append(id).toString();

        switch (method) {
            case PUT: return node.put(request, body, PROXY_HEADER);
            case DELETE: return node.delete(request, PROXY_HEADER);
            case GET: return node.get(request, PROXY_HEADER);
            default: return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    private ArrayList<HttpClient> getNodes(String id) throws IllegalArgumentException {
        if (id.isEmpty()) throw new IllegalArgumentException();

        int base = abs(id.hashCode()) % requestProcessor.getFrom();
        ArrayList<HttpClient> result = new ArrayList<>();
        for (int i = 0; i < requestProcessor.getFrom(); i++) {
            result.add(nodes.get(base));
            base = abs((base + 1)) % requestProcessor.getFrom();
        }

        return result;
    }

    public void handleDefaulte(Request request, HttpSession session) throws IOException {
        session.sendError(Response.NOT_FOUND, null);
    }
}
