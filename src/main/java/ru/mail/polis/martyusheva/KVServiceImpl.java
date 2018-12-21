package ru.mail.polis.martyusheva;

import one.nio.http.*;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Created by moresmart on 13.12.18.
 */
public class KVServiceImpl extends HttpServer implements KVService{
    private final KVDao dao;

    public KVServiceImpl(HttpServerConfig config, KVDao dao) throws IOException{
        super(config);
        this.dao = dao;
    }

    @Path("/v0/status")
    public void status(Request request, HttpSession session) throws IOException{
        session.sendResponse(Response.ok(Response.EMPTY));
    }

    @Path("/v0/entity")
    public void entity(Request request, HttpSession session) throws IOException{
        String id = request.getParameter("id=");

        try {
            if (id == null || id.isEmpty()) {
                session.sendError(Response.BAD_REQUEST, null);
            }

            switch (request.getMethod()) {
                case Request.METHOD_PUT: {
                    dao.upsert(id.getBytes(), request.getBody());
                    session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
                    return;
                }
                case Request.METHOD_GET: {
                    session.sendResponse(new Response(Response.OK, dao.get(id.getBytes())));
                    return;
                }
                case Request.METHOD_DELETE: {
                    dao.remove(id.getBytes());
                    session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
                    return;
                }
                default: {
                    session.sendError(Response.METHOD_NOT_ALLOWED, null);
                }
            }
        } catch (NoSuchElementException nSEE) {
            session.sendError(Response.NOT_FOUND, null);
            nSEE.printStackTrace();
        } catch (Exception e) {
            session.sendError(Response.INTERNAL_ERROR, null);
            e.printStackTrace();
        }
    }

    public void handleDefaulte(Request request, HttpSession session) throws IOException {
        session.sendError(Response.NOT_FOUND, null);
    }
}
