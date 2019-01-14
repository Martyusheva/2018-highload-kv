package ru.mail.polis.martyusheva;

import one.nio.http.HttpClient;
import one.nio.http.Response;

import java.util.ArrayList;

public class ReqRespProcessor {
    public static final int HTTP_CODE_OK = 200;
    public static final int HTTP_CODE_CREATED = 201;
    public static final int HTTP_CODE_ACCEPTED = 202;
    public static final int HTTP_CODE_NOT_FOUND = 404;

    public static final String PROXY_HEADER = "Proxied: True";
    public static final String TIMESTAMP_HEADER = "TIMESTAMP: ";

    public static final String ENTITY_PATH = "/v0/entity";
    public static final String STATUS_PATH = "/v0/status";

    public static final String PARAMS_SYMBOL = "?";

    public static final String ID_PARAM = "id=";
    public static final String REPLICAS_PARAM = "replicas=";

    public static final String REQUEST_FROM = "Request from";
    public static final String RESPONSE_TO = "Response to";
    public static final String PROXIED = "proxied";
    public static final String SPLITTER = " ";

    public enum HttpMethod {
        PUT,
        DELETE,
        GET
    }

    public static class ResponseProcessor {
        private final int condition;
        private final ArrayList<ResponseHolder> responses = new ArrayList<>();
        private ResponseHolder lastSucResponse = new ResponseHolder(null, null);

        public ResponseProcessor (int condition){
            this.condition = condition;
        }

        public void put (final Response response, final HttpClient client) throws IllegalArgumentException{
            try{
                if (response.getStatus() == HTTP_CODE_OK ||
                        response.getStatus() == HTTP_CODE_NOT_FOUND ||
                        response.getStatus() == 403){
                    ResponseHolder holder = new ResponseHolder(response, client);

                    if (response.getStatus() == HTTP_CODE_OK)
                        lastSucResponse = holder;

                    this.responses.add(holder);
                }
            } catch (Exception e){
                e.printStackTrace();
                throw  new IllegalArgumentException();
            }
        }

        public Response getResponse() {
            if (getSuccessAckAmount() >= condition) {
                return lastSucResponse.getResponse();
//                if (getRemovedAmount() > 0) {
//                    return new Response(Response.NOT_FOUND, Response.EMPTY);
//                }
//                else {
//                    return lastSucResponse.getResponse();
//                }
            }
            else
                return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }

        public int getSuccessAckAmount(){
            int amount = 0;
            for (ResponseHolder response : responses){
                if (response.getResponse().getStatus() == HTTP_CODE_OK)
                    amount++;
            }

            return amount;
        }

        public int getRemovedAmount() {
            int amount = 0;
            for (ResponseHolder response : responses){
                if (response.getResponse().getStatus() == 403)
                    amount++;
            }

            return amount;
        }

        private class ResponseHolder{
            Response response;
            HttpClient client;

            public ResponseHolder (Response response, HttpClient client) {
                this.response = response;
                this.client = client;
            }

            public Response getResponse() {
                return response;
            }

            public void setResponse(Response response) {
                this.response = response;
            }

            public HttpClient getClient() {
                return client;
            }

            public void setClient(HttpClient client) {
                this.client = client;
            }

        }
    }

    public static class RequestProcessor {
        private final int ack;
        private final int from;

        public RequestProcessor (final String replicas, int nodesAmount) throws IllegalArgumentException {
            String[] parts = replicas.split("/");
            if (parts.length != 2)
                throw  new IllegalArgumentException();

            int ack = Integer.parseInt(parts[0]);
            int from = Integer.parseInt(parts[1]);
            if (ack < 1 || ack > from || from > nodesAmount)
                throw new  IllegalArgumentException();

            this.ack = ack;
            this.from = from;
        }

        public RequestProcessor (int amount){
            this.ack = amount;
            this.from = amount;
        }

        public int getAck(){
            return ack;
        }

        public int getFrom() {
            return from;
        }
    }


}
