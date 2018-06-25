package ndr.brt.mybroker.server;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.core.streams.ReadStream;
import ndr.brt.mybroker.protocol.Header;
import ndr.brt.mybroker.protocol.Message;
import ndr.brt.mybroker.protocol.request.Register;
import ndr.brt.mybroker.protocol.request.Request;
import ndr.brt.mybroker.protocol.request.Unregister;
import ndr.brt.mybroker.protocol.response.Registered;
import ndr.brt.mybroker.protocol.response.Response;
import ndr.brt.mybroker.serdes.SerDes;

import java.util.HashMap;
import java.util.Map;

public class Broker extends AbstractVerticle {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Map<NetSocket, Client> clients;
    private final SerDes<Message> serdes;

    public Broker(SerDes<Message> serdes) {
        this.clients = new HashMap<>();
        this.serdes = serdes;
    }

    @Override
    public void start() throws Exception {
        NetServer server = vertx.createNetServer();

        server.connectHandler(socket -> {
            final RecordParser parser = RecordParser.newFixed(4, (ReadStream<Buffer>) null);
            final Handler<Buffer> handler = new Handler<Buffer>() {

                int size = -1;

                @Override
                public void handle(Buffer buffer) {
                    if (size == -1) {
                        size = buffer.getInt(0);
                        parser.fixedSizeMode(size);
                    } else {
                        final Request data = (Request) serdes.deserialize(buffer.getBytes());
                        receive(socket, data);
                        //metrics.messageRead(received.address(), buff.length());
                        parser.fixedSizeMode(4);
                        size = -1;
                    }
                }
            };

            parser.setOutput(handler);

            socket.handler(parser);

            socket.closeHandler(buffer -> {

                final Client client = clients.get(socket);
                if (client != null) {
                    if (!client.isClosed()) {
                        client.close();
                        logger.info("Client {} disconnected", client.clientId);
                        clients.remove(socket);
                    }
                } else {
                    logger.warn("Unknown client {} disconnected", client.clientId);
                }
            });

            socket.exceptionHandler(Throwable::printStackTrace);
        });

        server.listen(9999, "localhost", handler -> {
            if (handler.succeeded()) {
                logger.info("Broker (1) listening on localhost:9999");
            } else {
                logger.error("Error", handler.cause());
                throw new RuntimeException(handler.cause());
            }
        });
    }

    private void receive(NetSocket socket, Request data) {
        if (Register.class.isInstance(data)) {
            logger.info("Client {} register request", data.clientId());
            Register register = (Register) data;

            if (clients.containsKey(socket)) {
                logger.info("Client {} already exists", data.clientId());
            }
            else {
                Client client = new Client(register.clientId(), socket);
                clients.put(socket, client);
                client.handle(data);
                logger.info("Client {} registered", data.clientId());
            }
        } else if (Unregister.class.isInstance(data)) {
            logger.info("Client {} unregister request", data.clientId());
            if (clients.containsKey(socket)) {
                clients.remove(socket);
            }

        } else {
            logger.error("Message unknown");
            throw new RuntimeException("Message unknown");
        }
    }

    private class Client {

        private boolean isClosed = false;
        private final String clientId;
        private final NetSocket socket;

        private Client(String clientId, NetSocket socket) {
            this.clientId = clientId;
            this.socket = socket;
        }

        public void close() {
            isClosed = true;
        }

        public boolean isClosed() {
            return isClosed;
        }

        public void handle(final Request request) {
            if (Register.class.isInstance(request)) {
                Registered registered = new Registered(Header.headerFor(clientId, request.correlationId()), System.currentTimeMillis());
                send(socket, registered);
            }
        }

        private void send(final NetSocket socket, final Response message) {

            if (logger.isDebugEnabled()) logger.debug("Sending " + message);
            vertx.executeBlocking(future -> {
                final byte[] response = serdes.serialize(message);
                socket.write(Buffer.buffer()
                        .appendInt(response.length)
                        .appendBytes(response));

                future.complete();
            }, response -> {
                if (response.failed()) {
                    logger.error("Send failed", response.cause());
                    close();
                }
            });
        }
    }

}
