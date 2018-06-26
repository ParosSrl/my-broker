package ndr.brt.mybroker.server;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.core.streams.ReadStream;
import ndr.brt.mybroker.protocol.Message;
import ndr.brt.mybroker.protocol.request.*;
import ndr.brt.mybroker.serdes.SerDes;

import java.util.HashMap;
import java.util.Map;

public class Broker extends AbstractVerticle {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Map<NetSocket, Client> clients;
    private final SerDes<Message> serdes;
    private final Store store;

    public Broker(SerDes<Message> serdes, Store store) {
        this.store = store;
        this.clients = new HashMap<>();
        this.serdes = serdes;
    }

    @Override
    public void start() {
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
                    if (client.isOpen()) {
                        client.close();
                        logger.info("Client " + client.clientId() +" disconnected");
                        clients.remove(socket);
                    }
                } else {
                    logger.warn("Unknown client disconnected");
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
        logger.info(data.clientId() + " - " + data.getClass().getSimpleName());
        if (Register.class.isInstance(data)) {
            Register register = (Register) data;

            if (clients.containsKey(socket)) {
                logger.info("Client " + data.clientId() + " already exists");
            }
            else {
                Client client = new Client(this, register.clientId(), socket, serdes, store);
                clients.put(socket, client);
                client.handle(register);
                logger.info("Client " + data.clientId() +" registered");
            }
        } else if (Produce.class.isInstance(data)) {
            Produce produce = Produce.class.cast(data);
            Client client = clients.get(socket);
            if (client != null && client.isOpen()) {
                client.handle(produce);
            }
        } else if (Consume.class.isInstance(data)) {
            Consume consume = Consume.class.cast(data);
            Client client = clients.get(socket);
            if (client != null && client.isOpen()) {
                client.handle(consume);
            }
        } else if (Unregister.class.isInstance(data)) {
            Client client = clients.get(socket);
            if (client != null) {
                logger.info("Close client");
                client.close();
                clients.remove(socket);
            }
        } else {
            logger.error("Message unknown");
            throw new RuntimeException("Message unknown");
        }
    }

    public Vertx vertx() {
        return vertx;
    }
}
