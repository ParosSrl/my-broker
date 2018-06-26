package ndr.brt.mybroker.server;

import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.streams.ReadStream;
import ndr.brt.mybroker.protocol.ConsumerRecord;
import ndr.brt.mybroker.protocol.Message;
import ndr.brt.mybroker.protocol.request.*;
import ndr.brt.mybroker.protocol.response.*;
import ndr.brt.mybroker.serdes.SerDes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static ndr.brt.mybroker.protocol.Header.headerFor;

public class Broker extends AbstractVerticle {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Map<NetSocket, Client> clients;
    private final SerDes<Message> serdes;
    private final Store store;
    private final Map<String, Long> offsets;

    public Broker(SerDes<Message> serdes, Store store) {
        this.store = store;
        this.serdes = serdes;
        this.clients = new HashMap<>();
        this.offsets = new HashMap<>();
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
                Client client = new Client(this, register.clientId(), register.topic(), socket);
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

    private class Client {

        private Broker broker;
        private final Logger logger = LoggerFactory.getLogger(getClass());

        private boolean isClosed = false;
        private Lock lock;
        private boolean isLeader;
        private final String clientId;
        private final String topic;
        private final NetSocket socket;

        Client(Broker broker, String clientId, String topic, NetSocket socket) {
            this.broker = broker;
            this.clientId = clientId;
            this.topic = topic;
            this.socket = socket;
        }

        public String clientId() {
            return clientId;
        }

        public boolean isOpen() {
            return !isClosed;
        }

        public void handle(final Register register) {
            Registered registered = new Registered(headerFor(clientId, register.correlationId()), System.currentTimeMillis());
            send(socket, registered);
            acquire(broker.vertx());
        }

        public void handle(final Produce produce) {
            long offset = store.write(produce.topic(), produce.bytes());
            Produced produced = new Produced(headerFor(clientId, produce.correlationId()), offset);
            send(socket, produced);
        }

        public void handle(Consume consume) {
            List<ConsumerRecord> records = isLeader
                    ? store.read(consume.topic(), consume.offset())
                    : emptyList();

            if (records.size() > 0) {
                offsets.put(clientId + consume.topic(), records.get(records.size() - 1).offset());
            }

            send(socket, new Consumed(headerFor(clientId, consume.correlationId()), records));
        }

        private void notifyLeadership(NetSocket socket, String clientId, boolean isLeader, long offset) {
            send(socket, new LeadershipNotification(headerFor(clientId, "no-correlation-id"), isLeader, offset));
        }

        public void close() {
            if (lock != null) {
                Handler<Future<Object>> blockingCodeHandler = future -> {
                    logger.info("Releasing lock for client " + clientId);
                    lock.release();
                    future.complete();
                };
                Handler<AsyncResult<Object>> resultHandler = handler ->
                        logger.info("Lock released for client " + clientId);

                broker.vertx().executeBlocking(blockingCodeHandler, false, resultHandler);
            }
            isClosed = true;
            socket.close();
        }

        private void send(final NetSocket socket, final Response message) {

            if (logger.isDebugEnabled()) logger.debug("Sending " + message);
            broker.vertx().executeBlocking(future -> {
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

        private void acquire(final Vertx vertx) {

            logger.info("Acquiring lock for client " + clientId);
            vertx.sharedData().getLock(clientId + topic, result -> {
                if (result.succeeded()) {
                    lock = result.result();
                    isLeader = true;
                    Long offset = offsets.getOrDefault(clientId + topic, 0L);
                    notifyLeadership(socket, clientId, isLeader, offset);
                    logger.info("Lock acquired for client " + clientId);
                    logger.info("Client " + clientId +" is now leader");
                } else {
                    vertx.setTimer(1000, res -> acquire(vertx));
                }
            });
        }
    }
}
