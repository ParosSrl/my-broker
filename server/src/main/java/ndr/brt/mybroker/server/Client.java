package ndr.brt.mybroker.server;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetSocket;
import io.vertx.core.shareddata.Lock;
import ndr.brt.mybroker.protocol.ConsumerRecord;
import ndr.brt.mybroker.protocol.Message;
import ndr.brt.mybroker.protocol.request.Consume;
import ndr.brt.mybroker.protocol.request.Produce;
import ndr.brt.mybroker.protocol.request.Register;
import ndr.brt.mybroker.protocol.response.*;
import ndr.brt.mybroker.serdes.SerDes;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static ndr.brt.mybroker.protocol.Header.headerFor;

class Client {

    private Broker broker;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private boolean isClosed = false;
    private Lock lock;
    private boolean isLeader;
    private final String clientId;
    private final NetSocket socket;
    private final SerDes<Message> serdes;
    private final Store store;

    Client(Broker broker, String clientId, NetSocket socket, SerDes<Message> serdes, Store store) {
        this.broker = broker;
        this.clientId = clientId;
        this.socket = socket;
        this.serdes = serdes;
        this.store = store;
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

        send(socket, new Consumed(headerFor(clientId, consume.correlationId()), records));
    }

    private void notifyLeadership(NetSocket socket, String clientId, boolean isLeader) {
        send(socket, new LeadershipNotification(headerFor(clientId, "no-correlation-id"), isLeader));
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
        vertx.sharedData().getLock(clientId, result -> {
            if (result.succeeded()) {
                lock = result.result();
                isLeader = true;
                notifyLeadership(socket, clientId, isLeader);
                logger.info("Lock acquired for client " + clientId);
                logger.info("Client " + clientId +" is now leader");
            } else {
                vertx.setTimer(1000, res -> acquire(vertx));
            }
        });
    }
}
