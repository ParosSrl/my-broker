package ndr.brt.mybroker.server;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetSocket;
import io.vertx.core.shareddata.Lock;
import ndr.brt.mybroker.protocol.Message;
import ndr.brt.mybroker.protocol.request.Register;
import ndr.brt.mybroker.protocol.request.Request;
import ndr.brt.mybroker.protocol.response.LeadershipNotification;
import ndr.brt.mybroker.protocol.response.Registered;
import ndr.brt.mybroker.protocol.response.Response;
import ndr.brt.mybroker.serdes.SerDes;

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

    Client(Broker broker, String clientId, NetSocket socket, SerDes<Message> serdes) {
        this.broker = broker;
        this.clientId = clientId;
        this.socket = socket;
        this.serdes = serdes;
    }

    public void close() {
        if (lock != null) {
            broker.vertx().executeBlocking(future -> {
                logger.info("Releasing lock for client " + clientId);
                lock.release();
                future.complete();
            }, false,
            handler -> logger.info("Lock released for client " + clientId));
        }
        isClosed = true;
        socket.close();
    }

    public boolean isClosed() {
        return isClosed;
    }

    public void handle(final Request request) {
        if (Register.class.isInstance(request)) {
            Registered registered = new Registered(headerFor(clientId, request.correlationId()), System.currentTimeMillis());
            send(socket, registered);
            acquire(broker.vertx());
        }
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

    private void notifyLeadership(NetSocket socket, String clientId, boolean isLeader) {
        send(socket, new LeadershipNotification(headerFor(clientId, "no-correlation-id"), isLeader));
    }

    public String clientId() {
        return clientId;
    }
}
