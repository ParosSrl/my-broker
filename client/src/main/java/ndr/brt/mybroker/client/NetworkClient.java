package ndr.brt.mybroker.client;

import com.ea.async.Async;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.core.streams.ReadStream;
import ndr.brt.mybroker.protocol.ConsumerRecord;
import ndr.brt.mybroker.protocol.Header;
import ndr.brt.mybroker.protocol.Message;
import ndr.brt.mybroker.protocol.request.*;
import ndr.brt.mybroker.protocol.response.*;
import ndr.brt.mybroker.serdes.MessageSerDes;
import ndr.brt.mybroker.serdes.SerDes;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.vertx.core.Vertx.vertx;
import static io.vertx.core.buffer.Buffer.buffer;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class NetworkClient {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Vertx vertx;
    private SerDes<Message> serDes;
    private final String topic;
    private boolean isConnected = false;
    private NetSocket socket;
    private final ConcurrentMap<String, Request> requests = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Response> responses = new ConcurrentHashMap<>();
    private ExecutorService executor = newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private final AtomicBoolean isLeader;
    private final AtomicLong offset;
    private final String clientId;

    public NetworkClient(Vertx vertx, SerDes<Message> serDes, String clientId, String topic) {
        this.vertx = vertx;
        this.serDes = serDes;
        this.clientId = clientId;
        this.topic = topic;
        this.socket = bind("localhost", 9999);
        isLeader = new AtomicBoolean();
        offset = new AtomicLong();
    }

    private NetSocket bind(String host, int port) {
        CompletableFuture<NetSocket> promise = new CompletableFuture<>();
        vertx.createNetClient().connect(port, host, response -> {
            if (response.succeeded()) {
                socket = response.result();

                final RecordParser parser = RecordParser.newFixed(4, (ReadStream<Buffer>) null);
                parser.setOutput(getHandler(parser));
                socket.handler(parser);

                socket.closeHandler(handler -> {
                    //logger.info(getClientId() + " is disconnected");
                    //close();
                });

                socket.exceptionHandler(exception -> {
                    //logger.error(getClientId() + " exception: " + exception.getMessage());
                    //close();
                });

                promise.complete(socket);

            } else {
                promise.completeExceptionally(response.cause());
                throw new RuntimeException(response.cause());
            }
        });

        return Async.await(promise);
    }

    private Handler<Buffer> getHandler(final RecordParser parser) {

        return new Handler<Buffer>() {

            int size = -1;

            @Override
            public void handle(Buffer buffer) {
                if (size == -1) {
                    size = buffer.getInt(0);
                    parser.fixedSizeMode(size);
                } else {
                    final Message data = serDes.deserialize(buffer.getBytes());
                    receive((Response)data);
                    parser.fixedSizeMode(4);
                    size = -1;
                }
            }
        };
    }

    private void receive(Response response) {
        logger.info(response.getClass().getSimpleName() + " received");
        if (Registered.class.isInstance(response)) {
            responses.put(response.correlationId(), response);
            isConnected = true;
            logger.info("I'm registered!");
        } else if (Unregistered.class.isInstance(response)) {
            isConnected = false;
            logger.info("I'm unregistered! :(");
        } else if (Produced.class.isInstance(response)) {
            responses.put(response.correlationId(), response);
            logger.info("Produced!");
        } else if (Consumed.class.isInstance(response)) {
            responses.put(response.correlationId(), response);
            Consumed consumed = Consumed.class.cast(response);
            List<ConsumerRecord> records = consumed.records();
            offset.set(records.size() > 0 ? records.get(records.size() - 1).offset() : offset.get());
            logger.info("Consumed!");
        } else if (LeadershipNotification.class.isInstance(response)) {
            LeadershipNotification leadership = LeadershipNotification.class.cast(response);
            isLeader.set(leadership.isLeader());
            offset.set(leadership.offset());
            logger.info("Am I the leader? " + isLeader.get());
        } else {
            throw new RuntimeException("Unknown message");
        }
    }

    public Registered register() {
        Register request = new Register(Header.headerFor(clientId), topic);
        return syncSend(request, Registered.class);
    }

    public Produced produce(byte[] bytes) {
        Produce request = new Produce(Header.headerFor(clientId), topic, bytes);
        return syncSend(request, Produced.class);
    }

    public Unregistered unregister(final String clientId) {
        Request request = new Unregister(Header.headerFor(clientId));
        return syncSend(request, Unregistered.class);
    }

    public Consumed consume() {
        if (isLeader.get()) {
            Request request = new Consume(Header.headerFor(clientId), topic, offset.get());
            return syncSend(request, Consumed.class);
        }
        else {
            return null;
        }
    }

    private <T extends Response> T syncSend(Request request, Class<T> responseType) {
        CompletableFuture<T> promise = new CompletableFuture<>();
        try {
            send(request, response -> promise.complete(responseType.cast(response)));
        } catch (Exception e) {
            promise.completeExceptionally(e);
        }
        return Async.await(promise);
    }

    private void send(final Request request, final Handler<Response> handler) {
        CompletableFuture<Response> promise = new CompletableFuture<>();
        promise.thenAccept(handler::handle);
        executor.execute(new Task(request, promise));
    }

    private void write(final Message message) {
        byte[] bytes = serDes.serialize(message);
        socket.write(buffer()
                .appendInt(bytes.length)
                .appendBytes(bytes));
    }

    private class Task implements Runnable {

        private final Request request;
        private final CompletableFuture<Response> promise;
        private boolean keepRunning;

        private Task(final Request request, final CompletableFuture<Response> promise) {

            this.request = request;
            this.promise = promise;
            this.keepRunning = true;
        }

        @Override
        public void run() {

            final String correlationId = request.header().correlationId();
            requests.put(correlationId, request);
            write(request);
            while (keepRunning) {
                final Response response = responses.get(correlationId);
                if (response != null) {
                    responses.remove(correlationId);
                    requests.remove(correlationId);
                    promise.complete(response);
                    keepRunning = false;
                }
                Thread.yield();
            }
        }
    }

    public static void main(String[] args) {
        NetworkClient client = new NetworkClient(vertx(), new MessageSerDes(), "uniqueId", "topic");
        client.register();
    }
}
