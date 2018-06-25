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
import ndr.brt.mybroker.protocol.Header;
import ndr.brt.mybroker.protocol.Message;
import ndr.brt.mybroker.protocol.request.Register;
import ndr.brt.mybroker.protocol.request.Request;
import ndr.brt.mybroker.protocol.request.Unregister;
import ndr.brt.mybroker.protocol.response.LeadershipNotification;
import ndr.brt.mybroker.protocol.response.Registered;
import ndr.brt.mybroker.protocol.response.Response;
import ndr.brt.mybroker.protocol.response.Unregistered;
import ndr.brt.mybroker.serdes.MessageSerDes;
import ndr.brt.mybroker.serdes.SerDes;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.vertx.core.buffer.Buffer.buffer;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class NetworkClient {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Vertx vertx;
    private SerDes<Message> serDes;
    private boolean isConnected = false;
    private NetSocket socket;
    private final ConcurrentMap<String, Request> requests = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Response> responses = new ConcurrentHashMap<>();
    private ExecutorService executor = newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private final AtomicBoolean isLeader;

    public NetworkClient(Vertx vertx, SerDes<Message> serDes) {
        this.vertx = vertx;
        this.serDes = serDes;
        this.socket = bind("localhost", 9999);
        isLeader = new AtomicBoolean();
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
                    receive(data);
                    parser.fixedSizeMode(4);
                    size = -1;
                }
            }
        };
    }

    private void receive(Message data) {
        logger.info(data.getClass().getSimpleName() + " received");
        if (Registered.class.isInstance(data)) {
            responses.put(Registered.class.cast(data).correlationId(), (Response) data);
            isConnected = true;
        } else if (Unregistered.class.isInstance(data)) {
            isConnected = false;
        } else if (LeadershipNotification.class.isInstance(data)) {
            LeadershipNotification leadership = LeadershipNotification.class.cast(data);
            isLeader.set(leadership.isLeader());
            logger.info("Am I the leader? " + isLeader.get());
        } else {
            throw new RuntimeException("Unknown message");
        }
    }

    public Registered register(final String clientId, final String username, final String password) {
        CompletableFuture<Registered> promise = new CompletableFuture<>();
        Register request = new Register(Header.headerFor(clientId), username, password);
        try {
            send(request,
                    response -> promise.complete((Registered) response));
        } catch (Exception e) {
            promise.completeExceptionally(e);
        }
        return Async.await(promise);
    }

    public Unregistered unregister(final String clientId) {
        CompletableFuture<Unregistered> promise = new CompletableFuture<>();
        try {
            Request request = new Unregister(Header.headerFor(clientId));
            send(request,
                    response -> promise.complete((Unregistered) response));
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

    public static void main(String[] args) throws InterruptedException {
        Vertx vertx = Vertx.vertx();
        NetworkClient client = new NetworkClient(vertx, new MessageSerDes());
        System.out.println("Binded! " + client);
        String clientId = "solitoId";
        Registered registered = client.register(clientId, "Gigi", "Sabani");
        System.out.println("Registered! " + registered);
    }
}
