package ndr.brt.mybroker.server;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import ndr.brt.mybroker.serdes.MessageSerDes;

public class Server extends AbstractVerticle {
    private final ClusterManager clusterManager;

    public Server(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    @Override
    public void start() throws Exception {
        VertxOptions options = new VertxOptions().setClusterManager(clusterManager);

        Vertx.clusteredVertx(options, response -> {
            if (response.succeeded()) {
                vertx = response.result();
                AbstractVerticle verticle = new Broker(new MessageSerDes());
                vertx.deployVerticle(verticle, deployResponse -> {
                    System.out.println("chissà come sarà andato il deploy! " + deployResponse);
                });
            } else {
                response.cause().printStackTrace();
                throw new RuntimeException(response.cause());
            }
        });
    }

    public static void main(String[] args) {
        ClusterManager clusterManager = new HazelcastClusterManager();

        Server server = new Server(clusterManager);
        Vertx.vertx().deployVerticle(server);
    }
}
