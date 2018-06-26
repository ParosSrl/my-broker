package ndr.brt.mybroker.server;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import ndr.brt.mybroker.serdes.MessageSerDes;

public class Server extends AbstractVerticle {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ClusterManager clusterManager;

    private Server(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    @Override
    public void start() {
        VertxOptions options = new VertxOptions().setClusterManager(clusterManager);

        Vertx.clusteredVertx(options, response -> {
            if (response.succeeded()) {
                vertx = response.result();
                AbstractVerticle verticle = new Broker(new MessageSerDes(), new InMemoryStore());
                vertx.deployVerticle(verticle, deployResponse -> {
                    if (deployResponse.succeeded()) {
                        logger.info("Verticle deploy succeeded");
                    }
                    else {
                        logger.error("Verticle deploy failed", deployResponse.cause());
                    }
                });
            } else {
                logger.error("Clustered Vertx failed", response.cause());
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
