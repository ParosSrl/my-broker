package ndr.brt.mybroker.client;

import ndr.brt.mybroker.protocol.response.Consumed;
import ndr.brt.mybroker.serdes.MessageSerDes;

import static io.vertx.core.Vertx.vertx;

public class Consumer1 {

    public static void main(String[] args) throws InterruptedException {
        NetworkClient client = new NetworkClient(vertx(), new MessageSerDes(), "consumer1", "topic");
        client.register();

        while (true) {
            Consumed consume = client.consume();
            if (consume != null && consume.records().size() > 0) {
                System.out.println(consume.records());
            }

            Thread.sleep(100);
        }
    }

}
