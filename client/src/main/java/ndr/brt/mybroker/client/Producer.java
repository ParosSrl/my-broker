package ndr.brt.mybroker.client;

import io.vertx.core.json.JsonObject;
import ndr.brt.mybroker.protocol.response.Produced;
import ndr.brt.mybroker.serdes.MessageSerDes;

import static io.vertx.core.Vertx.vertx;

public class Producer {

    public static void main(String[] args) throws InterruptedException {
        NetworkClient client = new NetworkClient(vertx(), new MessageSerDes(), "producer", "topic");
        client.register();

        int i = 0;
        while (true) {
            Produced produced = client.produce(new JsonObject()
                    .put("name", "name" + i)
                    .toBuffer()
                    .getBytes());

            System.out.println(produced.offset());

            Thread.sleep(1000);
        }
    }

}
