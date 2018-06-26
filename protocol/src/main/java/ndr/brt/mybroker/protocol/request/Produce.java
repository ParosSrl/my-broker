package ndr.brt.mybroker.protocol.request;

import ndr.brt.mybroker.protocol.Header;

public class Produce extends Request {

    private final String topic;
    private final byte[] bytes;

    public Produce(Header header, String topic, byte[] bytes) {
        super(header);
        this.topic = topic;
        this.bytes = bytes;
    }

    public String topic() {
        return topic;
    }

    public byte[] bytes() {
        return bytes;
    }
}
