package ndr.brt.mybroker.protocol.request;

import ndr.brt.mybroker.protocol.Header;

public class Consume extends Request {

    private final String topic;
    private final long offset;

    public Consume(Header header, String topic, long offset) {
        super(header);
        this.topic = topic;
        this.offset = offset;
    }

    public String topic() {
        return topic;
    }

    public long offset() {
        return offset;
    }
}
