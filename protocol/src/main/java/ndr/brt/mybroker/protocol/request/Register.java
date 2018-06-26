package ndr.brt.mybroker.protocol.request;

import ndr.brt.mybroker.protocol.Header;

public class Register extends Request {
    private final String topic;

    public Register(Header header, String topic) {
        super(header);
        this.topic = topic;
    }

    public String topic() {
        return topic;
    }

}
