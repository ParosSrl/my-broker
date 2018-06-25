package ndr.brt.mybroker.protocol.response;

import ndr.brt.mybroker.protocol.Header;

public class Registered extends Response {

    private final long timestamp;

    public Registered(Header header, long timestamp) {
        super(header);
        this.timestamp = timestamp;
    }
}
