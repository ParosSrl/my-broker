package ndr.brt.mybroker.protocol.request;

import ndr.brt.mybroker.protocol.Header;
import ndr.brt.mybroker.protocol.Message;

public class Request implements Message {
    private final Header header;

    public Request(Header header) {
        this.header = header;
    }

    public Header header() {
        return header;
    }

    public String clientId() {
        return header().clientId();
    }

    public String correlationId() {
        return header().correlationId();
    }
}
