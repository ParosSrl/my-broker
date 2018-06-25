package ndr.brt.mybroker.protocol.response;

import ndr.brt.mybroker.protocol.Header;
import ndr.brt.mybroker.protocol.Message;

public class Response implements Message {
    private final Header header;

    public Response(Header header) {
        this.header = header;
    }

    public String correlationId() {
        return header.correlationId();
    }
}
