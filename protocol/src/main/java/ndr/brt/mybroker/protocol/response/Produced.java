package ndr.brt.mybroker.protocol.response;

import ndr.brt.mybroker.protocol.Header;

public class Produced extends Response {

    private final long offset;

    public Produced(Header header, long offset) {
        super(header);
        this.offset = offset;
    }

    public long offset() {
        return offset;
    }
}
