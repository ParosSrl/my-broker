package ndr.brt.mybroker.protocol.request;

import ndr.brt.mybroker.protocol.Header;
import ndr.brt.mybroker.protocol.Message;

public class Request implements Message {
    final Header header;

    public Request(Header header) {
        this.header = header;
    }
}
