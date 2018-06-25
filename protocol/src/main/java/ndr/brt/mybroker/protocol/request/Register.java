package ndr.brt.mybroker.protocol.request;

import ndr.brt.mybroker.protocol.Header;

public class Register extends Request {
    private final String username;
    private final String password;

    public Register(Header header, String username, String password) {
        super(header);
        this.username = username;
        this.password = password;
    }
}
