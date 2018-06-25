package ndr.brt.mybroker.protocol.response;

import ndr.brt.mybroker.protocol.Header;

public class LeadershipNotification extends Response {
    private final boolean isLeader;

    public LeadershipNotification(Header header, boolean isLeader) {
        super(header);
        this.isLeader = isLeader;
    }

    public boolean isLeader() {
        return isLeader;
    }
}
