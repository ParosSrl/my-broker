package ndr.brt.mybroker.protocol.response;

import ndr.brt.mybroker.protocol.Header;

public class LeadershipNotification extends Response {
    private final boolean isLeader;
    private final long offset;

    public LeadershipNotification(Header header, boolean isLeader, long offset) {
        super(header);
        this.isLeader = isLeader;
        this.offset = offset;
    }

    public boolean isLeader() {
        return isLeader;
    }

    public long offset() {
        return offset;
    }
}
