package ndr.brt.mybroker.protocol;

public class Header {
    private final String clientId;
    private final String messageId;
    private final String correlationId;
    private final String ipAddress;
    private final long timestamp;

    public Header(String clientId, String messageId, String correlationId, String ipAddress, long timestamp) {
        this.clientId = clientId;
        this.messageId = messageId;
        this.correlationId = correlationId;
        this.ipAddress = ipAddress;
        this.timestamp = timestamp;
    }
}
