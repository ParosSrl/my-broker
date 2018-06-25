package ndr.brt.mybroker.protocol;

public class Header {
    final String clientId;
    final String messageId;
    final String correlationId;
    final String ipAddress;
    final long timestamp;

    public Header(String clientId, String messageId, String correlationId, String ipAddress, long timestamp) {
        this.clientId = clientId;
        this.messageId = messageId;
        this.correlationId = correlationId;
        this.ipAddress = ipAddress;
        this.timestamp = timestamp;
    }
}
