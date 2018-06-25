package ndr.brt.mybroker.protocol;

import java.io.Serializable;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.UUID;

public class Header implements Serializable {
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

    public static Header headerFor(String clientId) {
        return headerFor(clientId, UUID.randomUUID().toString());
    }

    public static Header headerFor(String clientId, String correlationId) {
        String hostAddress;
        try {
            hostAddress = Inet4Address.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            hostAddress = "unknown";
        }
        return new Header(
                    clientId,
                    UUID.randomUUID().toString(),
                    correlationId,
                    hostAddress,
                    System.currentTimeMillis()
            );
    }

    public String correlationId() {
        return correlationId;
    }

    public String clientId() {
        return clientId;
    }
}
