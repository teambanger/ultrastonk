package fi.ultrastonk;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class EventProcessed implements Serializable {
    public long timestamp_emit;
    public String ticker;
    public String type;
    public double value;
    @JsonIgnore public long ingestionTimeMs;

    public EventProcessed() {}

    public EventProcessed(long timestamp, String ticker, String type, double value) {
        this.timestamp_emit = timestamp;
        this.ticker = ticker;
        this.type = type;
        this.value = value;
    }
}
