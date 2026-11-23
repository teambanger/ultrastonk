package fi.ultrastonk;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class PriceEvent implements Serializable {
    public long timestamp_emit;
    public String ticker;
    public String sec_type;
    public double price;
    @JsonIgnore public long ingestionTimeMs;

    public PriceEvent() {}

    public PriceEvent(long timestamp, String ticker, String secType, double price) {
        this.timestamp_emit = timestamp;
        this.ticker = ticker;
        this.sec_type = secType;
        this.price = price;
    }
}
