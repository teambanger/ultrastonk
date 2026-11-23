package fi.ultrastonk;

/** A decoded Kafka value. Invalid values are audited but never advance partition event time. */
public class PriceInput {
    public long eventTimeMs;
    public PriceEvent trade;
    public String rejection;

    public PriceInput() {}

    PriceInput(long eventTimeMs, PriceEvent trade, String rejection) {
        this.eventTimeMs = eventTimeMs;
        this.trade = trade;
        this.rejection = rejection;
    }
}
