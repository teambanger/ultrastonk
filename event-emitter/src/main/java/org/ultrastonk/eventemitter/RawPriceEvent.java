package org.ultrastonk.eventemitter;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * A single raw tick from the DEBS 2022 Grand Challenge data set.
 *
 * <p>The emitter deliberately forwards the row <em>as it appears in the CSV</em>: the data set
 * does not carry an explicit event type, the type is implied by which price attribute is
 * non-NULL (course project spec, section 2.2). {@code last}, {@code bid}, {@code ask} and
 * {@code close} are therefore boxed and nullable, and deciding which of them counts as the
 * "price event" of a window is left to the query layer instead of being baked into the transport.
 *
 * <p>{@code eventTimestampMs} is the original exchange event time (Trading date + Trading time
 * from the CSV) and is what downstream consumers should use for event-time processing /
 * watermarking. {@code emitTimestampMs} is the wall-clock time at which the event was handed to
 * the Kafka producer and is only used to measure emitter-to-consumer latency; it is {@code 0}
 * until the event is emitted (see {@link #withEmitTimestamp(long)}).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record RawPriceEvent(
        String id,             // FR, NL, or ETR
        char secType,          // Security type, [I]ndex or [E]quity
        Double last,           // Last trade price, null if absent
        Double bid,            // Price of best bid order, null if absent
        Double ask,            // Price of best ask order, null if absent
        Double close,          // Closing price, null if absent
        long eventTimestampMs, // Original event time, epoch millis (for Flink watermarks)
        long emitTimestampMs,  // Wall-clock time when emitted to Kafka, epoch millis (latency tracking)
        String tradingDate     // Date of last trade, dd-mm-yyyy
) {

    /** Copy of this event with the wall-clock emission timestamp filled in. */
    public RawPriceEvent withEmitTimestamp(long emitTimestampMs) {
        return new RawPriceEvent(id, secType, last, bid, ask, close,
                eventTimestampMs, emitTimestampMs, tradingDate);
    }

    /** Kafka record key; events of the same instrument stay on the same partition. */
    public String key() {
        return id;
    }

    /**
     * True when the row carries a usable trade price. Note that the data set contains rows with
     * an explicit {@code Last} of {@code 0.0}, which are not trades and are filtered out by
     * {@link DataLoader.PriceFilter#LAST_NONZERO}.
     */
    @JsonIgnore
    public boolean isTrade() {
        return last != null && last > 0.0 && Double.isFinite(last);
    }

    /**
     * Human-readable event type derived from the non-NULL price attributes, following the
     * "event types are identified by the respective non-NULL attributes" rule of the spec.
     * Only useful for logging and sanity checks; consumers should test the fields themselves.
     */
    public String kind() {
        if (last != null) {
            return "last";
        }
        if (bid != null && ask != null) {
            return "bid+ask";
        }
        if (bid != null) {
            return "bid";
        }
        if (ask != null) {
            return "ask";
        }
        if (close != null) {
            return "close";
        }
        return "none";
    }

}
