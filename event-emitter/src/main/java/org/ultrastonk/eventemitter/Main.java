package org.ultrastonk.eventemitter;

import java.nio.file.Path;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Entry point.
 *
 * <p>Without arguments it runs a quick sanity check on the CSV loader: how many rows the file
 * holds, how many survive as events, why the rest were dropped, and how the surviving events
 * split by the event type implied by their non-NULL price attributes. With {@code --emit} it hands
 * over to {@link EventEmitter}, which replays the full data set into Kafka (configured through
 * environment variables).
 *
 * <p>{@code PRICE_FILTER} and {@code EVENT_TIME_ZONE} are honoured by both modes.
 */
public class Main {
    private static final String EMIT_FLAG = "--emit";

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && EMIT_FLAG.equals(args[0])) {
            EventEmitter.main(args);
            return;
        }

        final ObjectMapper objectMapper = new ObjectMapper();
        var config = EventEmitter.Config.fromEnv();
        System.out.println("Loading CSV file...");

        // Stream instead of collecting: a full day file has ~54M rows.
        // Use the same zone and filter as the emitter so the printed numbers match real replays.
        ZoneId eventZone = config.eventZone();
        DataLoader.PriceFilter priceFilter = config.priceFilter();

        DataLoader.Counters counters = DataLoader.Counters.of();
        Map<String, Long> kinds = new HashMap<>();
        Set<String> symbols = new HashSet<>();
        Set<String> tradeSymbols = new HashSet<>();
        long[] trades = new long[1];
        RawPriceEvent[] first = new RawPriceEvent[1];

        for (Path path : config.csvPaths()) {
            new DataLoader(path, eventZone, priceFilter).load(counters, event -> {
                kinds.merge(event.kind(), 1L, Long::sum);
                symbols.add(event.id());
                if (event.isTrade()) {
                    trades[0]++;
                    tradeSymbols.add(event.id());
                }
                if (first[0] == null) first[0] = event;
            });
        }

        DataLoader.LoadStats stats = counters.toStats();
        System.out.printf("Read %,d rows, emitted %,d events in %s zone%n",
                stats.rowsRead(), stats.emitted(), eventZone);
        System.out.println("Accounting: " + stats.summary(priceFilter));
        // The gap between these two lines is how much of the symbol universe a source-side
        // PriceFilter.LAST_NONZERO run would make invisible to the query layer.
        System.out.printf("Distinct symbols: %,d, of which %,d carry at least one trade%n",
                symbols.size(), tradeSymbols.size());
        System.out.printf("Trade events (non-zero Last): %,d%n", trades[0]);
        System.out.println("Event kinds by non-NULL price attribute: " + kinds);

        if (first[0] != null) {
            System.out.println("First event: " + first[0]);
            System.out.println("As JSON:     " + objectMapper.writeValueAsString(first[0]));
        }
    }
}
