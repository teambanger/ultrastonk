package org.ultrastonk.eventemitter;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVParserBuilder;
import com.opencsv.ICSVParser;
import com.opencsv.exceptions.CsvException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.function.Consumer;

/**
 * Loads trading records from the DEBS 2022 Grand Challenge CSV data set and extracts them as
 * {@link RawPriceEvent}s.
 *
 * <p>CSV format: comma-separated, 13 leading comment/header lines, 40 columns per data row.
 * Extracted fields (0-based indices):
 * <ul>
 *     <li>0  - ID (e.g. "A2ASZ7.ETR")</li>
 *     <li>1  - SecType ('E'quity or 'I'ndex)</li>
 *     <li>2  - System date of the last update (dd-mm-yyyy), always populated</li>
 *     <li>3  - System time of the last update (HH:mm:ss.SSS)</li>
 *     <li>4  - Ask price</li>
 *     <li>6  - Bid price</li>
 *     <li>10 - Close</li>
 *     <li>21 - Last trade price</li>
 *     <li>23 - Trading time of the last update (HH:mm:ss.SSS)</li>
 *     <li>26 - Trading date of the last update (dd-mm-yyyy), populated in under 1% of the rows</li>
 * </ul>
 *
 * <p><strong>The loader is a transport, not a query engine.</strong> By default
 * ({@link PriceFilter#NONE}) it forwards every row that is structurally readable, including
 * bid/ask ticks and payload-less housekeeping events, so that the stream reaching Kafka is a
 * faithful replay of the data set. Selection of the price attribute and of the subscribed symbol
 * subset is a business rule that belongs to the query layer (course project spec, relaxations 2
 * and 3). The pre-filters below exist only to keep development runs cheap; whichever one is
 * active, every dropped row is attributed to an explicit counter in {@link LoadStats} so that
 * nothing disappears silently.
 *
 * <p>The event timestamp is reconstructed from a trading date + trading time pair
 * (see {@link #parseEventTimestamp(String, String, ZoneId)}). Because the dedicated "Trading date"
 * column (26) is almost always empty in the released data set, the system "Date" column (2) is
 * used as its fallback, and "Time" (3) is the fallback for "Trading time" (23).
 *
 * <p>The data set carries no time zone: the paper documents the timestamps as CEST. The zone the
 * date/time pair is interpreted in is configurable per loader instance and defaults to
 * {@link ZoneOffset#UTC}; any zone works for event-time processing as long as the replay and the
 * downstream job agree on it, but picking {@code Europe/Berlin} makes the absolute instants (and
 * therefore time-of-day window boundaries) match exchange local time.
 */
public class DataLoader {
    /** The CSV file uses commas as field separators. */
    private static final char SEPARATOR = ',';
    /** Comment + header lines to skip at the top of the file. */
    private static final int SKIP_LINES = 13;
    private static final int COLUMN_COUNT = 40;

    private static final int COL_ID = 0;
    private static final int COL_SEC_TYPE = 1;
    private static final int COL_DATE = 2;
    private static final int COL_TIME = 3;
    private static final int COL_ASK = 4;
    private static final int COL_BID = 6;
    private static final int COL_CLOSE = 10;
    private static final int COL_LAST = 21;
    private static final int COL_TRADING_TIME = 23;
    private static final int COL_TRADING_DATE = 26;

    /**
     * Trading/system time in the CSV, e.g. "14:33:02.512". The fraction is parsed with
     * 1 to 9 digits: the released files use milliseconds, the data set documentation
     * mentions {@code HH:MM:SS.ssss}, so do not be strict about it.
     */
    private static final DateTimeFormatter TIME_FMT = new DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .toFormatter(Locale.ROOT).withResolverStyle(ResolverStyle.STRICT);
    /** Trading/system date in the CSV, e.g. "08-11-2021". */
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("dd-MM-uuuu", Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT);

    /**
     * Optional pre-filter applied by the loader. Everything except {@link #NONE} throws away real
     * events, so it must never be the default for a run whose numbers go into the report.
     */
    public enum PriceFilter {
        /** Emit every structurally readable row (default, faithful replay). */
        NONE("none"),
        /** Emit only rows that carry a "Last" value at all, including the meaningless 0.0 ones. */
        LAST("last"),
        /** Emit only rows carrying a non-zero "Last" value, i.e. the actual trade events. */
        LAST_NONZERO("last-nonzero");

        private final String id;

        PriceFilter(String id) {
            this.id = id;
        }

        public String id() {
            return id;
        }

        /** True when the row survives this filter. */
        public boolean accepts(String[] row) {
            return switch (this) {
                case NONE -> true;
                case LAST -> parsePrice(row[COL_LAST]) != null;
                case LAST_NONZERO -> {
                    Double price = parsePrice(row[COL_LAST]);
                    yield price != null && price > 0.0;
                }
            };
        }

        /** Case-insensitive lookup; also accepts {@code raw} as a synonym for {@link #NONE}. */
        public static PriceFilter fromString(String value) {
            String v = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
            if (v.isEmpty() || v.equals("raw")) {
                return NONE;
            }
            for (PriceFilter filter : values()) {
                if (filter.id.equals(v)) {
                    return filter;
                }
            }
            throw new IllegalArgumentException("Unknown price filter '" + value
                    + "', expected one of: none, last, last-nonzero");
        }
    }

    /**
     * Per-file accounting of what the loader read and what it dropped. Every skipped row is
     * attributed to exactly one bucket, so {@code rowsRead == emitted + droppedTotal()}.
     *
     * <p>In the released data set the {@code droppedNoTime} bucket dominates the accounting and is
     * harmless for the queries: those rows carry neither a Trading time nor a system Time nor any
     * price attribute - they hold only ID, SecType, Date, Currency, ISIN and Nominal value, i.e.
     * they are the payload-less events the data-set paper mentions. Measured over the first 3M
     * rows of {@code debs2022-gc-trading-day-08-11-21.csv}: 0.4% of the rows, none of them with a
     * price payload (in the pre-open slice shipped as {@code part.csv} they reach 10%, because
     * metadata refreshes dominate before the session opens). They cannot be placed in a window, so
     * they are dropped and counted rather than invented. {@code droppedWidth} and
     * {@code droppedTimestamp} are single corrupt rows in those samples (a short row, and a row
     * whose Time is "::").
     *
     * @param malformedPrices non-blank price cells rejected during extraction; these prices are
     *                        omitted under {@link PriceFilter#NONE}
     */
    public record LoadStats(long rowsRead,
                            long emitted,
                            long droppedWidth,
                            long droppedNoSymbol,
                            long droppedNoTime,
                            long droppedPrice,
                            long droppedTimestamp,
                            long malformedPrices) {

        /** Rows that never reached the sink. */
        public long droppedTotal() {
            return droppedWidth + droppedNoSymbol + droppedNoTime + droppedPrice + droppedTimestamp;
        }

        /** Neutral element, useful before any file has been read. */
        public static LoadStats empty() {
            return new LoadStats(0, 0, 0, 0, 0, 0, 0, 0);
        }

        public double dropRatePercent() {
            return rowsRead == 0 ? 0.0 : droppedTotal() * 100.0 / rowsRead;
        }

        /** Add another file's statistics to these ones. */
        public LoadStats plus(LoadStats other) {
            return new LoadStats(rowsRead + other.rowsRead, emitted + other.emitted,
                    droppedWidth + other.droppedWidth, droppedNoSymbol + other.droppedNoSymbol,
                    droppedNoTime + other.droppedNoTime, droppedPrice + other.droppedPrice,
                    droppedTimestamp + other.droppedTimestamp, malformedPrices + other.malformedPrices);
        }

        /** One-line, report-friendly summary. */
        public String summary(PriceFilter filter) {
            return String.format(Locale.ROOT,
                    "read %,d -> emitted %,d (%.4f%% dropped: %,d bad width, %,d no symbol, %,d no event time, "
                            + "%,d filtered out by %s, %,d unparsable timestamp; %,d malformed prices nulled)",
                    rowsRead, emitted, dropRatePercent(), droppedWidth, droppedNoSymbol, droppedNoTime,
                    droppedPrice, filter.id(), droppedTimestamp, malformedPrices);
        }
    }

    /** Mutable accumulator used while a file is being streamed. */
    static final class Counters {
        static Counters of() {
            return new Counters();
        }

        long rowsRead;
        long emitted;
        long droppedWidth;
        long droppedNoSymbol;
        long droppedNoTime;
        long droppedPrice;
        long droppedTimestamp;
        long malformedPrices;

        LoadStats toStats() {
            return new LoadStats(rowsRead, emitted, droppedWidth, droppedNoSymbol, droppedNoTime,
                    droppedPrice, droppedTimestamp, malformedPrices);
        }
    }

    private final Path path;
    private final ZoneId eventZone;
    private final PriceFilter priceFilter;

    public DataLoader(Path path) {
        this(path, ZoneOffset.UTC, PriceFilter.NONE);
    }

    /**
     * @param eventZone   zone the CSV date/time strings are interpreted as being in
     *                    (the data set itself is zone-less; it documents CEST)
     * @param priceFilter optional source-side pre-filter; use {@link PriceFilter#NONE} for any
     *                    run whose results or throughput numbers are meant to be authoritative
     */
    public DataLoader(Path path, ZoneId eventZone, PriceFilter priceFilter) {
        this.path = path;
        this.eventZone = eventZone == null ? ZoneOffset.UTC : eventZone;
        this.priceFilter = priceFilter == null ? PriceFilter.NONE : priceFilter;
    }

    public DataLoader(String filename) {
        this(Path.of(filename));
    }

    public PriceFilter priceFilter() {
        return priceFilter;
    }

    /**
     * Streams the file row by row and hands each surviving {@link RawPriceEvent} to the given
     * consumer. Dropped rows are counted in {@code counters} rather than logged.
     *
     * @throws IOException if the file cannot be read
     */
    public void load(Counters counters, Consumer<RawPriceEvent> sink) throws IOException, CsvException {
        ICSVParser parser = new CSVParserBuilder()
                .withSeparator(SEPARATOR)
                .build();
        try (CSVReader csvReader = new CSVReaderBuilder(Files.newBufferedReader(path))
                .withSkipLines(SKIP_LINES)
                .withCSVParser(parser)
                .build()) {
            String[] row;
            while ((row = csvReader.readNext()) != null) {
                counters.rowsRead++;
                if (!structurallyValid(row, counters)) {
                    continue;
                }
                if (!priceFilter.accepts(row)) {
                    counters.droppedPrice++;
                    continue;
                }
                RawPriceEvent event;
                try {
                    event = extract(row, eventZone, counters);
                } catch (RuntimeException e) {
                    // Unparseable event timestamp - skip instead of aborting the replay.
                    counters.droppedTimestamp++;
                    continue;
                }
                counters.emitted++;
                sink.accept(event);
            }
        }
    }

    /** Convenience overload for callers that do not care about the accounting. */
    public void load(Consumer<RawPriceEvent> sink) throws IOException, CsvException {
        load(Counters.of(), sink);
    }

    /**
     * Maps one CSV row to a {@link RawPriceEvent}. The emit timestamp is left at {@code 0}; the
     * emitter fills it in via {@link RawPriceEvent#withEmitTimestamp(long)} right before sending.
     * Price columns that are blank or unparsable become {@code null}.
     */
    public static RawPriceEvent extract(String[] row) {
        return extract(row, ZoneOffset.UTC);
    }

    /** As {@link #extract(String[])}, interpreting the CSV date/time in the given zone. */
    public static RawPriceEvent extract(String[] row, ZoneId zone) {
        return extract(row, zone, null);
    }

    private static RawPriceEvent extract(String[] row, ZoneId zone, Counters counters) {
        String tradingDate = firstNonBlank(row[COL_TRADING_DATE], row[COL_DATE]);
        String tradingTime = firstNonBlank(row[COL_TRADING_TIME], row[COL_TIME]);
        long eventTimestamp;
        try {
            eventTimestamp = parseEventTimestamp(tradingDate, tradingTime, zone);
        } catch (DateTimeParseException e) {
            tradingDate = firstNonBlank(row[COL_DATE]);
            eventTimestamp = parseEventTimestamp(tradingDate, firstNonBlank(row[COL_TIME]), zone);
        }
        return new RawPriceEvent(
                row[COL_ID].trim(),                          // ID
                row[COL_SEC_TYPE].trim().charAt(0),          // SecType
                parsePrice(row[COL_LAST], counters),         // last trade price
                parsePrice(row[COL_BID], counters),          // best bid
                parsePrice(row[COL_ASK], counters),          // best ask
                parsePrice(row[COL_CLOSE], counters),        // close
                eventTimestamp,                             // event time, epoch millis
                0L,                                          // emit time, filled in by the emitter
                tradingDate                                  // trading date, dd-mm-yyyy
        );
    }

    /**
     * Converts a date (dd-mm-yyyy) plus time (HH:mm:ss[.SSS]) pair into epoch milliseconds,
     * interpreted in {@link ZoneOffset#UTC}.
     *
     * @throws java.time.format.DateTimeParseException if either value is malformed
     */
    public static long parseEventTimestamp(String dateStr, String timeStr) {
        return parseEventTimestamp(dateStr, timeStr, ZoneOffset.UTC);
    }

    /**
     * Converts a date (dd-mm-yyyy) plus time (HH:mm:ss[.SSS]) pair into epoch milliseconds. The
     * CSV carries no time zone information, so the caller decides which zone the wall-clock values
     * refer to (the data set documents CEST).
     *
     * @throws java.time.format.DateTimeParseException if either value is malformed
     */
    public static long parseEventTimestamp(String dateStr, String timeStr, ZoneId zone) {
        LocalDate date = LocalDate.parse(dateStr, DATE_FMT);
        LocalTime time = LocalTime.parse(timeStr, TIME_FMT);
        return LocalDateTime.of(date, time).atZone(zone == null ? ZoneOffset.UTC : zone).toInstant().toEpochMilli();
    }

    /**
     * Transport-level validity only: the row must have the right shape and be placeable in time.
     * Price attributes are deliberately <em>not</em> checked here - a row without any of them is a
     * payload-less housekeeping event, which is still part of the stream the spec asks us to replay.
     */
    private static boolean structurallyValid(String[] row, Counters counters) {
        if (row == null || row.length != COLUMN_COUNT) {
            counters.droppedWidth++;
            return false;
        }
        if (isBlank(row, COL_ID) || isBlank(row, COL_SEC_TYPE)) {
            counters.droppedNoSymbol++;
            return false;
        }
        if (!hasEventTime(row)) {
            counters.droppedNoTime++;
            return false;
        }
        return true;
    }

    private static boolean isBlank(String[] row, int column) {
        return row[column] == null || row[column].isBlank();
    }

    /** True when at least one date column and one time column carry a value. */
    private static boolean hasEventTime(String[] row) {
        return !firstNonBlank(row[COL_TRADING_DATE], row[COL_DATE]).isEmpty()
                && !firstNonBlank(row[COL_TRADING_TIME], row[COL_TIME]).isEmpty();
    }

    /** Parses a price cell, returning {@code null} for blank or unparsable values. */
    private static Double parsePrice(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            double price = Double.parseDouble(raw.trim());
            return Double.isFinite(price) && price >= 0 ? price : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** As {@link #parsePrice(String)}, counting cells that were present but unparsable. */
    private static Double parsePrice(String raw, Counters counters) {
        Double value = parsePrice(raw);
        if (value == null && counters != null && raw != null && !raw.isBlank()) {
            counters.malformedPrices++;
        }
        return value;
    }

    /** Returns the first non-blank, trimmed value, or "" if none is usable. */
    private static String firstNonBlank(String... candidates) {
        for (String candidate : candidates) {
            if (candidate != null && !candidate.isBlank()) {
                return candidate.trim();
            }
        }
        return "";
    }
}
