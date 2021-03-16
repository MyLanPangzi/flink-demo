
package com.hiscat.flink.custrom.connector.json;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/**
 * Time formats and timestamp formats respecting the RFC3339 specification, ISO-8601 specification
 * and SQL specification.
 */
class TimeFormats {

    /** Formatter for RFC 3339-compliant string representation of a time value. */
    static final DateTimeFormatter RFC3339_TIME_FORMAT =
            new DateTimeFormatterBuilder()
                    .appendPattern("HH:mm:ss")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .appendPattern("'Z'")
                    .toFormatter();

    /**
     * Formatter for RFC 3339-compliant string representation of a timestamp value (with UTC
     * timezone).
     */
    static final DateTimeFormatter RFC3339_TIMESTAMP_FORMAT =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral('T')
                    .append(RFC3339_TIME_FORMAT)
                    .toFormatter();

    /** Formatter for ISO8601 string representation of a timestamp value (without UTC timezone). */
    static final DateTimeFormatter ISO8601_TIMESTAMP_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    /** Formatter for ISO8601 string representation of a timestamp value (with UTC timezone). */
    static final DateTimeFormatter ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral('T')
                    .append(DateTimeFormatter.ISO_LOCAL_TIME)
                    .appendPattern("'Z'")
                    .toFormatter();

    /** Formatter for SQL string representation of a time value. */
    static final DateTimeFormatter SQL_TIME_FORMAT =
            new DateTimeFormatterBuilder()
                    .appendPattern("HH:mm:ss")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .toFormatter();

    /** Formatter for SQL string representation of a timestamp value (without UTC timezone). */
    static final DateTimeFormatter SQL_TIMESTAMP_FORMAT =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .append(SQL_TIME_FORMAT)
                    .toFormatter();

    /** Formatter for SQL string representation of a timestamp value (with UTC timezone). */
    static final DateTimeFormatter SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .append(SQL_TIME_FORMAT)
                    .appendPattern("'Z'")
                    .toFormatter();

    private TimeFormats() {}
}
