package com.hiscat.flink.custrom.format.json.ogg;

import com.hiscat.flink.custrom.format.json.JsonOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

/** Option utils for canal-json format. */
public class OggJsonOptions {

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = JsonOptions.IGNORE_PARSE_ERRORS;

    public static final ConfigOption<String> TIMESTAMP_FORMAT = JsonOptions.TIMESTAMP_FORMAT;

    public static final ConfigOption<String> JSON_MAP_NULL_KEY_MODE = JsonOptions.MAP_NULL_KEY_MODE;

    public static final ConfigOption<String> JSON_MAP_NULL_KEY_LITERAL =
            JsonOptions.MAP_NULL_KEY_LITERAL;

    public static final ConfigOption<String> TABLE_INCLUDE =
            ConfigOptions.key("table.include")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "An optional regular expression to only read the specific tables changelog rows by regular matching the \"table\" meta field in the Canal record."
                                    + "The pattern string is compatible with Java's Pattern.");


    public static final ConfigOption<String> OP_TYPE_INCLUDE =
            ConfigOptions.key("op-type.include")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("dml operation type values is I(INSERT),U(UPDATE),D(DELETE),T(TRUNCATE)");

    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

    /** Validator for canal decoding format. */
    public static void validateDecodingFormatOptions(ReadableConfig tableOptions) {
        JsonOptions.validateDecodingFormatOptions(tableOptions);
    }

    /** Validator for canal encoding format. */
    public static void validateEncodingFormatOptions(ReadableConfig tableOptions) {
        JsonOptions.validateEncodingFormatOptions(tableOptions);
    }
}
