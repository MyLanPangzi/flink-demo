package com.hiscat.flink.function;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class CallContext {
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;
    private ParameterTool args;
}
