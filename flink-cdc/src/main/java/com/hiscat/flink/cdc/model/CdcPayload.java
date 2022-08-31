package com.hiscat.flink.cdc.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class CdcPayload {
  private String key;
  private String value;
  private String db;
  private String table;
  private long ts;
}
