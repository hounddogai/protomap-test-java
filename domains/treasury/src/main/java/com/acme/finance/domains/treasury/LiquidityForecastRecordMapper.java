package com.acme.finance.domains.treasury;

import com.acme.treasury.v1.LiquidityForecast;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface LiquidityForecastRecordMapper {
    LiquidityForecastRecord toRecord(LiquidityForecast value);

    LiquidityForecast toProto(LiquidityForecastRecord value);
}
