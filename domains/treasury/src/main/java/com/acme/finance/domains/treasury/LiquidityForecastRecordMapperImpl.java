package com.acme.finance.domains.treasury;

import com.acme.treasury.v1.LiquidityForecast;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class LiquidityForecastRecordMapperImpl implements LiquidityForecastRecordMapper {
    @Override
    public LiquidityForecastRecord toRecord(LiquidityForecast value) {
        return new LiquidityForecastRecord(value.getLiquidityForecastId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public LiquidityForecast toProto(LiquidityForecastRecord value) {
        try {
            return LiquidityForecast.parseFrom(value.snapshot()).toBuilder()
                    .setLiquidityForecastId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
