package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.Forecast;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class ForecastRecordMapperImpl implements ForecastRecordMapper {
    @Override
    public ForecastRecord toRecord(Forecast value) {
        return new ForecastRecord(value.getForecastId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public Forecast toProto(ForecastRecord value) {
        try {
            return Forecast.parseFrom(value.snapshot()).toBuilder()
                    .setForecastId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
