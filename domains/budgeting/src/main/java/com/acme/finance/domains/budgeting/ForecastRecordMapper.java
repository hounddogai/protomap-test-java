package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.Forecast;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface ForecastRecordMapper {
    ForecastRecord toRecord(Forecast value);

    Forecast toProto(ForecastRecord value);
}
