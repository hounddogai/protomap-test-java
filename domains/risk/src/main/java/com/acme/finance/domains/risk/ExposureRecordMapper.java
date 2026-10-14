package com.acme.finance.domains.risk;

import com.acme.risk.v1.Exposure;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface ExposureRecordMapper {
    ExposureRecord toRecord(Exposure value);

    Exposure toProto(ExposureRecord value);
}
