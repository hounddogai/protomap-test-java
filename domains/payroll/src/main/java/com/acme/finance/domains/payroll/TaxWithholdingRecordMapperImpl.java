package com.acme.finance.domains.payroll;

import com.acme.payroll.v1.TaxWithholding;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class TaxWithholdingRecordMapperImpl implements TaxWithholdingRecordMapper {
    @Override
    public TaxWithholdingRecord toRecord(TaxWithholding value) {
        return new TaxWithholdingRecord(value.getTaxWithholdingId(), value.getName(), value.toByteArray());
    }

    @Override
    public TaxWithholding toProto(TaxWithholdingRecord value) {
        try {
            return TaxWithholding.parseFrom(value.snapshot()).toBuilder()
                    .setTaxWithholdingId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
