package com.acme.finance.domains.suppliers;

import com.acme.suppliers.v1.SupplierRisk;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class SupplierRiskRecordMapperImpl implements SupplierRiskRecordMapper {
    @Override
    public SupplierRiskRecord toRecord(SupplierRisk value) {
        return new SupplierRiskRecord(value.getSupplierRiskId(), value.getSubject(), value.toByteArray());
    }

    @Override
    public SupplierRisk toProto(SupplierRiskRecord value) {
        try {
            return SupplierRisk.parseFrom(value.snapshot()).toBuilder()
                    .setSupplierRiskId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
