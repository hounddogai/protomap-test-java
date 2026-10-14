package com.acme.finance.domains.suppliers;

import com.acme.suppliers.v1.Supplier;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class SupplierRecordMapperImpl implements SupplierRecordMapper {
    @Override
    public SupplierRecord toRecord(Supplier value) {
        return new SupplierRecord(value.getSupplierId(), value.getTaxNumber(), value.toByteArray());
    }

    @Override
    public Supplier toProto(SupplierRecord value) {
        try {
            return Supplier.parseFrom(value.snapshot()).toBuilder()
                    .setSupplierId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
