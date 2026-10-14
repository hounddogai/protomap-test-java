package com.acme.finance.domains.suppliers;

import com.acme.suppliers.v1.SupplierContract;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class SupplierContractRecordMapperImpl implements SupplierContractRecordMapper {
    @Override
    public SupplierContractRecord toRecord(SupplierContract value) {
        return new SupplierContractRecord(value.getSupplierContractId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public SupplierContract toProto(SupplierContractRecord value) {
        try {
            return SupplierContract.parseFrom(value.snapshot()).toBuilder()
                    .setSupplierContractId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
