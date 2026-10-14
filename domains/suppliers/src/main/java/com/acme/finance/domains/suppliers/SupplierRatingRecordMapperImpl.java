package com.acme.finance.domains.suppliers;

import com.acme.suppliers.v1.SupplierRating;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class SupplierRatingRecordMapperImpl implements SupplierRatingRecordMapper {
    @Override
    public SupplierRatingRecord toRecord(SupplierRating value) {
        return new SupplierRatingRecord(value.getSupplierRatingId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public SupplierRating toProto(SupplierRatingRecord value) {
        try {
            return SupplierRating.parseFrom(value.snapshot()).toBuilder()
                    .setSupplierRatingId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
