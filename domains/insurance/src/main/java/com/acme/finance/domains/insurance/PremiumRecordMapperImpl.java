package com.acme.finance.domains.insurance;

import com.acme.insurance.v1.Premium;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class PremiumRecordMapperImpl implements PremiumRecordMapper {
    @Override
    public PremiumRecord toRecord(Premium value) {
        return new PremiumRecord(value.getPremiumId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public Premium toProto(PremiumRecord value) {
        try {
            return Premium.parseFrom(value.snapshot()).toBuilder()
                    .setPremiumId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
