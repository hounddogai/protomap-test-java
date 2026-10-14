package com.acme.finance.domains.insurance;

import com.acme.insurance.v1.Claim;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class ClaimRecordMapperImpl implements ClaimRecordMapper {
    @Override
    public ClaimRecord toRecord(Claim value) {
        return new ClaimRecord(value.getClaimId(), value.getSubject(), value.toByteArray());
    }

    @Override
    public Claim toProto(ClaimRecord value) {
        try {
            return Claim.parseFrom(value.snapshot()).toBuilder()
                    .setClaimId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
