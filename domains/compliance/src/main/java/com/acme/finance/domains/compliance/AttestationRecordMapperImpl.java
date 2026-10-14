package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.Attestation;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class AttestationRecordMapperImpl implements AttestationRecordMapper {
    @Override
    public AttestationRecord toRecord(Attestation value) {
        return new AttestationRecord(value.getAttestationId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public Attestation toProto(AttestationRecord value) {
        try {
            return Attestation.parseFrom(value.snapshot()).toBuilder()
                    .setAttestationId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
