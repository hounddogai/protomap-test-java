package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.ComplianceCase;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class ComplianceCaseRecordMapperImpl implements ComplianceCaseRecordMapper {
    @Override
    public ComplianceCaseRecord toRecord(ComplianceCase value) {
        return new ComplianceCaseRecord(value.getComplianceCaseId(), value.getSubject(), value.toByteArray());
    }

    @Override
    public ComplianceCase toProto(ComplianceCaseRecord value) {
        try {
            return ComplianceCase.parseFrom(value.snapshot()).toBuilder()
                    .setComplianceCaseId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
