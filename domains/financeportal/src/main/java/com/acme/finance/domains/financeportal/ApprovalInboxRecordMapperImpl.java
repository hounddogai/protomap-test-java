package com.acme.finance.domains.financeportal;

import com.acme.financeportal.v1.ApprovalInbox;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class ApprovalInboxRecordMapperImpl implements ApprovalInboxRecordMapper {
    @Override
    public ApprovalInboxRecord toRecord(ApprovalInbox value) {
        return new ApprovalInboxRecord(value.getApprovalInboxId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public ApprovalInbox toProto(ApprovalInboxRecord value) {
        try {
            return ApprovalInbox.parseFrom(value.snapshot()).toBuilder()
                    .setApprovalInboxId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
