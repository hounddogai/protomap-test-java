package com.acme.finance.domains.financeportal;

import com.acme.financeportal.v1.FinanceWorkspace;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class FinanceWorkspaceRecordMapperImpl implements FinanceWorkspaceRecordMapper {
    @Override
    public FinanceWorkspaceRecord toRecord(FinanceWorkspace value) {
        return new FinanceWorkspaceRecord(value.getFinanceWorkspaceId(), value.getSubject(), value.toByteArray());
    }

    @Override
    public FinanceWorkspace toProto(FinanceWorkspaceRecord value) {
        try {
            return FinanceWorkspace.parseFrom(value.snapshot()).toBuilder()
                    .setFinanceWorkspaceId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
