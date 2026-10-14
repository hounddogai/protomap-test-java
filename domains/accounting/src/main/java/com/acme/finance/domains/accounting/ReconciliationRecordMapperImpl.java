package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Reconciliation;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class ReconciliationRecordMapperImpl implements ReconciliationRecordMapper {
    @Override
    public ReconciliationRecord toRecord(Reconciliation value) {
        return new ReconciliationRecord(value.getReconciliationId(), value.getSubject(), value.toByteArray());
    }

    @Override
    public Reconciliation toProto(ReconciliationRecord value) {
        try {
            return Reconciliation.parseFrom(value.snapshot()).toBuilder()
                    .setReconciliationId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
