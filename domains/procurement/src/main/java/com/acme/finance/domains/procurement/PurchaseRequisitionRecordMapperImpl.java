package com.acme.finance.domains.procurement;

import com.acme.procurement.v1.PurchaseRequisition;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class PurchaseRequisitionRecordMapperImpl implements PurchaseRequisitionRecordMapper {
    @Override
    public PurchaseRequisitionRecord toRecord(PurchaseRequisition value) {
        return new PurchaseRequisitionRecord(value.getPurchaseRequisitionId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public PurchaseRequisition toProto(PurchaseRequisitionRecord value) {
        try {
            return PurchaseRequisition.parseFrom(value.snapshot()).toBuilder()
                    .setPurchaseRequisitionId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
