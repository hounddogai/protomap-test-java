package com.acme.finance.domains.procurement;

import com.acme.procurement.v1.PurchaseOrder;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class PurchaseOrderRecordMapperImpl implements PurchaseOrderRecordMapper {
    @Override
    public PurchaseOrderRecord toRecord(PurchaseOrder value) {
        return new PurchaseOrderRecord(value.getPurchaseOrderId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public PurchaseOrder toProto(PurchaseOrderRecord value) {
        try {
            return PurchaseOrder.parseFrom(value.snapshot()).toBuilder()
                    .setPurchaseOrderId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
