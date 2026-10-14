package com.acme.finance.domains.procurement;

import com.acme.procurement.v1.GoodsReceipt;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class GoodsReceiptRecordMapperImpl implements GoodsReceiptRecordMapper {
    @Override
    public GoodsReceiptRecord toRecord(GoodsReceipt value) {
        return new GoodsReceiptRecord(value.getGoodsReceiptId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public GoodsReceipt toProto(GoodsReceiptRecord value) {
        try {
            return GoodsReceipt.parseFrom(value.snapshot()).toBuilder()
                    .setGoodsReceiptId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
