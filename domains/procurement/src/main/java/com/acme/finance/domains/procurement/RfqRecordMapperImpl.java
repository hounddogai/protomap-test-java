package com.acme.finance.domains.procurement;

import com.acme.procurement.v1.Rfq;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class RfqRecordMapperImpl implements RfqRecordMapper {
    @Override
    public RfqRecord toRecord(Rfq value) {
        return new RfqRecord(value.getRfqId(), value.getSubject(), value.toByteArray());
    }

    @Override
    public Rfq toProto(RfqRecord value) {
        try {
            return Rfq.parseFrom(value.snapshot()).toBuilder()
                    .setRfqId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
