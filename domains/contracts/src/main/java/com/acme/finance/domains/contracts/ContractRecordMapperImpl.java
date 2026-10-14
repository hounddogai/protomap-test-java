package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.Contract;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class ContractRecordMapperImpl implements ContractRecordMapper {
    @Override
    public ContractRecord toRecord(Contract value) {
        return new ContractRecord(value.getContractId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public Contract toProto(ContractRecord value) {
        try {
            return Contract.parseFrom(value.snapshot()).toBuilder()
                    .setContractId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
