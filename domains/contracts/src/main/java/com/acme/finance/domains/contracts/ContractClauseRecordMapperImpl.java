package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.ContractClause;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class ContractClauseRecordMapperImpl implements ContractClauseRecordMapper {
    @Override
    public ContractClauseRecord toRecord(ContractClause value) {
        return new ContractClauseRecord(value.getContractClauseId(), value.getName(), value.toByteArray());
    }

    @Override
    public ContractClause toProto(ContractClauseRecord value) {
        try {
            return ContractClause.parseFrom(value.snapshot()).toBuilder()
                    .setContractClauseId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
