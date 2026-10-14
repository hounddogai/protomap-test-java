package com.acme.finance.domains.financeportal;

import com.acme.financeportal.v1.FinanceWorkspace;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface FinanceWorkspaceRecordMapper {
    FinanceWorkspaceRecord toRecord(FinanceWorkspace value);

    FinanceWorkspace toProto(FinanceWorkspaceRecord value);
}
