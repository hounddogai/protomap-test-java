package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.ChartOfAccounts;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface ChartOfAccountsRecordMapper {
    ChartOfAccountsRecord toRecord(ChartOfAccounts value);

    ChartOfAccounts toProto(ChartOfAccountsRecord value);
}
