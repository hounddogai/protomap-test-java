package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.Budget;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface BudgetRecordMapper {
    BudgetRecord toRecord(Budget value);

    Budget toProto(BudgetRecord value);
}
