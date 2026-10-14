package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.BudgetLine;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface BudgetLineRecordMapper {
    BudgetLineRecord toRecord(BudgetLine value);

    BudgetLine toProto(BudgetLineRecord value);
}
