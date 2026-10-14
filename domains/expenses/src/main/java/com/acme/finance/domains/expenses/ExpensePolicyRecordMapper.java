package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.ExpensePolicy;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface ExpensePolicyRecordMapper {
    ExpensePolicyRecord toRecord(ExpensePolicy value);

    ExpensePolicy toProto(ExpensePolicyRecord value);
}
