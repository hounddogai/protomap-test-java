package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.ExpenseReport;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface ExpenseReportRecordMapper {
    ExpenseReportRecord toRecord(ExpenseReport value);

    ExpenseReport toProto(ExpenseReportRecord value);
}
