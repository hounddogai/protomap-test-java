package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.ExpenseReport;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.stereotype.Component;

@Component
public final class ExpenseReportRecordMapperImpl implements ExpenseReportRecordMapper {
    @Override
    public ExpenseReportRecord toRecord(ExpenseReport value) {
        return new ExpenseReportRecord(value.getExpenseReportId(), value.getNumber(), value.toByteArray());
    }

    @Override
    public ExpenseReport toProto(ExpenseReportRecord value) {
        try {
            return ExpenseReport.parseFrom(value.snapshot()).toBuilder()
                    .setExpenseReportId(value.id()).build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid persisted snapshot", failure);
        }
    }
}
