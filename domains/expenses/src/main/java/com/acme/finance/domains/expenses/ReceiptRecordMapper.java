package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.Receipt;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface ReceiptRecordMapper {
    ReceiptRecord toRecord(Receipt value);

    Receipt toProto(ReceiptRecord value);
}
