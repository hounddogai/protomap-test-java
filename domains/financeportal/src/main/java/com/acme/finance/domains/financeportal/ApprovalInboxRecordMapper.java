package com.acme.finance.domains.financeportal;

import com.acme.financeportal.v1.ApprovalInbox;

/**
 * Maps between the protobuf contract and the domain's persistence record.
 */
public interface ApprovalInboxRecordMapper {
    ApprovalInboxRecord toRecord(ApprovalInbox value);

    ApprovalInbox toProto(ApprovalInboxRecord value);
}
