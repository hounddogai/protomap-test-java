package com.acme.finance.domains.financeportal;

import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.FinanceWorkspace;

/**
 * The domain's durable record and audit port.
 */
public interface FinanceportalRepository {
    void recordAction(String tenant, String kind, String id, String action);

    void recordDetail(String tenant, String operation, String detail);

    void saveApprovalInbox(String tenant, ApprovalInbox value);

    void saveCloseChecklist(String tenant, CloseChecklist value);

    void saveFinanceWorkspace(String tenant, FinanceWorkspace value);
}
