package com.acme.finance.domains.banking;

import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.Statement;

/**
 * The domain's durable record and audit port.
 */
public interface BankingRepository {
    void recordAction(String tenant, String kind, String id, String action);

    void recordDetail(String tenant, String operation, String detail);

    void saveBankAccount(String tenant, BankAccount value);

    void saveMandate(String tenant, Mandate value);

    void saveStandingOrder(String tenant, StandingOrder value);

    void saveStatement(String tenant, Statement value);
}
