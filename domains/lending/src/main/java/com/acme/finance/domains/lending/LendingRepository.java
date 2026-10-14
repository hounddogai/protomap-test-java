package com.acme.finance.domains.lending;

import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.Repayment;

/**
 * The domain's durable record and audit port.
 */
public interface LendingRepository {
    void recordAction(String tenant, String kind, String id, String action);

    void recordDetail(String tenant, String operation, String detail);

    void saveCollateral(String tenant, Collateral value);

    void saveLoan(String tenant, Loan value);

    void saveLoanApplication(String tenant, LoanApplication value);

    void saveRepayment(String tenant, Repayment value);
}
