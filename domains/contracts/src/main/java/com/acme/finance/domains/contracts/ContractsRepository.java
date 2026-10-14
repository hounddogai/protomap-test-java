package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.Renewal;

/**
 * The domain's durable record and audit port.
 */
public interface ContractsRepository {
    void recordAction(String tenant, String kind, String id, String action);

    void recordDetail(String tenant, String operation, String detail);

    void saveContract(String tenant, Contract value);

    void saveContractClause(String tenant, ContractClause value);

    void saveObligation(String tenant, Obligation value);

    void saveRenewal(String tenant, Renewal value);
}
