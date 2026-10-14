package com.acme.finance.domains.contracts;

import com.acme.collections.v1.ExportPaymentPromisesRequest;
import com.acme.collections.v1.ExportPaymentPromisesResponse;
import com.acme.collections.v1.PaymentPromiseQueryServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseStatusChange;
import com.acme.compliance.v1.RecalculateComplianceCasesRequest;
import com.acme.compliance.v1.RecalculateComplianceCasesResponse;
import com.acme.contracts.v1.BatchCreateContractsRequest;
import com.acme.contracts.v1.BatchCreateContractsResponse;
import com.acme.contracts.v1.BatchGetContractsRequest;
import com.acme.contracts.v1.BatchGetContractsResponse;
import com.acme.contracts.v1.CloneContractRequest;
import com.acme.contracts.v1.CloneContractResponse;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractClauseQueryServiceGrpc;
import com.acme.contracts.v1.ContractQueryServiceGrpc;
import com.acme.contracts.v1.CountContractsRequest;
import com.acme.contracts.v1.CountContractsResponse;
import com.acme.contracts.v1.CreateContractRequest;
import com.acme.contracts.v1.CreateContractResponse;
import com.acme.contracts.v1.ExplainObligationCalculationRequest;
import com.acme.contracts.v1.ExplainObligationCalculationResponse;
import com.acme.contracts.v1.GetContractRequest;
import com.acme.contracts.v1.GetContractResponse;
import com.acme.contracts.v1.LintObligationsRequest;
import com.acme.contracts.v1.LintObligationsResponse;
import com.acme.contracts.v1.ListContractsRequest;
import com.acme.contracts.v1.ListContractsResponse;
import com.acme.contracts.v1.LookupContractRequest;
import com.acme.contracts.v1.LookupContractResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationCalculationServiceGrpc;
import com.acme.contracts.v1.ObligationSla;
import com.acme.contracts.v1.ObligationValidationServiceGrpc;
import com.acme.contracts.v1.PreviewRenewalRequest;
import com.acme.contracts.v1.PreviewRenewalResponse;
import com.acme.contracts.v1.RenewalRef;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.contracts.v1.ResolveContractClausesRequest;
import com.acme.contracts.v1.ResolveContractClausesResponse;
import com.acme.contracts.v1.ResolveContractsRequest;
import com.acme.contracts.v1.ResolveContractsResponse;
import com.acme.contracts.v1.SearchContractsRequest;
import com.acme.contracts.v1.SearchContractsResponse;
import com.acme.contracts.v1.SimulateObligationRequest;
import com.acme.contracts.v1.SimulateObligationResponse;
import com.acme.contracts.v1.SummarizeContractsRequest;
import com.acme.contracts.v1.SummarizeContractsResponse;
import com.acme.contracts.v1.UpdateContractRequest;
import com.acme.contracts.v1.UpdateContractResponse;
import com.acme.contracts.v1.ValidateObligationRequest;
import com.acme.contracts.v1.ValidateObligationResponse;
import com.acme.contracts.v1.WatchContractsRequest;
import com.acme.contracts.v1.WatchContractsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.BatchGetFeatureFlagsRequest;
import com.acme.featureflags.v1.BatchGetFeatureFlagsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionRequest;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountBankFunding;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanValidationServiceGrpc;
import com.acme.lending.v1.NormalizeLoanRequest;
import com.acme.lending.v1.NormalizeLoanResponse;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.WatchMetricSeriesesRequest;
import com.acme.metrics.v1.WatchMetricSeriesesResponse;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class ContractRequestMapperImpl implements ContractRequestMapper {
    @Override
    public PreviewRenewalRequest getContractStage0(GetContractRequest request) {
        return PreviewRenewalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewRenewalRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
    }

    @Override
    public ResolveContractClausesRequest cloneContractStage0(CloneContractRequest request) {
        return ResolveContractClausesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
    }
}
