package com.acme.finance.domains.contracts;

import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.ExportAccessPoliciesRequest;
import com.acme.authorization.v1.ExportAccessPoliciesResponse;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.BatchCreateBankAccountsRequest;
import com.acme.banking.v1.BatchCreateBankAccountsResponse;
import com.acme.banking.v1.EscalateMandateRequest;
import com.acme.banking.v1.EscalateMandateResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.budgeting.v1.BudgetLineRef;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.ReconcileForecastsRequest;
import com.acme.budgeting.v1.ReconcileForecastsResponse;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.SyncVariancesRequest;
import com.acme.budgeting.v1.SyncVariancesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.collections.v1.StartCollectionCaseWorkflowRequest;
import com.acme.collections.v1.StartCollectionCaseWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.AdvanceContractWorkflowRequest;
import com.acme.contracts.v1.AdvanceContractWorkflowResponse;
import com.acme.contracts.v1.CancelContractWorkflowRequest;
import com.acme.contracts.v1.CancelContractWorkflowResponse;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractApprovalServiceGrpc;
import com.acme.contracts.v1.ContractLine;
import com.acme.contracts.v1.ContractServiceGrpc;
import com.acme.contracts.v1.CreateContractRequest;
import com.acme.contracts.v1.CreateContractResponse;
import com.acme.contracts.v1.InspectContractWorkflowRequest;
import com.acme.contracts.v1.InspectContractWorkflowResponse;
import com.acme.contracts.v1.ListContractWorkflowsRequest;
import com.acme.contracts.v1.ListContractWorkflowsResponse;
import com.acme.contracts.v1.ListPendingContractsRequest;
import com.acme.contracts.v1.ListPendingContractsResponse;
import com.acme.contracts.v1.NormalizeRenewalRequest;
import com.acme.contracts.v1.NormalizeRenewalResponse;
import com.acme.contracts.v1.PauseContractWorkflowRequest;
import com.acme.contracts.v1.PauseContractWorkflowResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalRef;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.contracts.v1.ResumeContractWorkflowRequest;
import com.acme.contracts.v1.ResumeContractWorkflowResponse;
import com.acme.contracts.v1.RetryContractWorkflowRequest;
import com.acme.contracts.v1.RetryContractWorkflowResponse;
import com.acme.contracts.v1.SignalContractWorkflowRequest;
import com.acme.contracts.v1.SignalContractWorkflowResponse;
import com.acme.contracts.v1.StartContractWorkflowRequest;
import com.acme.contracts.v1.StartContractWorkflowResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.EstimatePreferenceRequest;
import com.acme.customers.v1.EstimatePreferenceResponse;
import com.acme.customers.v1.ExplainPreferenceDecisionRequest;
import com.acme.customers.v1.ExplainPreferenceDecisionResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.SearchCustomersRequest;
import com.acme.customers.v1.SearchCustomersResponse;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.CommentLoanApprovalRequest;
import com.acme.lending.v1.CommentLoanApprovalResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.lending.v1.LoanCardFunding;
import com.acme.risk.v1.EscalateRiskScenarioRequest;
import com.acme.risk.v1.EscalateRiskScenarioResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.search.v1.PreviewSearchIndexRequest;
import com.acme.search.v1.PreviewSearchIndexResponse;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.venues.v1.CountVenuesRequest;
import com.acme.venues.v1.CountVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a contract from start to finish. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ContractWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(ContractWorkflowApplication.class);

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final BankAccountServiceGrpc.BankAccountServiceStub bankingBankAccountServiceAsync;

    private final MandateApprovalServiceGrpc.MandateApprovalServiceBlockingStub bankingMandateApprovalServiceBlocking;

    private final ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking;

    private final VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync;

    private final VarianceSyncServiceGrpc.VarianceSyncServiceBlockingStub budgetingVarianceSyncServiceBlocking;

    private final CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceBlockingStub collectionsCollectionCaseWorkflowServiceBlocking;

    private final ContractApprovalServiceGrpc.ContractApprovalServiceBlockingStub contractsContractApprovalServiceBlocking;

    private final ContractServiceGrpc.ContractServiceBlockingStub contractsContractServiceBlocking;

    private final RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking;

    private final CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking;

    private final PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking;

    private final PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking;

    private final LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking;

    private final ContractWorkflowRequestMapper mapper;

    private final ContractsRepository repository;

    private final RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking;

    private final Executor rpcExecutor;

    private final SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking;

    private final VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking;

    public ContractWorkflowApplication(
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            BankAccountServiceGrpc.BankAccountServiceStub bankingBankAccountServiceAsync,
            MandateApprovalServiceGrpc.MandateApprovalServiceBlockingStub bankingMandateApprovalServiceBlocking,
            ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking,
            VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync,
            VarianceSyncServiceGrpc.VarianceSyncServiceBlockingStub budgetingVarianceSyncServiceBlocking,
            CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceBlockingStub collectionsCollectionCaseWorkflowServiceBlocking,
            ContractApprovalServiceGrpc.ContractApprovalServiceBlockingStub contractsContractApprovalServiceBlocking,
            ContractServiceGrpc.ContractServiceBlockingStub contractsContractServiceBlocking,
            RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking,
            CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking,
            PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking,
            PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking,
            LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking,
            ContractWorkflowRequestMapper mapper,
            ContractsRepository repository,
            RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking,
            VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking) {
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.bankingBankAccountServiceAsync = bankingBankAccountServiceAsync;
        this.bankingMandateApprovalServiceBlocking = bankingMandateApprovalServiceBlocking;
        this.budgetingForecastSyncServiceBlocking = budgetingForecastSyncServiceBlocking;
        this.budgetingVarianceSyncServiceAsync = budgetingVarianceSyncServiceAsync;
        this.budgetingVarianceSyncServiceBlocking = budgetingVarianceSyncServiceBlocking;
        this.collectionsCollectionCaseWorkflowServiceBlocking = collectionsCollectionCaseWorkflowServiceBlocking;
        this.contractsContractApprovalServiceBlocking = contractsContractApprovalServiceBlocking;
        this.contractsContractServiceBlocking = contractsContractServiceBlocking;
        this.contractsRenewalValidationServiceBlocking = contractsRenewalValidationServiceBlocking;
        this.customersCustomerServiceBlocking = customersCustomerServiceBlocking;
        this.customersPreferenceCalculationServiceBlocking = customersPreferenceCalculationServiceBlocking;
        this.customersPreferenceValidationServiceBlocking = customersPreferenceValidationServiceBlocking;
        this.lendingLoanApprovalServiceBlocking = lendingLoanApprovalServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskRiskScenarioApprovalServiceBlocking = riskRiskScenarioApprovalServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexValidationServiceBlocking = searchSearchIndexValidationServiceBlocking;
        this.venuesVenueQueryServiceBlocking = venuesVenueQueryServiceBlocking;
    }

    /**
     * Starts a contract.
     */
    public StartContractWorkflowResponse startContractWorkflow(StartContractWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartContractWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContract(request.getContext().getTenantId(), request.getContract());

        var outbound0 = SyncVariancesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SyncVariancesRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContract().getAudit().getCreatedAt())
                                .build())
                        .setCustomer(BudgetLineRef.newBuilder()
                                .setDisplayName(request.getContract().getCounterparty().getDisplayName())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotify().getName())
                                .build())
                        .build())
                .build();
        var reply0 = budgetingVarianceSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).syncVariances(outbound0);

        var outbound1 = mapper.startContractWorkflowStage1(request);
        var reply1 = contractsContractServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createContract(outbound1);

        var outbound2 = BatchCreateBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateBankAccountsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotify())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateBankAccountsResponse.getDefaultInstance());
        var input2 = bankingBankAccountServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateBankAccounts(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamVarianceChangesRequest.newBuilder()
                .setChange(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getContract().getCounterparty().getDisplayName())
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContract().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamVarianceChangesResponse.getDefaultInstance());
        var input3 = budgetingVarianceSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamVarianceChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartContractWorkflow", request.getContractId());
        return StartContractWorkflowResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setBillingAddress(reply1.getContract().getBillingAddress())
                        .setContractId(reply1.getContract().getContractId())
                        .setDueAt(request.getContract().getDueAt())
                        .setNumber(request.getContract().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Advances a contract.
     */
    public AdvanceContractWorkflowResponse advanceContractWorkflow(AdvanceContractWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceContractWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContract(request.getContext().getTenantId(), request.getContract());

        var outbound0 = ExportAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportAccessPoliciesRequest.Filter.newBuilder()
                        .setRegion(request.getContract().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream0 = authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportAccessPolicies(outbound0);
        var reply0 = ExportAccessPoliciesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = CountVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountVenuesRequest.Filter.newBuilder()
                        .setRegion(request.getContract().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(venuesVenueQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countVenues(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getContract().getContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceContractWorkflow", request.getContract().getContractId());
        return AdvanceContractWorkflowResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setContractId(request.getContractId())
                        .setNumber(request.getContract().getNumber())
                        .build())
                .setOutcome(AdvanceContractWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Cancels a contract.
     */
    public CancelContractWorkflowResponse cancelContractWorkflow(CancelContractWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelContractWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContract(request.getContext().getTenantId(), request.getContract());

        var outbound0 = CountVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountVenuesRequest.Filter.newBuilder()
                        .setRegion(request.getContract().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> venuesVenueQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countVenues(outbound0), rpcExecutor);

        var outbound1 = PreviewSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewSearchIndexRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> searchSearchIndexValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewSearchIndex(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getContract().getContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelContractWorkflow", request.getContract().getContractId());
        return CancelContractWorkflowResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getSearchIndex().getAudit().getCreatedBy())
                                .build())
                        .setContractId(request.getContractId())
                        .setCounterparty(request.getContract().getCounterparty())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Retrys a contract.
     */
    public RetryContractWorkflowResponse retryContractWorkflow(RetryContractWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryContractWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContract(request.getContext().getTenantId(), request.getContract());

        var outbound0 = ReconcileForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReconcileForecastsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getContract().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(budgetingForecastSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileForecasts(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = CommentLoanApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentLoanApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContract().getAudit().getCreatedAt())
                                .build())
                        .setCard(LoanCardFunding.newBuilder()
                                .setLast4(switch (reply0.getForecast().getSettlementCase()) {
                                    case CARD -> reply0.getForecast().getCard().getLast4();
                                    default -> "";
                                })
                                .setToken(switch (request.getContract().getSettlementCase()) {
                                    case CARD -> request.getContract().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = lendingLoanApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentLoanApproval(outbound1);

        var outbound2 = mapper.retryContractWorkflowStage2(request, reply0, reply1);
        var reply2 = contractsContractApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingContracts(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getContract().getContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryContractWorkflow", request.getContract().getContractId());
        return RetryContractWorkflowResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getLoan().getAudit().getUpdatedAt())
                                .build())
                        .setContractId(request.getContractId())
                        .setIssuedAt(request.getContract().getIssuedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Pauses a contract.
     */
    public PauseContractWorkflowResponse pauseContractWorkflow(PauseContractWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseContractWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContract(request.getContext().getTenantId(), request.getContract());

        var builder0 = EscalateRiskScenarioRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setRiskScenario(RiskScenario.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedAt(request.getContract().getAudit().getCreatedAt())
                        .setUpdatedAt(request.getContract().getAudit().getUpdatedAt())
                        .build())
                .setCustomer(CustomerRef.newBuilder()
                        .setEmail(request.getDetails().getNotify().getEmail())
                        .build())
                .setRequester(ContactInfo.newBuilder()
                        .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = riskRiskScenarioApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateRiskScenario(outbound0);

        var outbound1 = EstimatePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimatePreferenceRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getContract().getBillingAddress().getLinesList())
                                .setRecipient(request.getContract().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getContract().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.customers.v1.PreferenceCalculationService/EstimatePreference", outbound1);
        var reply1 = customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimatePreference(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getContract().getContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseContractWorkflow", request.getContract().getContractId());
        return PauseContractWorkflowResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setContractId(request.getContractId())
                        .setCounterparty(RenewalRef.newBuilder()
                                .setDisplayName(reply1.getPreference().getOwner().getDisplayName())
                                .build())
                        .setDueAt(request.getContract().getDueAt())
                        .addLines(ContractLine.newBuilder()
                                .setDescription(reply1.getPreference().getDescription())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Resumes a contract.
     */
    public ResumeContractWorkflowResponse resumeContractWorkflow(ResumeContractWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeContractWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContract(request.getContext().getTenantId(), request.getContract());

        var outbound0 = (SearchCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchCustomersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContract().getAudit().getUpdatedAt())
                        .setOwnerId(request.getContract().getCounterparty().getRenewalId())
                        .setRegion(request.getContract().getBillingAddress().getRegion())
                        .build())
                .build()).toBuilder().build();
        var reply0 = customersCustomerServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchCustomers(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getContract().getContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeContractWorkflow", request.getContract().getContractId());
        return ResumeContractWorkflowResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setContractId(request.getContractId())
                        .setCounterparty(request.getContract().getCounterparty())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns a contract.
     */
    public InspectContractWorkflowResponse inspectContractWorkflow(InspectContractWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectContractWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Contract", request.getContractId(), "InspectContractWorkflow");

        var outbound0 = CreateContractRequest.newBuilder()
                .setContext(request.getContext())
                .setContract(Contract.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setContractId(request.getContractId())
                        .build())
                .build();
        var reply0 = contractsContractServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createContract(outbound0);
        return InspectContractWorkflowResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setContractId(request.getContractId())
                        .setCounterparty(RenewalRef.newBuilder()
                                .setRenewalId(reply0.getContract().getCounterparty().getRenewalId())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists contracts a page at a time.
     */
    public ListContractWorkflowsResponse listContractWorkflows(ListContractWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListContractWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Contract", request.getContext().getTraceId(), "ListContractWorkflows");

        var outbound0 = NormalizeRenewalRequest.newBuilder()
                .setContext(request.getContext())
                .setRenewal(Renewal.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> contractsRenewalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeRenewal(value)).orElseThrow();

        var outbound1 = ExplainPreferenceDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPreferenceDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getRenewal().getOrganizer().getEmail())
                                .build())
                        .build())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getRenewal().getAudit().getEtag())
                                .setUpdatedAt(reply0.getRenewal().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getRenewal().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = customersPreferenceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPreferenceDecision(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListContractWorkflows", request.getPage().getPageToken());
        return ListContractWorkflowsResponse.newBuilder()
                .addContracts(Contract.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getPreference().getAudit().getEtag())
                                .build())
                        .setCounterparty(RenewalRef.newBuilder()
                                .setRenewalId(reply0.getRenewal().getRenewalId())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Signals a contract.
     */
    public SignalContractWorkflowResponse signalContractWorkflow(SignalContractWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalContractWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContract(request.getContext().getTenantId(), request.getContract());

        var outbound0 = EscalateMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EscalateMandateRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getContract().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setMandate(Mandate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getContract().getBillingAddress().getPostalCode())
                                .setRecipient(request.getContract().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            EscalateMandateResponse fetch(EscalateMandateRequest input) {
                return bankingMandateApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateMandate(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = JsonCodec.roundTrip(StartCollectionCaseWorkflowRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getMandate().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), StartCollectionCaseWorkflowRequest.class);
        var reply1 = collectionsCollectionCaseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startCollectionCaseWorkflow(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getContract().getContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalContractWorkflow", request.getContract().getContractId());
        return SignalContractWorkflowResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setContractId(request.getContractId())
                        .setIssuedAt(request.getContract().getIssuedAt())
                        .setNumber(reply0.getMandate().getNumber())
                        .build())
                .setOutcome(SignalContractWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .build();
    }
}
