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
import org.springframework.stereotype.Service;

/**
 * Manages contracts: creates, reads, changes, and lists them. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class ContractApplication {
    private static final Logger log = LoggerFactory.getLogger(ContractApplication.class);

    private final PaymentPromiseQueryServiceGrpc.PaymentPromiseQueryServiceBlockingStub collectionsPaymentPromiseQueryServiceBlocking;

    private final ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking;

    private final ContractClauseQueryServiceGrpc.ContractClauseQueryServiceBlockingStub contractsContractClauseQueryServiceBlocking;

    private final ContractQueryServiceGrpc.ContractQueryServiceBlockingStub contractsContractQueryServiceBlocking;

    private final ObligationCalculationServiceGrpc.ObligationCalculationServiceBlockingStub contractsObligationCalculationServiceBlocking;

    private final ObligationValidationServiceGrpc.ObligationValidationServiceBlockingStub contractsObligationValidationServiceBlocking;

    private final RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking;

    private final LoanValidationServiceGrpc.LoanValidationServiceBlockingStub lendingLoanValidationServiceBlocking;

    private final ContractRequestMapper mapper;

    private final MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking;

    private final ContractsRepository repository;

    public ContractApplication(
            PaymentPromiseQueryServiceGrpc.PaymentPromiseQueryServiceBlockingStub collectionsPaymentPromiseQueryServiceBlocking,
            ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking,
            ContractClauseQueryServiceGrpc.ContractClauseQueryServiceBlockingStub contractsContractClauseQueryServiceBlocking,
            ContractQueryServiceGrpc.ContractQueryServiceBlockingStub contractsContractQueryServiceBlocking,
            ObligationCalculationServiceGrpc.ObligationCalculationServiceBlockingStub contractsObligationCalculationServiceBlocking,
            ObligationValidationServiceGrpc.ObligationValidationServiceBlockingStub contractsObligationValidationServiceBlocking,
            RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking,
            ApplicationEventPublisher events,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking,
            LoanValidationServiceGrpc.LoanValidationServiceBlockingStub lendingLoanValidationServiceBlocking,
            ContractRequestMapper mapper,
            MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking,
            ContractsRepository repository) {
        this.collectionsPaymentPromiseQueryServiceBlocking = collectionsPaymentPromiseQueryServiceBlocking;
        this.complianceComplianceCaseCalculationServiceBlocking = complianceComplianceCaseCalculationServiceBlocking;
        this.contractsContractClauseQueryServiceBlocking = contractsContractClauseQueryServiceBlocking;
        this.contractsContractQueryServiceBlocking = contractsContractQueryServiceBlocking;
        this.contractsObligationCalculationServiceBlocking = contractsObligationCalculationServiceBlocking;
        this.contractsObligationValidationServiceBlocking = contractsObligationValidationServiceBlocking;
        this.contractsRenewalValidationServiceBlocking = contractsRenewalValidationServiceBlocking;
        this.events = events;
        this.ledgerLedgerAccountHistoryServiceBlocking = ledgerLedgerAccountHistoryServiceBlocking;
        this.lendingLoanValidationServiceBlocking = lendingLoanValidationServiceBlocking;
        this.mapper = mapper;
        this.metricsMetricSeriesServiceBlocking = metricsMetricSeriesServiceBlocking;
        this.repository = repository;
    }

    public record UpdateContractStage2Requested(BatchGetFeatureFlagsRequest request, CompletableFuture<BatchGetFeatureFlagsResponse> completion, io.grpc.Context context) {}

    /**
     * Creates a contract.
     */
    public CreateContractResponse createContract(CreateContractRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CreateContract")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContract(request.getContext().getTenantId(), request.getContract());

        var outbound0 = BeanCopies.copy(LintObligationsRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setDisplayName(request.getContract().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build(), LintObligationsRequest.class);
        var reply0 = contractsObligationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintObligations(outbound0);

        var outbound1 = ValidateObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateObligationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply0.getObligation().getRequester().getPhone())
                                .setPreferredChannel(reply0.getObligation().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .setObligation(Obligation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getObligation().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> contractsObligationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateObligation(value));

        var outbound2 = ExplainObligationCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainObligationCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply1.getObligation().getRequester().getPhone())
                                .build())
                        .build())
                .setObligation(Obligation.newBuilder()
                        .setSla(ObligationSla.newBuilder()
                                .setPausedFor(reply1.getObligation().getSla().getPausedFor())
                                .build())
                        .build())
                .build();
        RpcPort<ExplainObligationCalculationRequest, ExplainObligationCalculationResponse> port2 = RpcPort.adapt(contractsObligationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainObligationCalculation);
        var reply2 = port2.exchange(outbound2);

        var outbound3 = NormalizeLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeLoanRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply1.getObligation().getRequester().getName())
                                .build())
                        .build())
                .setLoan(Loan.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply2.getObligation().getRequester().getPhone())
                                .build())
                        .build())
                .build();
        Function<NormalizeLoanRequest, NormalizeLoanResponse> operation3 = lendingLoanValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::normalizeLoan;
        var reply3 = operation3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getContract().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CreateContract", request.getContract().getNumber());
        return CreateContractResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getObligation().getAudit().getEtag())
                                .setUpdatedBy(reply2.getObligation().getAudit().getUpdatedBy())
                                .build())
                        .setContractId(request.getContract().getContractId())
                        .setCounterparty(request.getContract().getCounterparty())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a contract.
     */
    public GetContractResponse getContract(GetContractRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetContract")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Contract", request.getContractId(), "GetContract");

        var outbound0 = mapper.getContractStage0(request);
        var reply0 = contractsRenewalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewRenewal(outbound0);
        return GetContractResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getRenewal().getLocation().getPostalCode())
                                .build())
                        .setContractId(request.getContractId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Changes a contract.
     */
    public UpdateContractResponse updateContract(UpdateContractRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UpdateContract")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContract(request.getContext().getTenantId(), request.getContract());

        var outbound0 = LookupContractRequest.newBuilder()
                .setContext(request.getContext())
                .setContractId(request.getContract().getContractId())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> contractsContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lookupContract(value)).orElseThrow();

        var outbound1 = AnnotateLedgerAccountVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AnnotateLedgerAccountVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getContract().getAudit().getUpdatedAt())
                                .build())
                        .setBank(LedgerAccountBankFunding.newBuilder()
                                .setBic(switch (request.getContract().getSettlementCase()) {
                                    case BANK -> request.getContract().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = ledgerLedgerAccountHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotateLedgerAccountVersion(outbound1);

        var outbound2 = BatchGetFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var completion2 = new CompletableFuture<BatchGetFeatureFlagsResponse>();
        events.publishEvent(new UpdateContractStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound3 = CountContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountContractsRequest.Filter.newBuilder()
                        .setRegion(reply0.getContract().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply3 = Stream.of(outbound3).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(contractsContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countContracts(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getContract().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UpdateContract", request.getContract().getNumber());
        return UpdateContractResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getContract().getAudit().getUpdatedAt())
                                .build())
                        .setContractId(request.getContract().getContractId())
                        .setIssuedAt(request.getContract().getIssuedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists contracts a page at a time.
     */
    public ListContractsResponse listContracts(ListContractsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListContracts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Contract", request.getContext().getTraceId(), "ListContracts");

        var outbound0 = WatchMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchMetricSeriesesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .setOwnerId(request.getFilter().getOwnerId())
                        .setQuery(request.getFilter().getQuery())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var stream0 = metricsMetricSeriesServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchMetricSerieses(outbound0);
        var reply0 = WatchMetricSeriesesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListContracts", request.getPage().getPageToken());
        return ListContractsResponse.getDefaultInstance();
    }

    /**
     * Returns several contracts at once.
     */
    public BatchGetContractsResponse batchGetContracts(BatchGetContractsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchGetContracts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Contract", request.getContext().getTraceId(), "BatchGetContracts");
        return BatchGetContractsResponse.getDefaultInstance();
    }

    /**
     * Lists contracts a page at a time.
     */
    public SearchContractsResponse searchContracts(SearchContractsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SearchContracts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Contract", request.getContext().getTraceId(), "SearchContracts");

        var outbound0 = ExportPaymentPromisesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportPaymentPromisesRequest.Filter.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .build();
        var stream0 = collectionsPaymentPromiseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportPaymentPromises(outbound0);
        var reply0 = ExportPaymentPromisesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = RecalculateComplianceCasesRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .addHistory(ComplianceCaseStatusChange.newBuilder()
                                .setChangedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RecalculateComplianceCasesRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.compliance.v1.ComplianceCaseCalculationService/RecalculateComplianceCases", outbound1);
        var reply1 = complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateComplianceCases(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SearchContracts", request.getPage().getPageToken());
        return SearchContractsResponse.newBuilder()
                .addContracts(Contract.newBuilder()
                        .setCounterparty(RenewalRef.newBuilder()
                                .setDisplayName(reply1.getComplianceCase().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Streams the changes of contracts as they happen.
     */
    public WatchContractsResponse watchContracts(WatchContractsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "WatchContracts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Contract", request.getContext().getTraceId(), "WatchContracts");

        var outbound0 = SummarizeContractsRequest.newBuilder()
                .putAllAnnotations(request.getContext().getBaggageMap())
                .setContext(request.getContext())
                .setDetails(SummarizeContractsRequest.Details.newBuilder()
                        .putAllFilters(request.getFilter().getLabelsMap())
                        .build())
                .build();
        final class Stage0Adapter {
            SummarizeContractsResponse fetch(SummarizeContractsRequest input) {
                return contractsContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeContracts(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "WatchContracts", request.getFilter().getQuery());
        return WatchContractsResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Takes a stream of contracts and answers once at the end.
     */
    public BatchCreateContractsResponse batchCreateContracts(BatchCreateContractsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchCreateContracts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.saveContract(request.getContext().getTenantId(), request.getRecord());
        }

        var outbound0 = SimulateObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getHeader().getSubmittedBy().getName())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> contractsObligationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateObligation(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BatchCreateContracts", request.getHeader().getSource());
        return BatchCreateContractsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Creates a contract.
     */
    public CloneContractResponse cloneContract(CloneContractRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CloneContract")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContract(request.getContext().getTenantId(), request.getContract());

        var outbound0 = mapper.cloneContractStage0(request);
        var reply0 = contractsContractClauseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveContractClauses(outbound0);

        var outbound1 = ResolveContractsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(contractsContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveContracts(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getContract().getCounterparty().getRenewalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CloneContract", request.getContract().getCounterparty().getRenewalId());
        return CloneContractResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setContractId(request.getContract().getContractId())
                        .setNumber(request.getContract().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
