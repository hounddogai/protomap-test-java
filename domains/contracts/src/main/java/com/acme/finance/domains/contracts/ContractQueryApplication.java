package com.acme.finance.domains.contracts;

import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.UnsubscribeAuditTrailUpdatesRequest;
import com.acme.audit.v1.UnsubscribeAuditTrailUpdatesResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.ExportAccessPoliciesRequest;
import com.acme.authorization.v1.ExportAccessPoliciesResponse;
import com.acme.broadcasts.v1.BroadcastQueryServiceGrpc;
import com.acme.broadcasts.v1.CountBroadcastsRequest;
import com.acme.broadcasts.v1.CountBroadcastsResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetValidationServiceGrpc;
import com.acme.budgeting.v1.PreviewBudgetRequest;
import com.acme.budgeting.v1.PreviewBudgetResponse;
import com.acme.certificates.v1.Certificate;
import com.acme.certificates.v1.CertificateCalculationServiceGrpc;
import com.acme.certificates.v1.CompareCertificateOptionsRequest;
import com.acme.certificates.v1.CompareCertificateOptionsResponse;
import com.acme.certificates.v1.CsrRef;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.BrowseContractsRequest;
import com.acme.contracts.v1.BrowseContractsResponse;
import com.acme.contracts.v1.CheckRenewalEligibilityRequest;
import com.acme.contracts.v1.CheckRenewalEligibilityResponse;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.CountContractsRequest;
import com.acme.contracts.v1.CountContractsResponse;
import com.acme.contracts.v1.ExportContractsRequest;
import com.acme.contracts.v1.ExportContractsResponse;
import com.acme.contracts.v1.FindContractsRequest;
import com.acme.contracts.v1.FindContractsResponse;
import com.acme.contracts.v1.LookupContractRequest;
import com.acme.contracts.v1.LookupContractResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationValidationServiceGrpc;
import com.acme.contracts.v1.PeekContractRequest;
import com.acme.contracts.v1.PeekContractResponse;
import com.acme.contracts.v1.PreviewObligationRequest;
import com.acme.contracts.v1.PreviewObligationResponse;
import com.acme.contracts.v1.PreviewRenewalRequest;
import com.acme.contracts.v1.PreviewRenewalResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalRef;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.contracts.v1.ResolveContractsRequest;
import com.acme.contracts.v1.ResolveContractsResponse;
import com.acme.contracts.v1.ScoreObligationRequest;
import com.acme.contracts.v1.ScoreObligationResponse;
import com.acme.contracts.v1.SuggestContractsRequest;
import com.acme.contracts.v1.SuggestContractsResponse;
import com.acme.contracts.v1.SummarizeContractsRequest;
import com.acme.contracts.v1.SummarizeContractsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.labeling.v1.LabelingTask;
import com.acme.labeling.v1.LabelingTaskCalculationServiceGrpc;
import com.acme.labeling.v1.SimulateLabelingTaskRequest;
import com.acme.labeling.v1.SimulateLabelingTaskResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentCalculationServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.SimulateRepaymentRequest;
import com.acme.lending.v1.SimulateRepaymentResponse;
import com.acme.risk.v1.ResolveRiskRegistersRequest;
import com.acme.risk.v1.ResolveRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about contracts, for screens and reports. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ContractQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(ContractQueryApplication.class);

    private final AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceFutureStub auditAuditTrailNotificationServiceFuture;

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final BroadcastQueryServiceGrpc.BroadcastQueryServiceBlockingStub broadcastsBroadcastQueryServiceBlocking;

    private final BudgetValidationServiceGrpc.BudgetValidationServiceBlockingStub budgetingBudgetValidationServiceBlocking;

    private final CertificateCalculationServiceGrpc.CertificateCalculationServiceBlockingStub certificatesCertificateCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final ObligationValidationServiceGrpc.ObligationValidationServiceBlockingStub contractsObligationValidationServiceBlocking;

    private final RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking;

    private final RenewalValidationServiceGrpc.RenewalValidationServiceFutureStub contractsRenewalValidationServiceFuture;

    private final LabelingTaskCalculationServiceGrpc.LabelingTaskCalculationServiceBlockingStub labelingLabelingTaskCalculationServiceBlocking;

    private final RepaymentCalculationServiceGrpc.RepaymentCalculationServiceBlockingStub lendingRepaymentCalculationServiceBlocking;

    private final ContractQueryRequestMapper mapper;

    private final ContractsRepository repository;

    private final RiskRegisterQueryServiceGrpc.RiskRegisterQueryServiceBlockingStub riskRiskRegisterQueryServiceBlocking;

    public ContractQueryApplication(
            AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceFutureStub auditAuditTrailNotificationServiceFuture,
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            BroadcastQueryServiceGrpc.BroadcastQueryServiceBlockingStub broadcastsBroadcastQueryServiceBlocking,
            BudgetValidationServiceGrpc.BudgetValidationServiceBlockingStub budgetingBudgetValidationServiceBlocking,
            CertificateCalculationServiceGrpc.CertificateCalculationServiceBlockingStub certificatesCertificateCalculationServiceBlocking,
            ChannelRegistry channels,
            ObligationValidationServiceGrpc.ObligationValidationServiceBlockingStub contractsObligationValidationServiceBlocking,
            RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking,
            RenewalValidationServiceGrpc.RenewalValidationServiceFutureStub contractsRenewalValidationServiceFuture,
            LabelingTaskCalculationServiceGrpc.LabelingTaskCalculationServiceBlockingStub labelingLabelingTaskCalculationServiceBlocking,
            RepaymentCalculationServiceGrpc.RepaymentCalculationServiceBlockingStub lendingRepaymentCalculationServiceBlocking,
            ContractQueryRequestMapper mapper,
            ContractsRepository repository,
            RiskRegisterQueryServiceGrpc.RiskRegisterQueryServiceBlockingStub riskRiskRegisterQueryServiceBlocking) {
        this.auditAuditTrailNotificationServiceFuture = auditAuditTrailNotificationServiceFuture;
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.broadcastsBroadcastQueryServiceBlocking = broadcastsBroadcastQueryServiceBlocking;
        this.budgetingBudgetValidationServiceBlocking = budgetingBudgetValidationServiceBlocking;
        this.certificatesCertificateCalculationServiceBlocking = certificatesCertificateCalculationServiceBlocking;
        this.channels = channels;
        this.contractsObligationValidationServiceBlocking = contractsObligationValidationServiceBlocking;
        this.contractsRenewalValidationServiceBlocking = contractsRenewalValidationServiceBlocking;
        this.contractsRenewalValidationServiceFuture = contractsRenewalValidationServiceFuture;
        this.labelingLabelingTaskCalculationServiceBlocking = labelingLabelingTaskCalculationServiceBlocking;
        this.lendingRepaymentCalculationServiceBlocking = lendingRepaymentCalculationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskRiskRegisterQueryServiceBlocking = riskRiskRegisterQueryServiceBlocking;
    }

    /**
     * Returns a contract.
     */
    public LookupContractResponse lookupContract(LookupContractRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupContract")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Contract", request.getContractId(), "LookupContract");

        var outbound0 = CheckRenewalEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckRenewalEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setRenewal(Renewal.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            CheckRenewalEligibilityResponse fetch(CheckRenewalEligibilityRequest input) {
                return contractsRenewalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkRenewalEligibility(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        return LookupContractResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getRenewal().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getRenewal().getLocation().getLinesList())
                                .build())
                        .setContractId(request.getContractId())
                        .build())
                .build();
    }

    /**
     * Lists contracts a page at a time.
     */
    public BrowseContractsResponse browseContracts(BrowseContractsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseContracts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Contract", request.getContext().getTraceId(), "BrowseContracts");

        var outbound0 = UnsubscribeAuditTrailUpdatesRequest.newBuilder()
                .putAllAnnotations(request.getContext().getBaggageMap())
                .setContext(request.getContext())
                .setDetails(UnsubscribeAuditTrailUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllVariables(request.getFilter().getLabelsMap())
                        .build())
                .build();
        var pending0 = auditAuditTrailNotificationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).unsubscribeAuditTrailUpdates(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseContracts", request.getPage().getPageToken());
        return BrowseContractsResponse.newBuilder()
                .addContracts(Contract.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getAuditTrail().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists contracts a page at a time.
     */
    public FindContractsResponse findContracts(FindContractsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindContracts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Contract", request.getContext().getTraceId(), "FindContracts");

        var outbound0 = (ResolveRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply0 = riskRiskRegisterQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveRiskRegisters(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindContracts", request.getPage().getPageToken());
        return FindContractsResponse.newBuilder()
                .setPageInfo(PageInfo.newBuilder()
                        .setNextPageToken(reply0.getPageInfo().getNextPageToken())
                        .setTotalSize(reply0.getPageInfo().getTotalSize())
                        .build())
                .build();
    }

    /**
     * Counts contracts by status.
     */
    public CountContractsResponse countContracts(CountContractsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountContracts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Contract", request.getContext().getTraceId(), "CountContracts");

        var outbound0 = mapper.countContractsStage0(request);
        var reply0 = lendingRepaymentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateRepayment(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountContracts", request.getPage().getPageToken());
        return CountContractsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns several contracts at once.
     */
    public ResolveContractsResponse resolveContracts(ResolveContractsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveContracts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Contract", request.getContext().getTraceId(), "ResolveContracts");

        var outbound0 = ScoreObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(contractsObligationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreObligation(value)))
                .collect(Collectors.toList()).getFirst();

        var builder1 = SimulateLabelingTaskRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setLabelingTask(LabelingTask.newBuilder()
                .setAssignee(UserRef.newBuilder()
                        .setDisplayName(reply0.getObligation().getAssignee().getDisplayName())
                        .build())
                .setRequester(ContactInfo.newBuilder()
                        .setName(reply0.getObligation().getRequester().getName())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = labelingLabelingTaskCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateLabelingTask(outbound1);
        return ResolveContractsResponse.newBuilder()
                .addContracts(Contract.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getObligation().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Streams every matching contract.
     */
    public ExportContractsResponse exportContracts(ExportContractsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportContracts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Contract", request.getContext().getTraceId(), "ExportContracts");

        var outbound0 = PreviewObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.contracts.v1.ObligationValidationService");
        var selected0 = ObligationValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.previewObligation(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportContracts", request.getFilter().getQuery());
        return ExportContractsResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getObligation().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(RenewalRef.newBuilder()
                                .setDisplayName(reply0.getObligation().getCustomer().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Summarizes a contract.
     */
    public SummarizeContractsResponse summarizeContracts(SummarizeContractsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeContracts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Contract", request.getContractId(), "SummarizeContracts");

        var outbound0 = mapper.summarizeContractsStage0(request);
        var reply0 = broadcastsBroadcastQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countBroadcasts(outbound0);

        var outbound1 = ExportAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportAccessPoliciesRequest.Filter.newBuilder()
                        .putAllLabels(request.getDetails().getFiltersMap())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .build())
                .setFormat(request.getDetails().getFormat())
                .build();
        var stream1 = authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportAccessPolicies(outbound1);
        var reply1 = ExportAccessPoliciesResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = PreviewBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var reply2 = budgetingBudgetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewBudget(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeContracts", request.getContractId());
        return SummarizeContractsResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists contracts a page at a time.
     */
    public SuggestContractsResponse suggestContracts(SuggestContractsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestContracts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Contract", request.getContext().getTraceId(), "SuggestContracts");

        var outbound0 = JsonCodec.roundTrip(PreviewObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewObligationRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build(), PreviewObligationRequest.class);
        var reply0 = contractsObligationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewObligation(outbound0);

        var outbound1 = CompareCertificateOptionsRequest.newBuilder()
                .setCertificate(Certificate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getObligation().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(CsrRef.newBuilder()
                                .setDisplayName(reply0.getObligation().getCustomer().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = certificatesCertificateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareCertificateOptions(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestContracts", request.getPage().getPageToken());
        return SuggestContractsResponse.newBuilder()
                .addContracts(Contract.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getObligation().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply1.getCertificate().getBillingAddress().getRecipient())
                                .build())
                        .setCounterparty(RenewalRef.newBuilder()
                                .setDisplayName(reply0.getObligation().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Returns a contract.
     */
    public PeekContractResponse peekContract(PeekContractRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekContract")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Contract", request.getContractId(), "PeekContract");

        var outbound0 = PreviewRenewalRequest.newBuilder()
                .setContext(request.getContext())
                .setRenewal(Renewal.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = contractsRenewalValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).previewRenewal(outbound0);
        var reply0 = RpcSupport.await(pending0);
        return PeekContractResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getRenewal().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getRenewal().getLocation().getLocality())
                                .build())
                        .setContractId(request.getContractId())
                        .build())
                .build();
    }
}
