package com.acme.finance.domains.insurance;

import com.acme.accounting.v1.ChartOfAccountsRef;
import com.acme.accounting.v1.EstimateReconciliationRequest;
import com.acme.accounting.v1.EstimateReconciliationResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.LintAuditEventsRequest;
import com.acme.audit.v1.LintAuditEventsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationValidationServiceGrpc;
import com.acme.contracts.v1.ScoreObligationRequest;
import com.acme.contracts.v1.ScoreObligationResponse;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.PreviewCustomerSegmentRequest;
import com.acme.customers.v1.PreviewCustomerSegmentResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.ListPremiumConflictsRequest;
import com.acme.insurance.v1.ListPremiumConflictsResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyBankFunding;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PreviewCoverageRequest;
import com.acme.insurance.v1.PreviewCoverageResponse;
import com.acme.insurance.v1.PreviewPolicyRequest;
import com.acme.insurance.v1.PreviewPolicyResponse;
import com.acme.insurance.v1.PreviewUnderwritingRequest;
import com.acme.insurance.v1.PreviewUnderwritingResponse;
import com.acme.insurance.v1.PullPremiumsRequest;
import com.acme.insurance.v1.PullPremiumsResponse;
import com.acme.insurance.v1.PushPremiumRequest;
import com.acme.insurance.v1.PushPremiumResponse;
import com.acme.insurance.v1.ResolvePremiumConflictRequest;
import com.acme.insurance.v1.ResolvePremiumConflictResponse;
import com.acme.insurance.v1.StreamPremiumChangesRequest;
import com.acme.insurance.v1.StreamPremiumChangesResponse;
import com.acme.insurance.v1.SummarizePoliciesRequest;
import com.acme.insurance.v1.SummarizePoliciesResponse;
import com.acme.insurance.v1.SyncPremiumsRequest;
import com.acme.insurance.v1.SyncPremiumsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.payroll.v1.CheckPayStubEligibilityRequest;
import com.acme.payroll.v1.CheckPayStubEligibilityResponse;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCardSettlement;
import com.acme.payroll.v1.PayStubValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import java.util.List;
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
 * Keeps premiums in step with the systems of record in other regions. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class PremiumSyncApplication {
    private static final Logger log = LoggerFactory.getLogger(PremiumSyncApplication.class);

    private final ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceBlockingStub accountingReconciliationCalculationServiceBlocking;

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ObligationValidationServiceGrpc.ObligationValidationServiceBlockingStub contractsObligationValidationServiceBlocking;

    private final CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking;

    private final PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking;

    private final UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking;

    private final PayStubValidationServiceGrpc.PayStubValidationServiceBlockingStub payrollPayStubValidationServiceBlocking;

    private final InsuranceRepository repository;

    private final Executor rpcExecutor;

    public PremiumSyncApplication(
            ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceBlockingStub accountingReconciliationCalculationServiceBlocking,
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            ChannelRegistry channels,
            ObligationValidationServiceGrpc.ObligationValidationServiceBlockingStub contractsObligationValidationServiceBlocking,
            CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking,
            PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking,
            UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking,
            PayStubValidationServiceGrpc.PayStubValidationServiceBlockingStub payrollPayStubValidationServiceBlocking,
            InsuranceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.accountingReconciliationCalculationServiceBlocking = accountingReconciliationCalculationServiceBlocking;
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.channels = channels;
        this.contractsObligationValidationServiceBlocking = contractsObligationValidationServiceBlocking;
        this.insuranceCoverageValidationServiceBlocking = insuranceCoverageValidationServiceBlocking;
        this.insurancePolicyValidationServiceBlocking = insurancePolicyValidationServiceBlocking;
        this.insuranceUnderwritingValidationServiceBlocking = insuranceUnderwritingValidationServiceBlocking;
        this.payrollPayStubValidationServiceBlocking = payrollPayStubValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Syncs a premium.
     */
    public SyncPremiumsResponse syncPremiums(SyncPremiumsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SyncPremiums")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        List<LintAuditEventsResponse> results0 = request.getPremium().getLinesList().stream()
                .map(element -> LintAuditEventsRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .putAllAttributes(element.getAttributesMap())
                        .setDetail(Any.pack(request.getContext()))
                        .setSource(element.getLineId())
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(request.getPremium().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(LintAuditEventsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getPremium().getCounterparty().getEmail())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintAuditEvents(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? LintAuditEventsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = EstimateReconciliationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateReconciliationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getPremium().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getPremium().getCounterparty().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPremium().getAudit().getUpdatedAt())
                                .build())
                        .setCustomer(ChartOfAccountsRef.newBuilder()
                                .setDisplayName(request.getPremium().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            EstimateReconciliationResponse fetch(EstimateReconciliationRequest input) {
                return accountingReconciliationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateReconciliation(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SyncPremiums", request.getPremium().getPremiumId());
        return SyncPremiumsResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setNumber(request.getPremium().getNumber())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Pushs a premium.
     */
    public PushPremiumResponse pushPremium(PushPremiumRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PushPremium")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = PreviewCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getPremium().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getPremium().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setDetails(PreviewCoverageRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getPremium().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewCoverage(value)).orElseThrow();

        var outbound1 = PreviewCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewCustomerSegmentRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getPremium().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.customers.v1.CustomerSegmentValidationService");
        var selected1 = CustomerSegmentValidationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.previewCustomerSegment(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PushPremium", request.getPremium().getPremiumId());
        return PushPremiumResponse.newBuilder()
                .setOutcome(PushPremiumResponse.Outcome.newBuilder()
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getCoverage().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply0.getCoverage().getOwner().getEmail())
                                .build())
                        .setPremiumId(request.getPremiumId())
                        .setStatus(request.getPremium().getStatus())
                        .build())
                .build();
    }

    /**
     * Pulls a premium.
     */
    public PullPremiumsResponse pullPremiums(PullPremiumsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PullPremiums")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = ScoreObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreObligationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getPremium().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getPremium().getCounterparty().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getPremium().getAudit().getCreatedBy())
                                .setEtag(request.getPremium().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(contractsObligationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreObligation(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PullPremiums", request.getPremium().getPremiumId());
        return PullPremiumsResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply0.getObligation().getCustomer().getEmail())
                                .build())
                        .setPremiumId(request.getPremiumId())
                        .setStatus(request.getPremium().getStatus())
                        .setTotal(reply0.getBreakdown().getTotal())
                        .build())
                .build();
    }

    /**
     * Exchanges changes of premiums with a peer.
     */
    public StreamPremiumChangesResponse streamPremiumChanges(StreamPremiumChangesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamPremiumChanges")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasChange()) {
            repository.savePremium(request.getContext().getTenantId(), request.getChange());
        }

        var outbound0 = PreviewPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setBank(PolicyBankFunding.newBuilder()
                                .setBic(request.getChange().getBank().getBic())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getChange().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> insurancePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewPolicy(outbound0), rpcExecutor);

        var outbound1 = PreviewUnderwritingRequest.newBuilder()
                .setContext(request.getContext())
                .setUnderwriting(Underwriting.newBuilder()
                        .addComments(UnderwritingComment.newBuilder()
                                .setCreatedAt(request.getChange().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insuranceUnderwritingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewUnderwriting(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getChange().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamPremiumChanges", request.getChange().getPremiumId());
        return StreamPremiumChangesResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getPolicy().getAudit().getEtag())
                                .setUpdatedBy(reply1.getUnderwriting().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply1.getUnderwriting().getAssignee().getEmail())
                                .build())
                        .setIssuedAt(request.getChange().getIssuedAt())
                        .build())
                .setSequence(request.getAck().getSequence())
                .build();
    }

    /**
     * Resolves a premium.
     */
    public ResolvePremiumConflictResponse resolvePremiumConflict(ResolvePremiumConflictRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolvePremiumConflict")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePremium(request.getContext().getTenantId(), request.getPremium());

        var outbound0 = BeanCopies.copy(CheckPayStubEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getPremium().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getPremium().getAudit().getUpdatedAt())
                                .build())
                        .setCard(PayStubCardSettlement.newBuilder()
                                .setLast4(request.getPremium().getCard().getLast4())
                                .build())
                        .build())
                .build(), CheckPayStubEligibilityRequest.class);
        var reply0 = payrollPayStubValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkPayStubEligibility(outbound0);

        var outbound1 = SummarizePoliciesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizePoliciesRequest.Details.newBuilder()
                        .putAllFilters(reply0.getPayStub().getMetadataMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var channel1 = channels.lookup("acme.insurance.v1.PolicyQueryService");
        var selected1 = PolicyQueryServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.summarizePolicies(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPremium().getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResolvePremiumConflict", request.getPremium().getPremiumId());
        return ResolvePremiumConflictResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setDueAt(reply0.getPayStub().getDueAt())
                        .setIssuedAt(request.getPremium().getIssuedAt())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Lists premiums a page at a time.
     */
    public ListPremiumConflictsResponse listPremiumConflicts(ListPremiumConflictsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPremiumConflicts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Premium", request.getContext().getTraceId(), "ListPremiumConflicts");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPremiumConflicts", request.getPage().getPageToken());
        return ListPremiumConflictsResponse.getDefaultInstance();
    }
}
