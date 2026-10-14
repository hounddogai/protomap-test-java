package com.acme.finance.domains.insurance;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.ExplainAuditEventDecisionRequest;
import com.acme.audit.v1.ExplainAuditEventDecisionResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.ValidateRetentionPolicyRequest;
import com.acme.audit.v1.ValidateRetentionPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimQueryServiceGrpc;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.CompareUnderwritingOptionsRequest;
import com.acme.insurance.v1.CompareUnderwritingOptionsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.CreatePolicyRequest;
import com.acme.insurance.v1.CreatePolicyResponse;
import com.acme.insurance.v1.DeletePolicyRequest;
import com.acme.insurance.v1.DeletePolicyResponse;
import com.acme.insurance.v1.ExplainCoverageCalculationRequest;
import com.acme.insurance.v1.ExplainCoverageCalculationResponse;
import com.acme.insurance.v1.ExportClaimsRequest;
import com.acme.insurance.v1.ExportClaimsResponse;
import com.acme.insurance.v1.GetPolicyRequest;
import com.acme.insurance.v1.GetPolicyResponse;
import com.acme.insurance.v1.ListPoliciesRequest;
import com.acme.insurance.v1.ListPoliciesResponse;
import com.acme.insurance.v1.NormalizeClaimRequest;
import com.acme.insurance.v1.NormalizeClaimResponse;
import com.acme.insurance.v1.PeekPolicyRequest;
import com.acme.insurance.v1.PeekPolicyResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.insurance.v1.SearchPoliciesRequest;
import com.acme.insurance.v1.SearchPoliciesResponse;
import com.acme.insurance.v1.SummarizeClaimsRequest;
import com.acme.insurance.v1.SummarizeClaimsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingCalculationServiceGrpc;
import com.acme.insurance.v1.UpdatePolicyRequest;
import com.acme.insurance.v1.UpdatePolicyResponse;
import com.acme.insurance.v1.WatchPoliciesRequest;
import com.acme.insurance.v1.WatchPoliciesResponse;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportRequest;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportResponse;
import com.acme.search.v1.PreviewSearchIndexRequest;
import com.acme.search.v1.PreviewSearchIndexResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.venues.v1.ScoreVenueRequest;
import com.acme.venues.v1.ScoreVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueComponent;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Manages policies: creates, reads, changes, and lists them. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class PolicyApplication {
    private static final Logger log = LoggerFactory.getLogger(PolicyApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking;

    private final ClaimQueryServiceGrpc.ClaimQueryServiceBlockingStub insuranceClaimQueryServiceBlocking;

    private final ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking;

    private final CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking;

    private final PolicyQueryServiceGrpc.PolicyQueryServiceBlockingStub insurancePolicyQueryServiceBlocking;

    private final UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking;

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking;

    private final InsuranceRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking;

    private final VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking;

    public PolicyApplication(
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking,
            ClaimQueryServiceGrpc.ClaimQueryServiceBlockingStub insuranceClaimQueryServiceBlocking,
            ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking,
            CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking,
            PolicyQueryServiceGrpc.PolicyQueryServiceBlockingStub insurancePolicyQueryServiceBlocking,
            UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking,
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking,
            InsuranceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking,
            VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking) {
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.auditRetentionPolicyValidationServiceBlocking = auditRetentionPolicyValidationServiceBlocking;
        this.insuranceClaimQueryServiceBlocking = insuranceClaimQueryServiceBlocking;
        this.insuranceClaimValidationServiceBlocking = insuranceClaimValidationServiceBlocking;
        this.insuranceCoverageCalculationServiceBlocking = insuranceCoverageCalculationServiceBlocking;
        this.insurancePolicyQueryServiceBlocking = insurancePolicyQueryServiceBlocking;
        this.insuranceUnderwritingCalculationServiceBlocking = insuranceUnderwritingCalculationServiceBlocking;
        this.ledgerFiscalPeriodExportServiceBlocking = ledgerFiscalPeriodExportServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexValidationServiceBlocking = searchSearchIndexValidationServiceBlocking;
        this.venuesVenueValidationServiceBlocking = venuesVenueValidationServiceBlocking;
    }

    /**
     * Creates a policy.
     */
    public CreatePolicyResponse createPolicy(CreatePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CreatePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = PeekPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicyId(request.getPolicy().getPolicyId())
                .build();
        var reply0 = RpcSupport.retryRead(() -> insurancePolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).peekPolicy(outbound0));

        var outbound1 = ExportClaimsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportClaimsRequest.Filter.newBuilder()
                        .putAllLabels(request.getPolicy().getLabelsMap())
                        .build())
                .build();
        var stream1 = insuranceClaimQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportClaims(outbound1);
        var reply1 = ExportClaimsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getPolicy().getContact())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateAuditEventsResponse.getDefaultInstance());
        var input2 = auditAuditEventServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateAuditEvents(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPolicy().getAudit().getCreatedAt())
                                .setCreatedBy(request.getPolicy().getAudit().getCreatedBy())
                                .setEtag(request.getPolicy().getAudit().getEtag())
                                .setUpdatedAt(request.getPolicy().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getCard().getHolderName()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CreatePolicy", request.getPolicy().getCard().getHolderName());
        return CreatePolicyResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getPolicy().getContact().getEmail())
                                .build())
                        .setCurrencyCode(request.getPolicy().getCurrencyCode())
                        .setHolder(request.getPolicy().getHolder())
                        .build())
                .build();
    }

    /**
     * Returns a policy.
     */
    public GetPolicyResponse getPolicy(GetPolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getPolicyId(), "GetPolicy");

        var outbound0 = ScoreVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setVenue(Venue.newBuilder()
                        .putComponents("primary", VenueComponent.newBuilder()
                                .setComponentId(request.getPolicyId())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<ScoreVenueRequest, ScoreVenueResponse>() {
            @Override
            public ScoreVenueResponse apply(ScoreVenueRequest input) {
                return venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreVenue(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        return GetPolicyResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getVenue().getAudit().getUpdatedAt())
                                .build())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .build();
    }

    /**
     * Changes a policy.
     */
    public UpdatePolicyResponse updatePolicy(UpdatePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UpdatePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = BeanCopies.copy(ExplainCoverageCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPolicy().getAudit().getCreatedAt())
                                .setCreatedBy(request.getPolicy().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getPolicy().getHolder().getDisplayName())
                                .build())
                        .build())
                .build(), ExplainCoverageCalculationRequest.class);
        var reply0 = insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCoverageCalculation(outbound0);

        var outbound1 = NormalizeClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getCoverage().getOwner().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getCoverage().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        Function<NormalizeClaimRequest, NormalizeClaimResponse> operation1 = insuranceClaimValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::normalizeClaim;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UpdatePolicy", request.getPolicy().getPolicyId());
        return UpdatePolicyResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setAvailable(request.getPolicy().getAvailable())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getCoverage().getOwner().getEmail())
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setVersion(reply0.getCoverage().getVersion())
                                .build())
                        .setOpenedAt(request.getPolicy().getOpenedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Deletes a policy.
     */
    public DeletePolicyResponse deletePolicy(DeletePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeletePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getPolicyId(), "DeletePolicy");

        var outbound0 = SummarizeClaimsRequest.newBuilder()
                .putAllAnnotations(request.getContext().getBaggageMap())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> insuranceClaimQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeClaims(outbound0), rpcExecutor);

        var outbound1 = PreviewSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewSearchIndexRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .setSearchIndex(SearchIndex.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getEtag())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> searchSearchIndexValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewSearchIndex(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = (ValidateRetentionPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateRetentionPolicyRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply1.getSearchIndex().getOwner().getEmail())
                                .build())
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getSearchIndex().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = auditRetentionPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateRetentionPolicy(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeletePolicy", request.getPolicyId());
        return DeletePolicyResponse.newBuilder()
                .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists policies a page at a time.
     */
    public ListPoliciesResponse listPolicies(ListPoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getContext().getTraceId(), "ListPolicies");

        var outbound0 = (ExplainAuditEventDecisionRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainAuditEventDecisionRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build()).toBuilder().build();
        var reply0 = auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainAuditEventDecision(outbound0);

        var builder1 = ScheduleFiscalPeriodExportRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setDetails(ScheduleFiscalPeriodExportRequest.Details.newBuilder()
                .putAllFilters(reply0.getResult().getDetailsMap())
                .build());
        var outbound1 = builder1.build();
        var reply1 = ledgerFiscalPeriodExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleFiscalPeriodExport(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPolicies", request.getPage().getPageToken());
        return ListPoliciesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getAuditEvent().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Lists policies a page at a time.
     */
    public SearchPoliciesResponse searchPolicies(SearchPoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SearchPolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getContext().getTraceId(), "SearchPolicies");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SearchPolicies", request.getPage().getPageToken());
        return SearchPoliciesResponse.getDefaultInstance();
    }

    /**
     * Streams the changes of policies as they happen.
     */
    public WatchPoliciesResponse watchPolicies(WatchPoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "WatchPolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getContext().getTraceId(), "WatchPolicies");

        var outbound0 = CompareUnderwritingOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setUnderwriting(Underwriting.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .setPhone(request.getSubscriber().getPhone())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<CompareUnderwritingOptionsRequest, CompareUnderwritingOptionsResponse>() {
            @Override
            public CompareUnderwritingOptionsResponse apply(CompareUnderwritingOptionsRequest input) {
                return insuranceUnderwritingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareUnderwritingOptions(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "WatchPolicies", request.getFilter().getQuery());
        return WatchPoliciesResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getSubscriber().getName())
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(reply0.getUnderwriting().getCustomer().getDisplayName())
                                .build())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .build();
    }
}
