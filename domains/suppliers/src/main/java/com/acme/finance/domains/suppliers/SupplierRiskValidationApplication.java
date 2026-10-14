package com.acme.finance.domains.suppliers;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventMeasurement;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrailQueryServiceGrpc;
import com.acme.audit.v1.BrowseAuditTrailsRequest;
import com.acme.audit.v1.BrowseAuditTrailsResponse;
import com.acme.audit.v1.CreateAuditEventRequest;
import com.acme.audit.v1.CreateAuditEventResponse;
import com.acme.audit.v1.ResolveAuditTrailsRequest;
import com.acme.audit.v1.ResolveAuditTrailsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.BatchCreateComplianceCasesRequest;
import com.acme.compliance.v1.BatchCreateComplianceCasesResponse;
import com.acme.compliance.v1.ComplianceCaseServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
import com.acme.featureflags.v1.SuggestFeatureFlagsRequest;
import com.acme.featureflags.v1.SuggestFeatureFlagsResponse;
import com.acme.featureflags.v1.SummarizeFeatureFlagsRequest;
import com.acme.featureflags.v1.SummarizeFeatureFlagsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.insurance.v1.PreviewClaimRequest;
import com.acme.insurance.v1.PreviewClaimResponse;
import com.acme.insurance.v1.ValidatePremiumRequest;
import com.acme.insurance.v1.ValidatePremiumResponse;
import com.acme.ledger.v1.ExplainJournalEntryDecisionRequest;
import com.acme.ledger.v1.ExplainJournalEntryDecisionResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.LintJournalEntriesRequest;
import com.acme.ledger.v1.LintJournalEntriesResponse;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityResponse;
import com.acme.suppliers.v1.CheckSupplierRiskEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRiskEligibilityResponse;
import com.acme.suppliers.v1.ExplainSupplierRiskDecisionRequest;
import com.acme.suppliers.v1.ExplainSupplierRiskDecisionResponse;
import com.acme.suppliers.v1.GetSupplierRiskRulesRequest;
import com.acme.suppliers.v1.GetSupplierRiskRulesResponse;
import com.acme.suppliers.v1.LintSupplierRatingsRequest;
import com.acme.suppliers.v1.LintSupplierRatingsResponse;
import com.acme.suppliers.v1.LintSupplierRisksRequest;
import com.acme.suppliers.v1.LintSupplierRisksResponse;
import com.acme.suppliers.v1.NormalizeSupplierRatingRequest;
import com.acme.suppliers.v1.NormalizeSupplierRatingResponse;
import com.acme.suppliers.v1.NormalizeSupplierRiskRequest;
import com.acme.suppliers.v1.NormalizeSupplierRiskResponse;
import com.acme.suppliers.v1.PreviewSupplierRiskRequest;
import com.acme.suppliers.v1.PreviewSupplierRiskResponse;
import com.acme.suppliers.v1.ScoreSupplierRatingRequest;
import com.acme.suppliers.v1.ScoreSupplierRatingResponse;
import com.acme.suppliers.v1.ScoreSupplierRiskRequest;
import com.acme.suppliers.v1.ScoreSupplierRiskResponse;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskComment;
import com.acme.suppliers.v1.SupplierRiskSla;
import com.acme.suppliers.v1.ValidateSupplierRiskRequest;
import com.acme.suppliers.v1.ValidateSupplierRiskResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Checks supplier risks against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class SupplierRiskValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(SupplierRiskValidationApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking;

    private final AuditTrailQueryServiceGrpc.AuditTrailQueryServiceBlockingStub auditAuditTrailQueryServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ChannelRegistry channels;

    private final ComplianceCaseServiceGrpc.ComplianceCaseServiceStub complianceComplianceCaseServiceAsync;

    private final FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking;

    private final ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking;

    private final PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking;

    private final JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking;

    private final SuppliersRepository repository;

    private final Executor rpcExecutor;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking;

    public SupplierRiskValidationApplication(
            AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking,
            AuditTrailQueryServiceGrpc.AuditTrailQueryServiceBlockingStub auditAuditTrailQueryServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ChannelRegistry channels,
            ComplianceCaseServiceGrpc.ComplianceCaseServiceStub complianceComplianceCaseServiceAsync,
            FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking,
            ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking,
            PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking,
            JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking,
            SuppliersRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking) {
        this.auditAuditEventServiceBlocking = auditAuditEventServiceBlocking;
        this.auditAuditTrailQueryServiceBlocking = auditAuditTrailQueryServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.channels = channels;
        this.complianceComplianceCaseServiceAsync = complianceComplianceCaseServiceAsync;
        this.featureflagsFeatureFlagQueryServiceBlocking = featureflagsFeatureFlagQueryServiceBlocking;
        this.insuranceClaimValidationServiceBlocking = insuranceClaimValidationServiceBlocking;
        this.insurancePremiumValidationServiceBlocking = insurancePremiumValidationServiceBlocking;
        this.ledgerJournalEntryValidationServiceBlocking = ledgerJournalEntryValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierRatingValidationServiceBlocking = suppliersSupplierRatingValidationServiceBlocking;
    }

    /**
     * Validates a supplier risk.
     */
    public ValidateSupplierRiskResponse validateSupplierRisk(ValidateSupplierRiskRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateSupplierRisk")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRisk(request.getContext().getTenantId(), request.getSupplierRisk());

        var outbound0 = JsonCodec.roundTrip(LintSupplierRatingsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintSupplierRatingsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getSupplierRisk().getRequester().getPhone())
                                .build())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getSupplierRisk().getAssignee().getEmail())
                                .build())
                        .build())
                .build(), LintSupplierRatingsRequest.class);
        var reply0 = suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintSupplierRatings(outbound0);

        var outbound1 = BatchCreateComplianceCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateComplianceCasesRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getContact())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateComplianceCasesResponse.getDefaultInstance());
        var input1 = complianceComplianceCaseServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateComplianceCases(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getSupplierRisk().getAudit().getCreatedAt())
                                .setCreatedBy(request.getSupplierRisk().getAudit().getCreatedBy())
                                .setEtag(request.getSupplierRisk().getAudit().getEtag())
                                .setUpdatedAt(request.getSupplierRisk().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input2 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getSupplierRisk().getSupplierRiskId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateSupplierRisk", request.getSupplierRisk().getSupplierRiskId());
        return ValidateSupplierRiskResponse.newBuilder()
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setSla(SupplierRiskSla.newBuilder()
                                .setDueAt(reply0.getSupplierRating().getDueAt())
                                .build())
                        .setStatus(request.getSupplierRisk().getStatus())
                        .setSupplierRiskId(request.getSupplierRiskId())
                        .build())
                .build();
    }

    /**
     * Previews a supplier risk.
     */
    public PreviewSupplierRiskResponse previewSupplierRisk(PreviewSupplierRiskRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewSupplierRisk")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRisk(request.getContext().getTenantId(), request.getSupplierRisk());

        var outbound0 = BeanCopies.copy(CheckSupplierRatingEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckSupplierRatingEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .build(), CheckSupplierRatingEligibilityRequest.class);
        var reply0 = suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkSupplierRatingEligibility(outbound0);

        var outbound1 = LintJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getSupplierRating().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getSupplierRating().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        RpcPort<LintJournalEntriesRequest, LintJournalEntriesResponse> port1 = RpcPort.adapt(ledgerJournalEntryValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::lintJournalEntries);
        var reply1 = port1.exchange(outbound1);

        var futures2 = request.getSupplierRisk().getCommentsList().stream()
                .map(element -> NormalizeSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeSupplierRating(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? NormalizeSupplierRatingResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getSupplierRisk().getSupplierRiskId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewSupplierRisk", request.getSupplierRisk().getSupplierRiskId());
        return PreviewSupplierRiskResponse.newBuilder()
                .setOutcome(PreviewSupplierRiskResponse.Outcome.newBuilder()
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getJournalEntry().getAudit().getUpdatedAt())
                                .build())
                        .putAllCustomFields(request.getSupplierRisk().getCustomFieldsMap())
                        .setSupplierRiskId(request.getSupplierRiskId())
                        .build())
                .build();
    }

    /**
     * Checks a supplier risk.
     */
    public CheckSupplierRiskEligibilityResponse checkSupplierRiskEligibility(CheckSupplierRiskEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckSupplierRiskEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRisk(request.getContext().getTenantId(), request.getSupplierRisk());

        var outbound0 = CheckSupplierRatingEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckSupplierRatingEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getSupplierRisk().getAudit().getCreatedBy())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getSupplierRisk().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.suppliers.v1.SupplierRatingValidationService");
        var serviceName0 = "acme.suppliers.v1" + "." + "SupplierRatingValidationService";
        var methodName0 = "Che" + "ckSupplierRatingEligibility";
        var descriptor0 = MethodDescriptor.<CheckSupplierRatingEligibilityRequest, CheckSupplierRatingEligibilityResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(CheckSupplierRatingEligibilityRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CheckSupplierRatingEligibilityResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getSupplierRisk().getSupplierRiskId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckSupplierRiskEligibility", request.getSupplierRisk().getSupplierRiskId());
        return CheckSupplierRiskEligibilityResponse.newBuilder()
                .setOutcome(CheckSupplierRiskEligibilityResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .addComments(SupplierRiskComment.newBuilder()
                                .setCreatedAt(reply0.getSupplierRating().getAudit().getCreatedAt())
                                .build())
                        .setRequester(request.getSupplierRisk().getRequester())
                        .setSupplierRiskId(request.getSupplierRiskId())
                        .build())
                .build();
    }

    /**
     * Normalizes a supplier risk.
     */
    public NormalizeSupplierRiskResponse normalizeSupplierRisk(NormalizeSupplierRiskRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeSupplierRisk")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRisk(request.getContext().getTenantId(), request.getSupplierRisk());

        var builder0 = ExplainJournalEntryDecisionRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(ExplainJournalEntryDecisionRequest.Details.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setEmail(request.getSupplierRisk().getAssignee().getEmail())
                        .setPhone(request.getDetails().getContact().getPhone())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = ledgerJournalEntryValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainJournalEntryDecision(outbound0);

        var outbound1 = SuggestFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestFeatureFlagsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getJournalEntry().getIssuedAt())
                        .setRegion(reply0.getJournalEntry().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> featureflagsFeatureFlagQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestFeatureFlags(value)).orElseThrow();

        var outbound2 = BrowseAuditTrailsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseAuditTrailsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getJournalEntry().getAudit().getUpdatedAt())
                        .setOwnerId(request.getSupplierRisk().getSla().getPolicyId())
                        .setRegion(reply0.getJournalEntry().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> auditAuditTrailQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseAuditTrails(value));

        List<PreviewClaimResponse> results3 = request.getSupplierRisk().getCommentsList().stream()
                .map(element -> PreviewClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .addComments(ClaimComment.newBuilder()
                                .setAuthorEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .flatMap(value -> Stream.of(insuranceClaimValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewClaim(value)))
                .collect(Collectors.toList());
        var reply3 = results3.isEmpty() ? PreviewClaimResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getSupplierRisk().getSupplierRiskId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeSupplierRisk", request.getSupplierRisk().getSupplierRiskId());
        return NormalizeSupplierRiskResponse.newBuilder()
                .setOutcome(NormalizeSupplierRiskResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getJournalEntry().getAudit().getUpdatedBy())
                                .build())
                        .setSubject(request.getSupplierRisk().getSubject())
                        .setSupplierRiskId(request.getSupplierRiskId())
                        .build())
                .build();
    }

    /**
     * Explains a supplier risk.
     */
    public ExplainSupplierRiskDecisionResponse explainSupplierRiskDecision(ExplainSupplierRiskDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainSupplierRiskDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRisk(request.getContext().getTenantId(), request.getSupplierRisk());

        var outbound0 = LintSupplierRatingsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintSupplierRatingsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .setPreferredChannel(request.getSupplierRisk().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getSupplierRisk().getAudit().getCreatedBy())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintSupplierRatings(outbound0);

        var futures1 = request.getSupplierRisk().getCommentsList().stream()
                .map(element -> CreateAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .addMeasurements(AuditEventMeasurement.newBuilder()
                                .setObservedAt(element.getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> auditAuditEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createAuditEvent(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? CreateAuditEventResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getSupplierRisk().getSupplierRiskId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainSupplierRiskDecision", request.getSupplierRisk().getSupplierRiskId());
        return ExplainSupplierRiskDecisionResponse.newBuilder()
                .setOutcome(ExplainSupplierRiskDecisionResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .build())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getSupplierRating().getCounterparty().getDisplayName())
                                .build())
                        .setRequester(request.getSupplierRisk().getRequester())
                        .setSupplierRiskId(request.getSupplierRiskId())
                        .build())
                .build();
    }

    /**
     * Scores a supplier risk.
     */
    public ScoreSupplierRiskResponse scoreSupplierRisk(ScoreSupplierRiskRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreSupplierRisk")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRisk(request.getContext().getTenantId(), request.getSupplierRisk());

        var results0 = new ArrayList<ValidatePremiumResponse>();
        for (var element : request.getSupplierRisk().getCommentsList()) {
            ValidatePremiumRequest itemRequest = ValidatePremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .build())
                .build();
            results0.add(insurancePremiumValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePremium(itemRequest));
        }
        var reply0 = results0.isEmpty() ? ValidatePremiumResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = SummarizeFeatureFlagsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeFeatureFlagsRequest.Details.newBuilder()
                        .putAllFilters(request.getSupplierRisk().getCustomFieldsMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var adapter1 = new Function<SummarizeFeatureFlagsRequest, SummarizeFeatureFlagsResponse>() {
            @Override
            public SummarizeFeatureFlagsResponse apply(SummarizeFeatureFlagsRequest input) {
                return featureflagsFeatureFlagQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeFeatureFlags(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getSupplierRisk().getSupplierRiskId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreSupplierRisk", request.getSupplierRisk().getSupplierRiskId());
        return ScoreSupplierRiskResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .putAllCustomFields(request.getSupplierRisk().getCustomFieldsMap())
                        .setSupplierRiskId(request.getSupplierRiskId())
                        .build())
                .build();
    }

    /**
     * Lints a supplier risk.
     */
    public LintSupplierRisksResponse lintSupplierRisks(LintSupplierRisksRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintSupplierRisks")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRisk(request.getContext().getTenantId(), request.getSupplierRisk());

        var outbound0 = BeanCopies.copy(ResolveAuditTrailsRequest.newBuilder()
                .setContext(request.getContext())
                .build(), ResolveAuditTrailsRequest.class);
        var reply0 = auditAuditTrailQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveAuditTrails(outbound0);

        var builder1 = NormalizeSupplierRatingRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setDetails(NormalizeSupplierRatingRequest.Details.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setName(request.getSupplierRisk().getRequester().getName())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeSupplierRating(outbound1);

        var outbound2 = ScoreSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getSupplierRating().getAudit().getCreatedAt())
                                .setCreatedBy(request.getSupplierRisk().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.suppliers.v1.SupplierRatingValidationService");
        var serviceName2 = "acme.suppliers.v1" + "." + "SupplierRatingValidationService";
        var methodName2 = "Sco" + "reSupplierRating";
        var descriptor2 = MethodDescriptor.<ScoreSupplierRatingRequest, ScoreSupplierRatingResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(ScoreSupplierRatingRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ScoreSupplierRatingResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);
        log.atDebug().addKeyValue("auditValue", request.getSupplierRisk().getSupplierRiskId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintSupplierRisks", request.getSupplierRisk().getSupplierRiskId());
        return LintSupplierRisksResponse.newBuilder()
                .setOutcome(LintSupplierRisksResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply1.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setSla(SupplierRiskSla.newBuilder()
                                .setDueAt(reply2.getSupplierRating().getDueAt())
                                .build())
                        .setSubject(request.getSupplierRisk().getSubject())
                        .setSupplierRiskId(request.getSupplierRiskId())
                        .build())
                .build();
    }

    /**
     * Returns a supplier risk.
     */
    public GetSupplierRiskRulesResponse getSupplierRiskRules(GetSupplierRiskRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetSupplierRiskRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "SupplierRisk", request.getSupplierRiskId(), "GetSupplierRiskRules");
        return GetSupplierRiskRulesResponse.newBuilder()
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setSupplierRiskId(request.getSupplierRiskId())
                        .build())
                .build();
    }
}
