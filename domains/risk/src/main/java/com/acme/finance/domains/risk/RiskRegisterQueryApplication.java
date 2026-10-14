package com.acme.finance.domains.risk;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventMeasurement;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.CreateAuditEventRequest;
import com.acme.audit.v1.CreateAuditEventResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.BrowseAccessPoliciesRequest;
import com.acme.authorization.v1.BrowseAccessPoliciesResponse;
import com.acme.banking.v1.ExplainMandateCalculationRequest;
import com.acme.banking.v1.ExplainMandateCalculationResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LintPostingBatchesRequest;
import com.acme.ledger.v1.LintPostingBatchesResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.ScorePostingBatchRequest;
import com.acme.ledger.v1.ScorePostingBatchResponse;
import com.acme.risk.v1.BrowseRiskRegistersRequest;
import com.acme.risk.v1.BrowseRiskRegistersResponse;
import com.acme.risk.v1.CalculateExposureRequest;
import com.acme.risk.v1.CalculateExposureResponse;
import com.acme.risk.v1.CompareExposureOptionsRequest;
import com.acme.risk.v1.CompareExposureOptionsResponse;
import com.acme.risk.v1.EstimateExposureRequest;
import com.acme.risk.v1.EstimateExposureResponse;
import com.acme.risk.v1.ExportRiskRegistersRequest;
import com.acme.risk.v1.ExportRiskRegistersResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureCalculationServiceGrpc;
import com.acme.risk.v1.FindRiskRegistersRequest;
import com.acme.risk.v1.FindRiskRegistersResponse;
import com.acme.risk.v1.LookupRiskRegisterRequest;
import com.acme.risk.v1.LookupRiskRegisterResponse;
import com.acme.risk.v1.PeekRiskRegisterRequest;
import com.acme.risk.v1.PeekRiskRegisterResponse;
import com.acme.risk.v1.ResolveRiskRegistersRequest;
import com.acme.risk.v1.ResolveRiskRegistersResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.SuggestRiskRegistersRequest;
import com.acme.risk.v1.SuggestRiskRegistersResponse;
import com.acme.risk.v1.SummarizeRiskRegistersRequest;
import com.acme.risk.v1.SummarizeRiskRegistersResponse;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about risk registers, for screens and reports. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class RiskRegisterQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(RiskRegisterQueryApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking;

    private final RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceFutureStub auditRetentionPolicyNotificationServiceFuture;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking;

    private final RiskRegisterQueryRequestMapper mapper;

    private final RiskRepository repository;

    private final ExposureCalculationServiceGrpc.ExposureCalculationServiceBlockingStub riskExposureCalculationServiceBlocking;

    public RiskRegisterQueryApplication(
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking,
            RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceFutureStub auditRetentionPolicyNotificationServiceFuture,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking,
            ChannelRegistry channels,
            PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking,
            RiskRegisterQueryRequestMapper mapper,
            RiskRepository repository,
            ExposureCalculationServiceGrpc.ExposureCalculationServiceBlockingStub riskExposureCalculationServiceBlocking) {
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditAuditEventServiceBlocking = auditAuditEventServiceBlocking;
        this.auditRetentionPolicyNotificationServiceFuture = auditRetentionPolicyNotificationServiceFuture;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.bankingMandateCalculationServiceBlocking = bankingMandateCalculationServiceBlocking;
        this.channels = channels;
        this.ledgerPostingBatchValidationServiceBlocking = ledgerPostingBatchValidationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskExposureCalculationServiceBlocking = riskExposureCalculationServiceBlocking;
    }

    /**
     * Returns a risk register.
     */
    public LookupRiskRegisterResponse lookupRiskRegister(LookupRiskRegisterRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupRiskRegister")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskRegister", request.getRiskRegisterId(), "LookupRiskRegister");

        var outbound0 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector0 = new StreamCollector<>(BatchCreateAuditEventsResponse.getDefaultInstance());
        var input0 = auditAuditEventServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateAuditEvents(collector0);
        input0.onNext(outbound0);
        input0.onCompleted();
        var reply0 = collector0.await();

        var outbound1 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input1 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();
        return LookupRiskRegisterResponse.newBuilder()
                .setRiskRegister(RiskRegister.newBuilder()
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }

    /**
     * Lists risk registers a page at a time.
     */
    public BrowseRiskRegistersResponse browseRiskRegisters(BrowseRiskRegistersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseRiskRegisters")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskRegister", request.getContext().getTraceId(), "BrowseRiskRegisters");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseRiskRegisters", request.getPage().getPageToken());
        return BrowseRiskRegistersResponse.getDefaultInstance();
    }

    /**
     * Lists risk registers a page at a time.
     */
    public FindRiskRegistersResponse findRiskRegisters(FindRiskRegistersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindRiskRegisters")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskRegister", request.getContext().getTraceId(), "FindRiskRegisters");

        var outbound0 = ScorePostingBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        RpcPort<ScorePostingBatchRequest, ScorePostingBatchResponse> port0 = RpcPort.adapt(ledgerPostingBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scorePostingBatch);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindRiskRegisters", request.getPage().getPageToken());
        return FindRiskRegistersResponse.newBuilder()
                .addRiskRegisters(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setEtag(reply0.getPostingBatch().getAudit().getEtag())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
    }

    /**
     * Returns several risk registers at once.
     */
    public ResolveRiskRegistersResponse resolveRiskRegisters(ResolveRiskRegistersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveRiskRegisters")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskRegister", request.getContext().getTraceId(), "ResolveRiskRegisters");

        var outbound0 = EstimateExposureRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = riskExposureCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateExposure(outbound0);
        return ResolveRiskRegistersResponse.newBuilder()
                .addRiskRegisters(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setUpdatedAt(reply0.getExposure().getAudit().getUpdatedAt())
                                        .build())
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedBy(reply0.getExposure().getAudit().getCreatedBy())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
    }

    /**
     * Streams every matching risk register.
     */
    public ExportRiskRegistersResponse exportRiskRegisters(ExportRiskRegistersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportRiskRegisters")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskRegister", request.getContext().getTraceId(), "ExportRiskRegisters");

        var outbound0 = RpcSupport.unpack(Any.pack(CreateAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .putAllAttributes(request.getFilter().getLabelsMap())
                        .setDetail(Any.pack(request.getContext()))
                        .addMeasurements(AuditEventMeasurement.newBuilder()
                                .setObservedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setOccurredAt(request.getContext().getDeadline())
                        .setSource(request.getFilter().getOwnerId())
                        .build())
                .setContext(request.getContext())
                .build()), CreateAuditEventRequest.class);
        var reply0 = auditAuditEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createAuditEvent(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportRiskRegisters", request.getFilter().getQuery());
        return ExportRiskRegistersResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .setManager(UserRef.newBuilder()
                                        .setDisplayName(reply0.getAuditEvent().getSubject().getDisplayName())
                                        .build())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
    }

    /**
     * Summarizes a risk register.
     */
    public SummarizeRiskRegistersResponse summarizeRiskRegisters(SummarizeRiskRegistersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeRiskRegisters")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskRegister", request.getRiskRegisterId(), "SummarizeRiskRegisters");

        var outbound0 = RpcSupport.unpack(Any.pack(CalculateExposureRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build()), CalculateExposureRequest.class);
        var reply0 = riskExposureCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateExposure(outbound0);

        var outbound1 = CompareExposureOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getExposure().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.risk.v1.ExposureCalculationService");
        var selected1 = ExposureCalculationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.compareExposureOptions(outbound1);

        var outbound2 = LintPostingBatchesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintPostingBatchesRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply1.getExposure().getHolder().getEmail())
                                .setPhone(reply0.getExposure().getContact().getPhone())
                                .build())
                        .build())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getExposure().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        Function<LintPostingBatchesRequest, LintPostingBatchesResponse> operation2 = ledgerPostingBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::lintPostingBatches;
        var reply2 = operation2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeRiskRegisters", request.getRiskRegisterId());
        return SummarizeRiskRegistersResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply2.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists risk registers a page at a time.
     */
    public SuggestRiskRegistersResponse suggestRiskRegisters(SuggestRiskRegistersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestRiskRegisters")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskRegister", request.getContext().getTraceId(), "SuggestRiskRegisters");

        var outbound0 = mapper.suggestRiskRegistersStage0(request);
        var reply0 = authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseAccessPolicies(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestRiskRegisters", request.getPage().getPageToken());
        return SuggestRiskRegistersResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns a risk register.
     */
    public PeekRiskRegisterResponse peekRiskRegister(PeekRiskRegisterRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekRiskRegister")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskRegister", request.getRiskRegisterId(), "PeekRiskRegister");

        var outbound0 = SubscribeRetentionPolicyUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeRetentionPolicyUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllVariables(request.getContext().getBaggageMap())
                        .build())
                .build();
        var pending0 = auditRetentionPolicyNotificationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeRetentionPolicyUpdates(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = ExplainMandateCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getRetentionPolicy().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getRetentionPolicy().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getRetentionPolicy().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getRetentionPolicy().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        RpcPort<ExplainMandateCalculationRequest, ExplainMandateCalculationResponse> port1 = RpcPort.adapt(bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainMandateCalculation);
        var reply1 = port1.exchange(outbound1);
        return PeekRiskRegisterResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedAt(reply1.getMandate().getAudit().getUpdatedAt())
                                                .build())
                                        .setName(reply0.getRetentionPolicy().getName())
                                        .build())
                                .build())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }
}
