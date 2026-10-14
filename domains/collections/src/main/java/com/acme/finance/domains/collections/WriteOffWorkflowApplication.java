package com.acme.finance.domains.collections;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.RevertAuditEventRequest;
import com.acme.audit.v1.RevertAuditEventResponse;
import com.acme.audit.v1.SearchAuditEventsRequest;
import com.acme.audit.v1.SearchAuditEventsResponse;
import com.acme.audit.v1.StreamRetentionPolicyExportRequest;
import com.acme.audit.v1.StreamRetentionPolicyExportResponse;
import com.acme.collections.v1.AdvanceWriteOffWorkflowRequest;
import com.acme.collections.v1.AdvanceWriteOffWorkflowResponse;
import com.acme.collections.v1.CancelCollectionCaseWorkflowRequest;
import com.acme.collections.v1.CancelCollectionCaseWorkflowResponse;
import com.acme.collections.v1.CancelPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.CancelPaymentPromiseWorkflowResponse;
import com.acme.collections.v1.CancelWriteOffWorkflowRequest;
import com.acme.collections.v1.CancelWriteOffWorkflowResponse;
import com.acme.collections.v1.CheckWriteOffEligibilityRequest;
import com.acme.collections.v1.CheckWriteOffEligibilityResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseRef;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.collections.v1.CommentPaymentPromiseApprovalRequest;
import com.acme.collections.v1.CommentPaymentPromiseApprovalResponse;
import com.acme.collections.v1.InspectWriteOffWorkflowRequest;
import com.acme.collections.v1.InspectWriteOffWorkflowResponse;
import com.acme.collections.v1.ListWriteOffWorkflowsRequest;
import com.acme.collections.v1.ListWriteOffWorkflowsResponse;
import com.acme.collections.v1.PauseWriteOffWorkflowRequest;
import com.acme.collections.v1.PauseWriteOffWorkflowResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.collections.v1.PaymentPromiseRef;
import com.acme.collections.v1.PreviewWriteOffRequest;
import com.acme.collections.v1.PreviewWriteOffResponse;
import com.acme.collections.v1.RejectPaymentPromiseRequest;
import com.acme.collections.v1.RejectPaymentPromiseResponse;
import com.acme.collections.v1.ResumeWriteOffWorkflowRequest;
import com.acme.collections.v1.ResumeWriteOffWorkflowResponse;
import com.acme.collections.v1.RetryWriteOffWorkflowRequest;
import com.acme.collections.v1.RetryWriteOffWorkflowResponse;
import com.acme.collections.v1.ScoreWriteOffRequest;
import com.acme.collections.v1.ScoreWriteOffResponse;
import com.acme.collections.v1.SignalWriteOffWorkflowRequest;
import com.acme.collections.v1.SignalWriteOffWorkflowResponse;
import com.acme.collections.v1.StartWriteOffWorkflowRequest;
import com.acme.collections.v1.StartWriteOffWorkflowResponse;
import com.acme.collections.v1.UpdateCollectionCaseRequest;
import com.acme.collections.v1.UpdateCollectionCaseResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffCardSettlement;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ApproveClaimRequest;
import com.acme.insurance.v1.ApproveClaimResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.RecallPolicyRequest;
import com.acme.insurance.v1.RecallPolicyResponse;
import com.acme.insurance.v1.ShareCoverageReportRequest;
import com.acme.insurance.v1.ShareCoverageReportResponse;
import com.acme.insurance.v1.StreamUnderwritingMetricsRequest;
import com.acme.insurance.v1.StreamUnderwritingMetricsResponse;
import com.acme.insurance.v1.UnderwritingReportingServiceGrpc;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.NormalizeJournalEntryRequest;
import com.acme.ledger.v1.NormalizeJournalEntryResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
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
 * Runs the long-lived workflow that takes a write off from start to finish. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class WriteOffWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(WriteOffWorkflowApplication.class);

    private final AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking;

    private final AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking;

    private final RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking;

    private final ChannelRegistry channels;

    private final CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceBlockingStub collectionsCollectionCaseWorkflowServiceBlocking;

    private final PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking;

    private final WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking;

    private final UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking;

    private final CollectionsRepository repository;

    public WriteOffWorkflowApplication(
            AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking,
            AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking,
            RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking,
            ChannelRegistry channels,
            CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceBlockingStub collectionsCollectionCaseWorkflowServiceBlocking,
            PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking,
            WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking,
            ApplicationEventPublisher events,
            PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking,
            UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking,
            CollectionsRepository repository) {
        this.auditAuditEventHistoryServiceBlocking = auditAuditEventHistoryServiceBlocking;
        this.auditAuditEventServiceBlocking = auditAuditEventServiceBlocking;
        this.auditRetentionPolicyExportServiceBlocking = auditRetentionPolicyExportServiceBlocking;
        this.channels = channels;
        this.collectionsCollectionCaseWorkflowServiceBlocking = collectionsCollectionCaseWorkflowServiceBlocking;
        this.collectionsPaymentPromiseApprovalServiceBlocking = collectionsPaymentPromiseApprovalServiceBlocking;
        this.collectionsWriteOffValidationServiceBlocking = collectionsWriteOffValidationServiceBlocking;
        this.events = events;
        this.insurancePolicyApprovalServiceBlocking = insurancePolicyApprovalServiceBlocking;
        this.insuranceUnderwritingReportingServiceBlocking = insuranceUnderwritingReportingServiceBlocking;
        this.repository = repository;
    }

    public record ResumeWriteOffWorkflowStage3Requested(ShareCoverageReportRequest request, CompletableFuture<ShareCoverageReportResponse> completion, io.grpc.Context context) {}

    public record InspectWriteOffWorkflowStage1Requested(CancelPaymentPromiseWorkflowRequest request, CompletableFuture<CancelPaymentPromiseWorkflowResponse> completion, io.grpc.Context context) {}

    /**
     * Starts a write off.
     */
    public StartWriteOffWorkflowResponse startWriteOffWorkflow(StartWriteOffWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartWriteOffWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveWriteOff(request.getContext().getTenantId(), request.getWriteOff());

        var outbound0 = RejectPaymentPromiseRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getWriteOff().getAudit().getUpdatedBy())
                                .build())
                        .addParticipants(PaymentPromiseParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPhone(request.getDetails().getNotify().getPhone())
                                        .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(collectionsPaymentPromiseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectPaymentPromise(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = CancelCollectionCaseWorkflowRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getPaymentPromise().getOrganizer().getDisplayName())
                                .setEmail(reply0.getPaymentPromise().getOrganizer().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getPaymentPromise().getAudit().getEtag())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CancelCollectionCaseWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<CancelCollectionCaseWorkflowRequest, CancelCollectionCaseWorkflowResponse>() {
            @Override
            public CancelCollectionCaseWorkflowResponse apply(CancelCollectionCaseWorkflowRequest input) {
                return collectionsCollectionCaseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelCollectionCaseWorkflow(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getWriteOff().getWriteOffId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartWriteOffWorkflow", request.getWriteOff().getWriteOffId());
        return StartWriteOffWorkflowResponse.newBuilder()
                .setOutcome(StartWriteOffWorkflowResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setIssuedAt(request.getWriteOff().getIssuedAt())
                        .setWriteOffId(request.getWriteOffId())
                        .build())
                .build();
    }

    /**
     * Advances a write off.
     */
    public AdvanceWriteOffWorkflowResponse advanceWriteOffWorkflow(AdvanceWriteOffWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceWriteOffWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveWriteOff(request.getContext().getTenantId(), request.getWriteOff());

        var outbound0 = SearchAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getWriteOff().getAudit().getUpdatedAt())
                        .putAllLabels(request.getDetails().getParametersMap())
                        .setOwnerId(request.getWriteOffId())
                        .setRegion(request.getWriteOff().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply0 = RpcSupport.retryRead(() -> auditAuditEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchAuditEvents(outbound0));

        var outbound1 = ApproveClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getWriteOff().getAudit().getUpdatedBy())
                                .build())
                        .addComments(ClaimComment.newBuilder()
                                .setCreatedAt(request.getWriteOff().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel1 = channels.lookup("acme.insurance.v1.ClaimApprovalService");
        var selected1 = ClaimApprovalServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.approveClaim(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getWriteOff().getWriteOffId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceWriteOffWorkflow", request.getWriteOff().getWriteOffId());
        return AdvanceWriteOffWorkflowResponse.newBuilder()
                .setOutcome(AdvanceWriteOffWorkflowResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setTotal(request.getWriteOff().getTotal())
                        .setWriteOffId(request.getWriteOffId())
                        .build())
                .build();
    }

    /**
     * Cancels a write off.
     */
    public CancelWriteOffWorkflowResponse cancelWriteOffWorkflow(CancelWriteOffWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelWriteOffWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveWriteOff(request.getContext().getTenantId(), request.getWriteOff());

        var outbound0 = CommentPaymentPromiseApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRecipient(request.getWriteOff().getBillingAddress().getRecipient())
                                .build())
                        .addParticipants(PaymentPromiseParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getNotify().getEmail())
                                        .setName(request.getDetails().getNotify().getName())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = collectionsPaymentPromiseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentPaymentPromiseApproval(outbound0);

        var outbound1 = NormalizeJournalEntryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeJournalEntryRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getPaymentPromise().getOrganizer().getEmail())
                                .setName(request.getDetails().getNotify().getName())
                                .build())
                        .build())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getWriteOff().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.ledger.v1.JournalEntryValidationService");
        var serviceName1 = "acme.ledger.v1" + "." + "JournalEntryValidationService";
        var methodName1 = "Nor" + "malizeJournalEntry";
        var descriptor1 = MethodDescriptor.<NormalizeJournalEntryRequest, NormalizeJournalEntryResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(NormalizeJournalEntryRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(NormalizeJournalEntryResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getWriteOff().getWriteOffId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelWriteOffWorkflow", request.getWriteOff().getWriteOffId());
        return CancelWriteOffWorkflowResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .putAllMetadata(reply1.getJournalEntry().getMetadataMap())
                        .setNumber(request.getWriteOff().getNumber())
                        .setWriteOffId(request.getWriteOffId())
                        .build())
                .build();
    }

    /**
     * Retrys a write off.
     */
    public RetryWriteOffWorkflowResponse retryWriteOffWorkflow(RetryWriteOffWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryWriteOffWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveWriteOff(request.getContext().getTenantId(), request.getWriteOff());

        var outbound0 = UpdateCollectionCaseRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setCustomer(PaymentPromiseRef.newBuilder()
                                .setDisplayName(request.getWriteOff().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.collections.v1.CollectionCaseService");
        var serviceName0 = "acme.collections.v1" + "." + "CollectionCaseService";
        var methodName0 = "Upd" + "ateCollectionCase";
        var descriptor0 = MethodDescriptor.<UpdateCollectionCaseRequest, UpdateCollectionCaseResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(UpdateCollectionCaseRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(UpdateCollectionCaseResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getWriteOff().getWriteOffId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryWriteOffWorkflow", request.getWriteOff().getWriteOffId());
        return RetryWriteOffWorkflowResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setCounterparty(CollectionCaseRef.newBuilder()
                                .setCollectionCaseId(reply0.getCollectionCase().getCollectionCaseId())
                                .build())
                        .setNotes(request.getWriteOff().getNotes())
                        .setWriteOffId(request.getWriteOffId())
                        .build())
                .build();
    }

    /**
     * Pauses a write off.
     */
    public PauseWriteOffWorkflowResponse pauseWriteOffWorkflow(PauseWriteOffWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseWriteOffWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveWriteOff(request.getContext().getTenantId(), request.getWriteOff());

        var outbound0 = RevertAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(request.getWriteOff().getCounterparty().getDisplayName())
                                .setNumber(request.getWriteOff().getNumber())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RevertAuditEventRequest.Details.newBuilder()
                        .setAuthorEmail(request.getDetails().getNotify().getEmail())
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        RpcPort<RevertAuditEventRequest, RevertAuditEventResponse> port0 = RpcPort.adapt(auditAuditEventHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::revertAuditEvent);
        var reply0 = port0.exchange(outbound0);

        var builder1 = ScoreWriteOffRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setWriteOff(WriteOff.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedBy(request.getWriteOff().getAudit().getCreatedBy())
                        .build())
                .setCard(WriteOffCardSettlement.newBuilder()
                        .setLast4(request.getWriteOff().getCard().getLast4())
                        .build())
                .setCounterparty(CollectionCaseRef.newBuilder()
                        .setDisplayName(reply0.getAuditEvent().getSubject().getDisplayName())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreWriteOff(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getWriteOff().getWriteOffId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseWriteOffWorkflow", request.getWriteOff().getWriteOffId());
        return PauseWriteOffWorkflowResponse.newBuilder()
                .setOutcome(PauseWriteOffWorkflowResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setStatus(request.getWriteOff().getStatus())
                        .setTotal(reply1.getWriteOff().getTotal())
                        .setWriteOffId(request.getWriteOffId())
                        .build())
                .build();
    }

    /**
     * Resumes a write off.
     */
    public ResumeWriteOffWorkflowResponse resumeWriteOffWorkflow(ResumeWriteOffWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeWriteOffWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveWriteOff(request.getContext().getTenantId(), request.getWriteOff());

        var outbound0 = CheckWriteOffEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getWriteOff().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getWriteOff().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkWriteOffEligibility(value));

        var outbound1 = JsonCodec.roundTrip(CommentPaymentPromiseApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(request.getWriteOff().getCounterparty().getDisplayName())
                                .build())
                        .addParticipants(PaymentPromiseParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getDetails().getNotify().getName())
                                        .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build(), CommentPaymentPromiseApprovalRequest.class);
        var reply1 = collectionsPaymentPromiseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentPaymentPromiseApproval(outbound1);

        var outbound2 = PreviewWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getWriteOff().getBillingAddress().getLocality())
                                .setRecipient(reply0.getWriteOff().getBillingAddress().getRecipient())
                                .setRegion(reply1.getPaymentPromise().getLocation().getRegion())
                                .build())
                        .setCard(WriteOffCardSettlement.newBuilder()
                                .setNetwork(reply0.getWriteOff().getCard().getNetwork())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.collections.v1.WriteOffValidationService");
        var serviceName2 = "acme.collections.v1" + "." + "WriteOffValidationService";
        var methodName2 = "Pre" + "viewWriteOff";
        var descriptor2 = MethodDescriptor.<PreviewWriteOffRequest, PreviewWriteOffResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(PreviewWriteOffRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(PreviewWriteOffResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = ShareCoverageReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareCoverageReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply1.getPaymentPromise().getOrganizer().getEmail())
                                .setName(request.getDetails().getNotify().getName())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var completion3 = new CompletableFuture<ShareCoverageReportResponse>();
        events.publishEvent(new ResumeWriteOffWorkflowStage3Requested(outbound3, completion3, io.grpc.Context.current()));
        var reply3 = completion3.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getWriteOff().getWriteOffId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeWriteOffWorkflow", request.getWriteOff().getWriteOffId());
        return ResumeWriteOffWorkflowResponse.newBuilder()
                .setOutcome(ResumeWriteOffWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getWriteOff().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(reply0.getWriteOff().getBillingAddress())
                        .putAllMetadata(request.getWriteOff().getMetadataMap())
                        .setWriteOffId(request.getWriteOffId())
                        .build())
                .build();
    }

    /**
     * Returns a write off.
     */
    public InspectWriteOffWorkflowResponse inspectWriteOffWorkflow(InspectWriteOffWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectWriteOffWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "WriteOff", request.getWriteOffId(), "InspectWriteOffWorkflow");

        var outbound0 = StreamUnderwritingMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamUnderwritingMetricsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .setOwnerId(request.getWriteOffId())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .build();
        var stream0 = insuranceUnderwritingReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamUnderwritingMetrics(outbound0);
        var reply0 = StreamUnderwritingMetricsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = CancelPaymentPromiseWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .addParticipants(PaymentPromiseParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getContext().getActor().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<CancelPaymentPromiseWorkflowResponse>();
        events.publishEvent(new InspectWriteOffWorkflowStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        return InspectWriteOffWorkflowResponse.newBuilder()
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getPaymentPromise().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getPaymentPromise().getLocation().getCountryCode())
                                .build())
                        .setWriteOffId(request.getWriteOffId())
                        .build())
                .build();
    }

    /**
     * Lists write offs a page at a time.
     */
    public ListWriteOffWorkflowsResponse listWriteOffWorkflows(ListWriteOffWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListWriteOffWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "WriteOff", request.getContext().getTraceId(), "ListWriteOffWorkflows");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListWriteOffWorkflows", request.getPage().getPageToken());
        return ListWriteOffWorkflowsResponse.getDefaultInstance();
    }

    /**
     * Signals a write off.
     */
    public SignalWriteOffWorkflowResponse signalWriteOffWorkflow(SignalWriteOffWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalWriteOffWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveWriteOff(request.getContext().getTenantId(), request.getWriteOff());

        var results0 = new ArrayList<StreamRetentionPolicyExportResponse>();
        for (var element : request.getWriteOff().getLinesList()) {
            StreamRetentionPolicyExportRequest itemRequest = StreamRetentionPolicyExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamRetentionPolicyExportRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getWriteOff().getAudit().getCreatedAt())
                        .putAllLabels(element.getAttributesMap())
                        .setOwnerId(element.getLineId())
                        .setRegion(request.getWriteOff().getBillingAddress().getRegion())
                        .build())
                .build();
            var responses = auditRetentionPolicyExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyExport(itemRequest);
            responses.forEachRemaining(results0::add);
        }
        var reply0 = results0.isEmpty() ? StreamRetentionPolicyExportResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = RecallPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setCard(PolicyCardFunding.newBuilder()
                                .setToken(switch (request.getWriteOff().getSettlementCase()) {
                                    case CARD -> request.getWriteOff().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotify().getName())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(request.getWriteOff().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = insurancePolicyApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallPolicy(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getWriteOff().getWriteOffId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalWriteOffWorkflow", request.getWriteOff().getWriteOffId());
        return SignalWriteOffWorkflowResponse.newBuilder()
                .setOutcome(SignalWriteOffWorkflowResponse.Outcome.newBuilder()
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setBillingAddress(request.getWriteOff().getBillingAddress())
                        .setWriteOffId(request.getWriteOffId())
                        .build())
                .build();
    }
}
