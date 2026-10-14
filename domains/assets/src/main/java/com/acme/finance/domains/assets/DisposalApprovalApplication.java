package com.acme.finance.domains.assets;

import com.acme.assets.v1.ApproveDisposalRequest;
import com.acme.assets.v1.ApproveDisposalResponse;
import com.acme.assets.v1.CommentDisposalApprovalRequest;
import com.acme.assets.v1.CommentDisposalApprovalResponse;
import com.acme.assets.v1.DelegateDisposalApprovalRequest;
import com.acme.assets.v1.DelegateDisposalApprovalResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationApprovalServiceGrpc;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalComment;
import com.acme.assets.v1.DisposalReportingServiceGrpc;
import com.acme.assets.v1.DisposalSla;
import com.acme.assets.v1.GetDisposalApprovalStatusRequest;
import com.acme.assets.v1.GetDisposalApprovalStatusResponse;
import com.acme.assets.v1.ListDisposalReportsRequest;
import com.acme.assets.v1.ListDisposalReportsResponse;
import com.acme.assets.v1.ListPendingDisposalsRequest;
import com.acme.assets.v1.ListPendingDisposalsResponse;
import com.acme.assets.v1.RecallDisposalRequest;
import com.acme.assets.v1.RecallDisposalResponse;
import com.acme.assets.v1.RejectDepreciationRequest;
import com.acme.assets.v1.RejectDepreciationResponse;
import com.acme.assets.v1.RejectDisposalRequest;
import com.acme.assets.v1.RejectDisposalResponse;
import com.acme.assets.v1.ScheduleDisposalReportRequest;
import com.acme.assets.v1.ScheduleDisposalReportResponse;
import com.acme.assets.v1.SubmitDisposalRequest;
import com.acme.assets.v1.SubmitDisposalResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.CompareAuditTrailVersionsRequest;
import com.acme.audit.v1.CompareAuditTrailVersionsResponse;
import com.acme.audit.v1.LintAuditEventsRequest;
import com.acme.audit.v1.LintAuditEventsResponse;
import com.acme.audit.v1.ReconcileAuditTrailsRequest;
import com.acme.audit.v1.ReconcileAuditTrailsResponse;
import com.acme.audit.v1.ResolveAuditTrailConflictRequest;
import com.acme.audit.v1.ResolveAuditTrailConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.collections.v1.PaymentPromiseRef;
import com.acme.collections.v1.RetryCollectionCaseWorkflowRequest;
import com.acme.collections.v1.RetryCollectionCaseWorkflowResponse;
import com.acme.collections.v1.SubmitWriteOffRequest;
import com.acme.collections.v1.SubmitWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.helpdesk.v1.CloneTicketRequest;
import com.acme.helpdesk.v1.CloneTicketResponse;
import com.acme.helpdesk.v1.Ticket;
import com.acme.helpdesk.v1.TicketServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageApprovalServiceGrpc;
import com.acme.insurance.v1.DelegateCoverageApprovalRequest;
import com.acme.insurance.v1.DelegateCoverageApprovalResponse;
import com.acme.insurance.v1.ImportUnderwritingsRequest;
import com.acme.insurance.v1.ImportUnderwritingsResponse;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.lending.v1.StreamRepaymentMetricsRequest;
import com.acme.lending.v1.StreamRepaymentMetricsResponse;
import com.google.protobuf.Any;
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
 * Routes disposals through approval, with escalation and delegation. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class DisposalApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(DisposalApprovalApplication.class);

    private final DepreciationApprovalServiceGrpc.DepreciationApprovalServiceBlockingStub assetsDepreciationApprovalServiceBlocking;

    private final DisposalReportingServiceGrpc.DisposalReportingServiceBlockingStub assetsDisposalReportingServiceBlocking;

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking;

    private final AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceBlockingStub collectionsCollectionCaseWorkflowServiceBlocking;

    private final WriteOffApprovalServiceGrpc.WriteOffApprovalServiceBlockingStub collectionsWriteOffApprovalServiceBlocking;

    private final TicketServiceGrpc.TicketServiceBlockingStub helpdeskTicketServiceBlocking;

    private final CoverageApprovalServiceGrpc.CoverageApprovalServiceBlockingStub insuranceCoverageApprovalServiceBlocking;

    private final UnderwritingImportServiceGrpc.UnderwritingImportServiceStub insuranceUnderwritingImportServiceAsync;

    private final RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking;

    private final AssetsRepository repository;

    private final Executor rpcExecutor;

    public DisposalApprovalApplication(
            DepreciationApprovalServiceGrpc.DepreciationApprovalServiceBlockingStub assetsDepreciationApprovalServiceBlocking,
            DisposalReportingServiceGrpc.DisposalReportingServiceBlockingStub assetsDisposalReportingServiceBlocking,
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking,
            AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceBlockingStub collectionsCollectionCaseWorkflowServiceBlocking,
            WriteOffApprovalServiceGrpc.WriteOffApprovalServiceBlockingStub collectionsWriteOffApprovalServiceBlocking,
            TicketServiceGrpc.TicketServiceBlockingStub helpdeskTicketServiceBlocking,
            CoverageApprovalServiceGrpc.CoverageApprovalServiceBlockingStub insuranceCoverageApprovalServiceBlocking,
            UnderwritingImportServiceGrpc.UnderwritingImportServiceStub insuranceUnderwritingImportServiceAsync,
            RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking,
            AssetsRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsDepreciationApprovalServiceBlocking = assetsDepreciationApprovalServiceBlocking;
        this.assetsDisposalReportingServiceBlocking = assetsDisposalReportingServiceBlocking;
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.auditAuditTrailHistoryServiceBlocking = auditAuditTrailHistoryServiceBlocking;
        this.auditAuditTrailSyncServiceBlocking = auditAuditTrailSyncServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.collectionsCollectionCaseWorkflowServiceBlocking = collectionsCollectionCaseWorkflowServiceBlocking;
        this.collectionsWriteOffApprovalServiceBlocking = collectionsWriteOffApprovalServiceBlocking;
        this.helpdeskTicketServiceBlocking = helpdeskTicketServiceBlocking;
        this.insuranceCoverageApprovalServiceBlocking = insuranceCoverageApprovalServiceBlocking;
        this.insuranceUnderwritingImportServiceAsync = insuranceUnderwritingImportServiceAsync;
        this.lendingRepaymentReportingServiceBlocking = lendingRepaymentReportingServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Submits a disposal.
     */
    public SubmitDisposalResponse submitDisposal(SubmitDisposalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitDisposal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDisposal(request.getContext().getTenantId(), request.getDisposal());

        var outbound0 = ReconcileAuditTrailsRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getDisposal().getAudit().getEtag())
                                .build())
                        .setCounterparty(RetentionPolicyRef.newBuilder()
                                .setDisplayName(switch (request.getDisposal().getOriginCase()) {
                                    case CUSTOMER -> request.getDisposal().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ReconcileAuditTrailsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply0 = auditAuditTrailSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileAuditTrails(outbound0);

        var outbound1 = CompareAuditTrailVersionsRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getAuditTrail().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareAuditTrailVersionsRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.audit.v1.AuditTrailHistoryService/CompareAuditTrailVersions", outbound1);
        var reply1 = auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareAuditTrailVersions(command1.payload());

        var outbound2 = ImportUnderwritingsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(ImportUnderwritingsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDisposal().getRequester())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(ImportUnderwritingsResponse.getDefaultInstance());
        var input2 = insuranceUnderwritingImportServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).importUnderwritings(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getDisposal().getAudit().getCreatedAt())
                                .setCreatedBy(request.getDisposal().getAudit().getCreatedBy())
                                .setEtag(request.getDisposal().getAudit().getEtag())
                                .setUpdatedAt(request.getDisposal().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getDisposal().getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitDisposal", request.getDisposal().getDisposalId());
        return SubmitDisposalResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply1.getAuditTrail().getCounterparty().getDisplayName())
                                .build())
                        .addComments(DisposalComment.newBuilder()
                                .setCreatedAt(reply1.getAuditTrail().getAudit().getCreatedAt())
                                .build())
                        .setDisposalId(request.getDisposalId())
                        .setSla(DisposalSla.newBuilder()
                                .setDueAt(reply0.getAuditTrail().getDueAt())
                                .build())
                        .setStatus(request.getDisposal().getStatus())
                        .build())
                .build();
    }

    /**
     * Approves a disposal.
     */
    public ApproveDisposalResponse approveDisposal(ApproveDisposalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveDisposal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDisposal(request.getContext().getTenantId(), request.getDisposal());

        var outbound0 = RejectDepreciationRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getApprover().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> assetsDepreciationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectDepreciation(outbound0), rpcExecutor);

        var outbound1 = ScheduleDisposalReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDisposalId(request.getDisposal().getDisposalId())
                .setReason(request.getReason())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> assetsDisposalReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleDisposalReport(outbound1), rpcExecutor);

        var outbound2 = SubmitWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getDisposal().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var pending2 = CompletableFuture.supplyAsync(() -> collectionsWriteOffApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitWriteOff(outbound2), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1, pending2).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        var reply2 = pending2.join();
        log.atDebug().addKeyValue("auditValue", request.getDisposal().getSubject()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveDisposal", request.getDisposal().getSubject());
        return ApproveDisposalResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply0.getDepreciation().getOrganizer().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getWriteOff().getAudit().getUpdatedBy())
                                .build())
                        .putAllCustomFields(request.getDisposal().getCustomFieldsMap())
                        .setDisposalId(request.getDisposalId())
                        .build())
                .setOutcome(ApproveDisposalResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Rejects a disposal.
     */
    public RejectDisposalResponse rejectDisposal(RejectDisposalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectDisposal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDisposal(request.getContext().getTenantId(), request.getDisposal());

        var outbound0 = RetryCollectionCaseWorkflowRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setCustomer(PaymentPromiseRef.newBuilder()
                                .setDisplayName(request.getDisposal().getCustomer().getDisplayName())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDisposal().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RetryCollectionCaseWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDisposal().getRequester().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> collectionsCollectionCaseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryCollectionCaseWorkflow(value));
        log.atDebug().addKeyValue("auditValue", request.getDisposal().getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectDisposal", request.getDisposal().getDisposalId());
        return RejectDisposalResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getCollectionCase().getAudit().getUpdatedBy())
                                .build())
                        .setDisposalId(request.getDisposalId())
                        .build())
                .setOutcome(RejectDisposalResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Lists disposals a page at a time.
     */
    public ListPendingDisposalsResponse listPendingDisposals(ListPendingDisposalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingDisposals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Disposal", request.getContext().getTraceId(), "ListPendingDisposals");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingDisposals", request.getPage().getPageToken());
        return ListPendingDisposalsResponse.getDefaultInstance();
    }

    /**
     * Delegates a disposal.
     */
    public DelegateDisposalApprovalResponse delegateDisposalApproval(DelegateDisposalApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateDisposalApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDisposal(request.getContext().getTenantId(), request.getDisposal());

        var outbound0 = RejectDepreciationRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getDisposal().getRequester().getName())
                                        .setPreferredChannel(request.getDisposal().getRequester().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .setDetails(RejectDepreciationRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(assetsDepreciationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectDepreciation(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getDisposal().getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateDisposalApproval", request.getDisposal().getDisposalId());
        return DelegateDisposalApprovalResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getDepreciation().getOrganizer().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getDepreciation().getAudit().getUpdatedAt())
                                .build())
                        .setDisposalId(request.getDisposalId())
                        .setPriority(request.getDisposal().getPriority())
                        .build())
                .build();
    }

    /**
     * Recalls a disposal.
     */
    public RecallDisposalResponse recallDisposal(RecallDisposalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallDisposal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDisposal(request.getContext().getTenantId(), request.getDisposal());

        var outbound0 = LintAuditEventsRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(LintAuditEventsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .setPreferredChannel(request.getDisposal().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintAuditEvents(outbound0), rpcExecutor);

        var outbound1 = ListDisposalReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListDisposalReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .setOwnerId(request.getDisposal().getAssignee().getUserId())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> assetsDisposalReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listDisposalReports(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDisposal().getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallDisposal", request.getDisposal().getDisposalId());
        return RecallDisposalResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setDisposalId(request.getDisposalId())
                        .setRequester(request.getDisposal().getRequester())
                        .build())
                .setOutcome(RecallDisposalResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns a disposal.
     */
    public GetDisposalApprovalStatusResponse getDisposalApprovalStatus(GetDisposalApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetDisposalApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Disposal", request.getDisposalId(), "GetDisposalApprovalStatus");
        return GetDisposalApprovalStatusResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setDisposalId(request.getDisposalId())
                        .build())
                .build();
    }

    /**
     * Comments a disposal.
     */
    public CommentDisposalApprovalResponse commentDisposalApproval(CommentDisposalApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentDisposalApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDisposal(request.getContext().getTenantId(), request.getDisposal());

        var outbound0 = CloneTicketRequest.newBuilder()
                .setContext(request.getContext())
                .setTicket(Ticket.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getDisposal().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<CloneTicketRequest, CloneTicketResponse>() {
            @Override
            public CloneTicketResponse apply(CloneTicketRequest input) {
                return helpdeskTicketServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneTicket(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = ResolveAuditTrailConflictRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getTicket().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ResolveAuditTrailConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        RpcPort<ResolveAuditTrailConflictRequest, ResolveAuditTrailConflictResponse> port1 = RpcPort.adapt(auditAuditTrailSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::resolveAuditTrailConflict);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = StreamRepaymentMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamRepaymentMetricsRequest.Filter.newBuilder()
                        .setRegion(reply1.getAuditTrail().getBillingAddress().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDisposal().getRequester().getEmail())
                        .setName(reply0.getTicket().getRequester().getName())
                        .setPreferredChannel(reply0.getTicket().getRequester().getPreferredChannel())
                        .build())
                .build();
        var stream2 = lendingRepaymentReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamRepaymentMetrics(outbound2);
        var reply2 = StreamRepaymentMetricsResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }

        var outbound3 = JsonCodec.roundTrip(DelegateCoverageApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getTicket().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build(), DelegateCoverageApprovalRequest.class);
        var reply3 = insuranceCoverageApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateCoverageApproval(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDisposal().getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentDisposalApproval", request.getDisposal().getDisposalId());
        return CommentDisposalApprovalResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply3.getCoverage().getOwner().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply3.getCoverage().getAudit().getCreatedAt())
                                .setUpdatedBy(reply1.getAuditTrail().getAudit().getUpdatedBy())
                                .build())
                        .putAllCustomFields(request.getDisposal().getCustomFieldsMap())
                        .setDisposalId(request.getDisposalId())
                        .setSla(DisposalSla.newBuilder()
                                .setBreached(reply0.getTicket().getSla().getBreached())
                                .build())
                        .build())
                .build();
    }
}
