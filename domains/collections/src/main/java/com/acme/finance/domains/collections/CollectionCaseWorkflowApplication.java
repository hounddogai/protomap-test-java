package com.acme.finance.domains.collections;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.CompareAuditTrailVersionsRequest;
import com.acme.audit.v1.CompareAuditTrailVersionsResponse;
import com.acme.audit.v1.ListAuditTrailVersionsRequest;
import com.acme.audit.v1.ListAuditTrailVersionsResponse;
import com.acme.audit.v1.ListRetentionPolicyConflictsRequest;
import com.acme.audit.v1.ListRetentionPolicyConflictsResponse;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.banking.v1.RecallMandateRequest;
import com.acme.banking.v1.RecallMandateResponse;
import com.acme.collections.v1.AdvanceCollectionCaseWorkflowRequest;
import com.acme.collections.v1.AdvanceCollectionCaseWorkflowResponse;
import com.acme.collections.v1.BrowseCollectionCasesRequest;
import com.acme.collections.v1.BrowseCollectionCasesResponse;
import com.acme.collections.v1.CancelCollectionCaseWorkflowRequest;
import com.acme.collections.v1.CancelCollectionCaseWorkflowResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseApprovalServiceGrpc;
import com.acme.collections.v1.CollectionCaseQueryServiceGrpc;
import com.acme.collections.v1.CollectionCaseRef;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.collections.v1.CollectionCaseSla;
import com.acme.collections.v1.CreateCollectionCaseRequest;
import com.acme.collections.v1.CreateCollectionCaseResponse;
import com.acme.collections.v1.DeleteCollectionCaseRequest;
import com.acme.collections.v1.DeleteCollectionCaseResponse;
import com.acme.collections.v1.InspectCollectionCaseWorkflowRequest;
import com.acme.collections.v1.InspectCollectionCaseWorkflowResponse;
import com.acme.collections.v1.LintWriteOffsRequest;
import com.acme.collections.v1.LintWriteOffsResponse;
import com.acme.collections.v1.ListCollectionCaseWorkflowsRequest;
import com.acme.collections.v1.ListCollectionCaseWorkflowsResponse;
import com.acme.collections.v1.NormalizeWriteOffRequest;
import com.acme.collections.v1.NormalizeWriteOffResponse;
import com.acme.collections.v1.PauseCollectionCaseWorkflowRequest;
import com.acme.collections.v1.PauseCollectionCaseWorkflowResponse;
import com.acme.collections.v1.RejectCollectionCaseRequest;
import com.acme.collections.v1.RejectCollectionCaseResponse;
import com.acme.collections.v1.ResumeCollectionCaseWorkflowRequest;
import com.acme.collections.v1.ResumeCollectionCaseWorkflowResponse;
import com.acme.collections.v1.RetryCollectionCaseWorkflowRequest;
import com.acme.collections.v1.RetryCollectionCaseWorkflowResponse;
import com.acme.collections.v1.SignalCollectionCaseWorkflowRequest;
import com.acme.collections.v1.SignalCollectionCaseWorkflowResponse;
import com.acme.collections.v1.StartCollectionCaseWorkflowRequest;
import com.acme.collections.v1.StartCollectionCaseWorkflowResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ApprovePremiumRequest;
import com.acme.insurance.v1.ApprovePremiumResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageApprovalServiceGrpc;
import com.acme.insurance.v1.CoverageBlockAction;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.CoverageReportingServiceGrpc;
import com.acme.insurance.v1.DelegateCoverageApprovalRequest;
import com.acme.insurance.v1.DelegateCoverageApprovalResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.PremiumApprovalServiceGrpc;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.insurance.v1.RecallPolicyRequest;
import com.acme.insurance.v1.RecallPolicyResponse;
import com.acme.insurance.v1.ScheduleCoverageReportRequest;
import com.acme.insurance.v1.ScheduleCoverageReportResponse;
import com.acme.insurance.v1.StreamPremiumMetricsRequest;
import com.acme.insurance.v1.StreamPremiumMetricsResponse;
import com.acme.ledger.v1.AnnotateJournalEntryVersionRequest;
import com.acme.ledger.v1.AnnotateJournalEntryVersionResponse;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionRequest;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionResponse;
import com.acme.ledger.v1.CompareJournalEntryVersionsRequest;
import com.acme.ledger.v1.CompareJournalEntryVersionsResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryCardSettlement;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountCardFunding;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationTemplateRef;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.ValidateNotificationRequest;
import com.acme.notifications.v1.ValidateNotificationResponse;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
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
 * Runs the long-lived workflow that takes a collection case from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class CollectionCaseWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(CollectionCaseWorkflowApplication.class);

    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking;

    private final MandateApprovalServiceGrpc.MandateApprovalServiceBlockingStub bankingMandateApprovalServiceBlocking;

    private final ChannelRegistry channels;

    private final CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceBlockingStub collectionsCollectionCaseApprovalServiceBlocking;

    private final CollectionCaseServiceGrpc.CollectionCaseServiceBlockingStub collectionsCollectionCaseServiceBlocking;

    private final WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking;

    private final CoverageApprovalServiceGrpc.CoverageApprovalServiceBlockingStub insuranceCoverageApprovalServiceBlocking;

    private final CoverageReportingServiceGrpc.CoverageReportingServiceBlockingStub insuranceCoverageReportingServiceBlocking;

    private final PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking;

    private final PremiumApprovalServiceGrpc.PremiumApprovalServiceBlockingStub insurancePremiumApprovalServiceBlocking;

    private final PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking;

    private final JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking;

    private final NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking;

    private final CollectionsRepository repository;

    private final Executor rpcExecutor;

    public CollectionCaseWorkflowApplication(
            AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking,
            MandateApprovalServiceGrpc.MandateApprovalServiceBlockingStub bankingMandateApprovalServiceBlocking,
            ChannelRegistry channels,
            CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceBlockingStub collectionsCollectionCaseApprovalServiceBlocking,
            CollectionCaseServiceGrpc.CollectionCaseServiceBlockingStub collectionsCollectionCaseServiceBlocking,
            WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking,
            CoverageApprovalServiceGrpc.CoverageApprovalServiceBlockingStub insuranceCoverageApprovalServiceBlocking,
            CoverageReportingServiceGrpc.CoverageReportingServiceBlockingStub insuranceCoverageReportingServiceBlocking,
            PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking,
            PremiumApprovalServiceGrpc.PremiumApprovalServiceBlockingStub insurancePremiumApprovalServiceBlocking,
            PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking,
            JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking,
            NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking,
            CollectionsRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditAuditTrailHistoryServiceBlocking = auditAuditTrailHistoryServiceBlocking;
        this.auditRetentionPolicySyncServiceBlocking = auditRetentionPolicySyncServiceBlocking;
        this.bankingMandateApprovalServiceBlocking = bankingMandateApprovalServiceBlocking;
        this.channels = channels;
        this.collectionsCollectionCaseApprovalServiceBlocking = collectionsCollectionCaseApprovalServiceBlocking;
        this.collectionsCollectionCaseServiceBlocking = collectionsCollectionCaseServiceBlocking;
        this.collectionsWriteOffValidationServiceBlocking = collectionsWriteOffValidationServiceBlocking;
        this.insuranceCoverageApprovalServiceBlocking = insuranceCoverageApprovalServiceBlocking;
        this.insuranceCoverageReportingServiceBlocking = insuranceCoverageReportingServiceBlocking;
        this.insurancePolicyApprovalServiceBlocking = insurancePolicyApprovalServiceBlocking;
        this.insurancePremiumApprovalServiceBlocking = insurancePremiumApprovalServiceBlocking;
        this.insurancePremiumReportingServiceBlocking = insurancePremiumReportingServiceBlocking;
        this.ledgerJournalEntryHistoryServiceBlocking = ledgerJournalEntryHistoryServiceBlocking;
        this.ledgerLedgerAccountHistoryServiceBlocking = ledgerLedgerAccountHistoryServiceBlocking;
        this.notificationsNotificationValidationServiceBlocking = notificationsNotificationValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Starts a collection case.
     */
    public StartCollectionCaseWorkflowResponse startCollectionCaseWorkflow(StartCollectionCaseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartCollectionCaseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollectionCase(request.getContext().getTenantId(), request.getCollectionCase());

        List<ListAuditTrailVersionsResponse> results0 = request.getCollectionCase().getCommentsList().stream()
                .map(element -> ListAuditTrailVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditTrailVersionsRequest.Filter.newBuilder()
                        .setCreatedAfter(element.getCreatedAt())
                        .putAllLabels(request.getDetails().getParametersMap())
                        .setOwnerId(element.getCommentId())
                        .build())
                .build())
                .flatMap(value -> Stream.of(auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditTrailVersions(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? ListAuditTrailVersionsResponse.getDefaultInstance()
                : results0.getLast();

        List<AnnotateJournalEntryVersionResponse> results1 = request.getCollectionCase().getCommentsList().stream()
                .map(element -> AnnotateJournalEntryVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AnnotateJournalEntryVersionRequest.Details.newBuilder()
                        .setAuthorEmail(element.getAuthorEmail())
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .setEtag(request.getCollectionCase().getAudit().getEtag())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(ledgerJournalEntryHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotateJournalEntryVersion(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? AnnotateJournalEntryVersionResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = BrowseCollectionCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseCollectionCasesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getJournalEntry().getAudit().getUpdatedAt())
                        .setRegion(reply1.getJournalEntry().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.collections.v1.CollectionCaseQueryService");
        var selected2 = CollectionCaseQueryServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.browseCollectionCases(outbound2);

        var futures3 = request.getCollectionCase().getCommentsList().stream()
                .map(element -> RecallMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .setEtag(request.getCollectionCase().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply1.getJournalEntry().getBillingAddress().getRecipient())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> bankingMandateApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallMandate(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures3.toArray(CompletableFuture[]::new)).join();
        var results3 = futures3.stream().map(CompletableFuture::join).toList();
        var reply3 = results3.isEmpty() ? RecallMandateResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getCollectionCase().getCollectionCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartCollectionCaseWorkflow", request.getCollectionCase().getCollectionCaseId());
        return StartCollectionCaseWorkflowResponse.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setCollectionCaseId(request.getCollectionCaseId())
                        .setRequester(request.getCollectionCase().getRequester())
                        .build())
                .setOutcome(StartCollectionCaseWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Advances a collection case.
     */
    public AdvanceCollectionCaseWorkflowResponse advanceCollectionCaseWorkflow(AdvanceCollectionCaseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceCollectionCaseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollectionCase(request.getContext().getTenantId(), request.getCollectionCase());

        var outbound0 = ScheduleCoverageReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(ScheduleCoverageReportRequest.Details.newBuilder()
                        .putAllFilters(request.getDetails().getParametersMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> insuranceCoverageReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleCoverageReport(outbound0), rpcExecutor);

        var outbound1 = LintWriteOffsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintWriteOffsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintWriteOffs(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = RejectCollectionCaseRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getWriteOff().getAudit().getEtag())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotify().getName())
                                .build())
                        .setSla(CollectionCaseSla.newBuilder()
                                .setDueAt(reply1.getWriteOff().getDueAt())
                                .setPolicyId(request.getCollectionCase().getSla().getPolicyId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        Function<RejectCollectionCaseRequest, RejectCollectionCaseResponse> operation2 = collectionsCollectionCaseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::rejectCollectionCase;
        var reply2 = operation2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCollectionCase().getCollectionCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceCollectionCaseWorkflow", request.getCollectionCase().getCollectionCaseId());
        return AdvanceCollectionCaseWorkflowResponse.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(request.getCollectionCase().getAssignee())
                        .setCollectionCaseId(request.getCollectionCaseId())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply2.getCollectionCase().getAssignee().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Cancels a collection case.
     */
    public CancelCollectionCaseWorkflowResponse cancelCollectionCaseWorkflow(CancelCollectionCaseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelCollectionCaseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollectionCase(request.getContext().getTenantId(), request.getCollectionCase());

        var outbound0 = CompareAuditTrailVersionsRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCollectionCase().getAudit().getCreatedAt())
                                .setCreatedBy(request.getCollectionCase().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getCollectionCase().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(RetentionPolicyRef.newBuilder()
                                .setDisplayName(request.getCollectionCase().getAssignee().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareAuditTrailVersionsRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply0 = auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareAuditTrailVersions(outbound0);

        var outbound1 = CompareJournalEntryVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareJournalEntryVersionsRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCollectionCase().getAudit().getCreatedAt())
                                .build())
                        .setCard(JournalEntryCardSettlement.newBuilder()
                                .setToken(switch (reply0.getAuditTrail().getSettlementCase()) {
                                    case CARD -> reply0.getAuditTrail().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = ledgerJournalEntryHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareJournalEntryVersions(outbound1);

        var outbound2 = RpcSupport.unpack(Any.pack(CreateCollectionCaseRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getJournalEntry().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()), CreateCollectionCaseRequest.class);
        var reply2 = collectionsCollectionCaseServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createCollectionCase(outbound2);

        var outbound3 = DeleteCollectionCaseRequest.newBuilder()
                .setCollectionCaseId(request.getCollectionCase().getCollectionCaseId())
                .setContext(request.getContext())
                .setEtag(request.getCollectionCase().getAudit().getEtag())
                .build();
        Function<DeleteCollectionCaseRequest, DeleteCollectionCaseResponse> operation3 = collectionsCollectionCaseServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::deleteCollectionCase;
        var reply3 = operation3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getCollectionCase().getAssignee().getUserId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelCollectionCaseWorkflow", request.getCollectionCase().getAssignee().getUserId());
        return CancelCollectionCaseWorkflowResponse.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setCollectionCaseId(request.getCollectionCaseId())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply2.getCollectionCase().getAssignee().getEmail())
                                .build())
                        .setSubject(request.getCollectionCase().getSubject())
                        .build())
                .setOutcome(CancelCollectionCaseWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply3.getResult().getMessage())
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Retrys a collection case.
     */
    public RetryCollectionCaseWorkflowResponse retryCollectionCaseWorkflow(RetryCollectionCaseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryCollectionCaseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollectionCase(request.getContext().getTenantId(), request.getCollectionCase());

        var outbound0 = StreamPremiumMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getCollectionCase().getRequester().getEmail())
                        .setName(request.getCollectionCase().getRequester().getName())
                        .setPhone(request.getDetails().getNotify().getPhone())
                        .build())
                .build();
        var stream0 = insurancePremiumReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamPremiumMetrics(outbound0);
        var reply0 = StreamPremiumMetricsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = ValidateNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getCollectionCase().getAssignee().getEmail())
                                .setUserId(request.getCollectionCase().getAssignee().getUserId())
                                .build())
                        .setCustomer(NotificationTemplateRef.newBuilder()
                                .setDisplayName(request.getCollectionCase().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
        Function<ValidateNotificationRequest, ValidateNotificationResponse> operation1 = notificationsNotificationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::validateNotification;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCollectionCase().getCollectionCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryCollectionCaseWorkflow", request.getCollectionCase().getCollectionCaseId());
        return RetryCollectionCaseWorkflowResponse.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply1.getNotification().getAssignee().getUserId())
                                .build())
                        .setCollectionCaseId(request.getCollectionCaseId())
                        .setPriority(request.getCollectionCase().getPriority())
                        .build())
                .build();
    }

    /**
     * Pauses a collection case.
     */
    public PauseCollectionCaseWorkflowResponse pauseCollectionCaseWorkflow(PauseCollectionCaseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseCollectionCaseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollectionCase(request.getContext().getTenantId(), request.getCollectionCase());

        var results0 = new ArrayList<DelegateCoverageApprovalResponse>();
        for (var element : request.getCollectionCase().getCommentsList()) {
            DelegateCoverageApprovalRequest itemRequest = DelegateCoverageApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .setBlock(CoverageBlockAction.newBuilder()
                                .setNotifyEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .build();
            results0.add(insuranceCoverageApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateCoverageApproval(itemRequest));
        }
        var reply0 = results0.isEmpty() ? DelegateCoverageApprovalResponse.getDefaultInstance()
                : results0.getLast();

        var builder1 = RecallPolicyRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setPolicy(Policy.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedAt(reply0.getCoverage().getAudit().getCreatedAt())
                        .setUpdatedBy(reply0.getCoverage().getAudit().getUpdatedBy())
                        .build())
                .setHolder(CoverageRef.newBuilder()
                        .setDisplayName(request.getCollectionCase().getAssignee().getDisplayName())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = insurancePolicyApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallPolicy(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCollectionCase().getCollectionCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseCollectionCaseWorkflow", request.getCollectionCase().getCollectionCaseId());
        return PauseCollectionCaseWorkflowResponse.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setCollectionCaseId(request.getCollectionCaseId())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(reply1.getPolicy().getContact().getPhone())
                                .build())
                        .setSubject(request.getCollectionCase().getSubject())
                        .build())
                .build();
    }

    /**
     * Resumes a collection case.
     */
    public ResumeCollectionCaseWorkflowResponse resumeCollectionCaseWorkflow(ResumeCollectionCaseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeCollectionCaseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollectionCase(request.getContext().getTenantId(), request.getCollectionCase());

        var outbound0 = ApprovePremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ApprovePremiumRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getCollectionCase().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = insurancePremiumApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approvePremium(outbound0);

        var outbound1 = AnnotateLedgerAccountVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AnnotateLedgerAccountVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setCard(LedgerAccountCardFunding.newBuilder()
                                .setNetwork(switch (reply0.getPremium().getSettlementCase()) {
                                    case CARD -> reply0.getPremium().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = ledgerLedgerAccountHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotateLedgerAccountVersion(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCollectionCase().getCollectionCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeCollectionCaseWorkflow", request.getCollectionCase().getCollectionCaseId());
        return ResumeCollectionCaseWorkflowResponse.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(request.getCollectionCase().getAssignee())
                        .setCollectionCaseId(request.getCollectionCaseId())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(reply1.getLedgerAccount().getContact().getPhone())
                                .build())
                        .build())
                .setOutcome(ResumeCollectionCaseWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a collection case.
     */
    public InspectCollectionCaseWorkflowResponse inspectCollectionCaseWorkflow(InspectCollectionCaseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectCollectionCaseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CollectionCase", request.getCollectionCaseId(), "InspectCollectionCaseWorkflow");

        var outbound0 = LintWriteOffsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintWriteOffsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setCounterparty(CollectionCaseRef.newBuilder()
                                .setCollectionCaseId(request.getCollectionCaseId())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.collections.v1.WriteOffValidationService/LintWriteOffs", outbound0);
        var reply0 = collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintWriteOffs(command0.payload());
        return InspectCollectionCaseWorkflowResponse.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setCollectionCaseId(request.getCollectionCaseId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists collection cases a page at a time.
     */
    public ListCollectionCaseWorkflowsResponse listCollectionCaseWorkflows(ListCollectionCaseWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListCollectionCaseWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CollectionCase", request.getContext().getTraceId(), "ListCollectionCaseWorkflows");

        var outbound0 = (NormalizeWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeWriteOffRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeWriteOff(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListCollectionCaseWorkflows", request.getPage().getPageToken());
        return ListCollectionCaseWorkflowsResponse.newBuilder()
                .addCollectionCases(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getWriteOff().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Signals a collection case.
     */
    public SignalCollectionCaseWorkflowResponse signalCollectionCaseWorkflow(SignalCollectionCaseWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalCollectionCaseWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCollectionCase(request.getContext().getTenantId(), request.getCollectionCase());

        var outbound0 = ListRetentionPolicyConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListRetentionPolicyConflictsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCollectionCase().getCustomer().getStartsAt())
                        .setOwnerId(switch (request.getCollectionCase().getOriginCase()) {
                            case CUSTOMER -> request.getCollectionCase().getCustomer().getPaymentPromiseId();
                            default -> "";
                        })
                        .build())
                .build();
        var reply0 = auditRetentionPolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listRetentionPolicyConflicts(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getCollectionCase().getCollectionCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalCollectionCaseWorkflow", request.getCollectionCase().getCollectionCaseId());
        return SignalCollectionCaseWorkflowResponse.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setCollectionCaseId(request.getCollectionCaseId())
                        .setRequester(request.getCollectionCase().getRequester())
                        .build())
                .setOutcome(SignalCollectionCaseWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
