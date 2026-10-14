package com.acme.finance.domains.financeportal;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalApprovalServiceGrpc;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.RejectDisposalRequest;
import com.acme.assets.v1.RejectDisposalResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CommentHouseholdApprovalRequest;
import com.acme.customers.v1.CommentHouseholdApprovalResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.DelegateHouseholdApprovalRequest;
import com.acme.customers.v1.DelegateHouseholdApprovalResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdApprovalServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.ApprovalInboxNotificationServiceGrpc;
import com.acme.financeportal.v1.ApprovalInboxWorkflowServiceGrpc;
import com.acme.financeportal.v1.BatchGetFinanceWorkspacesRequest;
import com.acme.financeportal.v1.BatchGetFinanceWorkspacesResponse;
import com.acme.financeportal.v1.BrowseCloseChecklistsRequest;
import com.acme.financeportal.v1.BrowseCloseChecklistsResponse;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.CloseChecklistIntegrationServiceGrpc;
import com.acme.financeportal.v1.CloseChecklistNotificationServiceGrpc;
import com.acme.financeportal.v1.ExportCloseChecklistsRequest;
import com.acme.financeportal.v1.ExportCloseChecklistsResponse;
import com.acme.financeportal.v1.FinanceWorkspaceIntegrationServiceGrpc;
import com.acme.financeportal.v1.FinanceWorkspaceServiceGrpc;
import com.acme.financeportal.v1.ListApprovalInboxSubscriptionsRequest;
import com.acme.financeportal.v1.ListApprovalInboxSubscriptionsResponse;
import com.acme.financeportal.v1.ListFinanceWorkspaceSourcesRequest;
import com.acme.financeportal.v1.ListFinanceWorkspaceSourcesResponse;
import com.acme.financeportal.v1.LookupCloseChecklistRequest;
import com.acme.financeportal.v1.LookupCloseChecklistResponse;
import com.acme.financeportal.v1.NotifyCloseChecklistWatchersRequest;
import com.acme.financeportal.v1.NotifyCloseChecklistWatchersResponse;
import com.acme.financeportal.v1.PeekCloseChecklistRequest;
import com.acme.financeportal.v1.PeekCloseChecklistResponse;
import com.acme.financeportal.v1.ReplayCloseChecklistEventsRequest;
import com.acme.financeportal.v1.ReplayCloseChecklistEventsResponse;
import com.acme.financeportal.v1.ReplayFinanceWorkspaceEventsRequest;
import com.acme.financeportal.v1.ReplayFinanceWorkspaceEventsResponse;
import com.acme.financeportal.v1.ResolveCloseChecklistsRequest;
import com.acme.financeportal.v1.ResolveCloseChecklistsResponse;
import com.acme.financeportal.v1.ResumeApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.ResumeApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.SuggestCloseChecklistsRequest;
import com.acme.financeportal.v1.SuggestCloseChecklistsResponse;
import com.acme.financeportal.v1.SummarizeCloseChecklistsRequest;
import com.acme.financeportal.v1.SummarizeCloseChecklistsResponse;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.CloneLedgerAccountRequest;
import com.acme.ledger.v1.CloneLedgerAccountResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.SearchLedgerAccountsRequest;
import com.acme.ledger.v1.SearchLedgerAccountsResponse;
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
 * Answers read-only questions about close checklists, for screens and reports. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class CloseChecklistQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(CloseChecklistQueryApplication.class);

    private final DisposalApprovalServiceGrpc.DisposalApprovalServiceBlockingStub assetsDisposalApprovalServiceBlocking;

    private final ChannelRegistry channels;

    private final HouseholdApprovalServiceGrpc.HouseholdApprovalServiceBlockingStub customersHouseholdApprovalServiceBlocking;

    private final ApprovalInboxNotificationServiceGrpc.ApprovalInboxNotificationServiceBlockingStub financeportalApprovalInboxNotificationServiceBlocking;

    private final ApprovalInboxWorkflowServiceGrpc.ApprovalInboxWorkflowServiceBlockingStub financeportalApprovalInboxWorkflowServiceBlocking;

    private final CloseChecklistIntegrationServiceGrpc.CloseChecklistIntegrationServiceBlockingStub financeportalCloseChecklistIntegrationServiceBlocking;

    private final FinanceWorkspaceIntegrationServiceGrpc.FinanceWorkspaceIntegrationServiceBlockingStub financeportalFinanceWorkspaceIntegrationServiceBlocking;

    private final FinanceWorkspaceServiceGrpc.FinanceWorkspaceServiceBlockingStub financeportalFinanceWorkspaceServiceBlocking;

    private final LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking;

    private final LedgerAccountServiceGrpc.LedgerAccountServiceFutureStub ledgerLedgerAccountServiceFuture;

    private final FinanceportalRepository repository;

    private final Executor rpcExecutor;

    public CloseChecklistQueryApplication(
            DisposalApprovalServiceGrpc.DisposalApprovalServiceBlockingStub assetsDisposalApprovalServiceBlocking,
            ChannelRegistry channels,
            HouseholdApprovalServiceGrpc.HouseholdApprovalServiceBlockingStub customersHouseholdApprovalServiceBlocking,
            ApprovalInboxNotificationServiceGrpc.ApprovalInboxNotificationServiceBlockingStub financeportalApprovalInboxNotificationServiceBlocking,
            ApprovalInboxWorkflowServiceGrpc.ApprovalInboxWorkflowServiceBlockingStub financeportalApprovalInboxWorkflowServiceBlocking,
            CloseChecklistIntegrationServiceGrpc.CloseChecklistIntegrationServiceBlockingStub financeportalCloseChecklistIntegrationServiceBlocking,
            FinanceWorkspaceIntegrationServiceGrpc.FinanceWorkspaceIntegrationServiceBlockingStub financeportalFinanceWorkspaceIntegrationServiceBlocking,
            FinanceWorkspaceServiceGrpc.FinanceWorkspaceServiceBlockingStub financeportalFinanceWorkspaceServiceBlocking,
            LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking,
            LedgerAccountServiceGrpc.LedgerAccountServiceFutureStub ledgerLedgerAccountServiceFuture,
            FinanceportalRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsDisposalApprovalServiceBlocking = assetsDisposalApprovalServiceBlocking;
        this.channels = channels;
        this.customersHouseholdApprovalServiceBlocking = customersHouseholdApprovalServiceBlocking;
        this.financeportalApprovalInboxNotificationServiceBlocking = financeportalApprovalInboxNotificationServiceBlocking;
        this.financeportalApprovalInboxWorkflowServiceBlocking = financeportalApprovalInboxWorkflowServiceBlocking;
        this.financeportalCloseChecklistIntegrationServiceBlocking = financeportalCloseChecklistIntegrationServiceBlocking;
        this.financeportalFinanceWorkspaceIntegrationServiceBlocking = financeportalFinanceWorkspaceIntegrationServiceBlocking;
        this.financeportalFinanceWorkspaceServiceBlocking = financeportalFinanceWorkspaceServiceBlocking;
        this.ledgerLedgerAccountServiceBlocking = ledgerLedgerAccountServiceBlocking;
        this.ledgerLedgerAccountServiceFuture = ledgerLedgerAccountServiceFuture;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Returns a close checklist.
     */
    public LookupCloseChecklistResponse lookupCloseChecklist(LookupCloseChecklistRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupCloseChecklist")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CloseChecklist", request.getCloseChecklistId(), "LookupCloseChecklist");
        return LookupCloseChecklistResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setCloseChecklistId(request.getCloseChecklistId())
                        .build())
                .build();
    }

    /**
     * Lists close checklists a page at a time.
     */
    public BrowseCloseChecklistsResponse browseCloseChecklists(BrowseCloseChecklistsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseCloseChecklists")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CloseChecklist", request.getContext().getTraceId(), "BrowseCloseChecklists");

        var outbound0 = BatchGetFinanceWorkspacesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> financeportalFinanceWorkspaceServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetFinanceWorkspaces(outbound0), rpcExecutor);

        var outbound1 = DelegateHouseholdApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRegion(request.getFilter().getRegion())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setUserId(request.getContext().getActor().getUserId())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setEmail(request.getContext().getActor().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> customersHouseholdApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateHouseholdApproval(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseCloseChecklists", request.getPage().getPageToken());
        return BrowseCloseChecklistsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns several close checklists at once.
     */
    public ResolveCloseChecklistsResponse resolveCloseChecklists(ResolveCloseChecklistsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveCloseChecklists")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CloseChecklist", request.getContext().getTraceId(), "ResolveCloseChecklists");

        var outbound0 = CloneLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setExternalAccountId(request.getContext().getActor().getUserId())
                        .build())
                .build();
        var pending0 = ledgerLedgerAccountServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).cloneLedgerAccount(outbound0);
        var reply0 = RpcSupport.await(pending0);
        return ResolveCloseChecklistsResponse.newBuilder()
                .addCloseChecklists(CloseChecklist.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getLedgerAccount().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
    }

    /**
     * Streams every matching close checklist.
     */
    public ExportCloseChecklistsResponse exportCloseChecklists(ExportCloseChecklistsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportCloseChecklists")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CloseChecklist", request.getContext().getTraceId(), "ExportCloseChecklists");

        var outbound0 = ReplayCloseChecklistEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReplayCloseChecklistEventsRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .setMessageId(request.getFilter().getOwnerId())
                        .setReceivedAt(request.getFilter().getCreatedAfter())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(financeportalCloseChecklistIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).replayCloseChecklistEvents(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = RejectDisposalRequest.newBuilder()
                .setContext(request.getContext())
                .setDisposal(Disposal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getCloseChecklist().getAudit().getUpdatedAt())
                                .build())
                        .setCustomer(FixedAssetRef.newBuilder()
                                .setDisplayName(reply0.getCloseChecklist().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        Function<RejectDisposalRequest, RejectDisposalResponse> operation1 = assetsDisposalApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::rejectDisposal;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = CommentHouseholdApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedBy(reply1.getDisposal().getAudit().getUpdatedBy())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRegion(request.getFilter().getRegion())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(reply1.getDisposal().getCustomer().getDisplayName())
                                                .setEmail(reply1.getDisposal().getAssignee().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.customers.v1.HouseholdApprovalService");
        var selected2 = HouseholdApprovalServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.commentHouseholdApproval(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportCloseChecklists", request.getFilter().getQuery());
        return ExportCloseChecklistsResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply2.getHousehold().getLocation().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getCloseChecklist().getCounterparty().getDisplayName())
                                .setEmail(reply1.getDisposal().getAssignee().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Summarizes a close checklist.
     */
    public SummarizeCloseChecklistsResponse summarizeCloseChecklists(SummarizeCloseChecklistsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeCloseChecklists")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CloseChecklist", request.getCloseChecklistId(), "SummarizeCloseChecklists");

        var outbound0 = ListApprovalInboxSubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListApprovalInboxSubscriptionsRequest.Filter.newBuilder()
                        .putAllLabels(request.getDetails().getFiltersMap())
                        .setOwnerId(request.getCloseChecklistId())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(financeportalApprovalInboxNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listApprovalInboxSubscriptions(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = ReplayFinanceWorkspaceEventsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(ReplayFinanceWorkspaceEventsRequest.Details.newBuilder()
                        .setEnvelope(Any.pack(request.getContext()))
                        .setMessageId(reply0.getResult().getMessage())
                        .setReceivedAt(request.getContext().getDeadline())
                        .build())
                .setReason(request.getReason())
                .build();
        Function<ReplayFinanceWorkspaceEventsRequest, ReplayFinanceWorkspaceEventsResponse> operation1 = financeportalFinanceWorkspaceIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::replayFinanceWorkspaceEvents;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = NotifyCloseChecklistWatchersRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NotifyCloseChecklistWatchersRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setName(reply1.getFinanceWorkspace().getRequester().getName())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.financeportal.v1.CloseChecklistNotificationService");
        var selected2 = CloseChecklistNotificationServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.notifyCloseChecklistWatchers(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getFormat()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeCloseChecklists", request.getDetails().getFormat());
        return SummarizeCloseChecklistsResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply2.getOutcome().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists close checklists a page at a time.
     */
    public SuggestCloseChecklistsResponse suggestCloseChecklists(SuggestCloseChecklistsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestCloseChecklists")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CloseChecklist", request.getContext().getTraceId(), "SuggestCloseChecklists");

        var outbound0 = ResumeApprovalInboxWorkflowRequest.newBuilder()
                .setApprovalInbox(ApprovalInbox.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .putAllMetadata(request.getFilter().getLabelsMap())
                        .build())
                .setContext(request.getContext())
                .setDetails(ResumeApprovalInboxWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            ResumeApprovalInboxWorkflowResponse fetch(ResumeApprovalInboxWorkflowRequest input) {
                return financeportalApprovalInboxWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeApprovalInboxWorkflow(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestCloseChecklists", request.getPage().getPageToken());
        return SuggestCloseChecklistsResponse.newBuilder()
                .addCloseChecklists(CloseChecklist.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getApprovalInbox().getAudit().getEtag())
                                .build())
                        .build())
                .build();
    }

    /**
     * Returns a close checklist.
     */
    public PeekCloseChecklistResponse peekCloseChecklist(PeekCloseChecklistRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekCloseChecklist")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CloseChecklist", request.getCloseChecklistId(), "PeekCloseChecklist");

        var outbound0 = BeanCopies.copy(ListFinanceWorkspaceSourcesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListFinanceWorkspaceSourcesRequest.Filter.newBuilder()
                        .setOwnerId(request.getCloseChecklistId())
                        .build())
                .build(), ListFinanceWorkspaceSourcesRequest.class);
        var reply0 = financeportalFinanceWorkspaceIntegrationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listFinanceWorkspaceSources(outbound0);

        var outbound1 = SearchLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchLedgerAccountsRequest.Filter.newBuilder()
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .build())
                .build();
        var reply1 = RpcSupport.retryRead(() -> ledgerLedgerAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchLedgerAccounts(outbound1));
        return PeekCloseChecklistResponse.newBuilder()
                .setCloseChecklist(CloseChecklist.newBuilder()
                        .setCloseChecklistId(request.getCloseChecklistId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
