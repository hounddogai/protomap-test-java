package com.acme.finance.domains.ledger;

import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.FindAccessPoliciesRequest;
import com.acme.authorization.v1.FindAccessPoliciesResponse;
import com.acme.authorization.v1.WatchAccessPoliciesRequest;
import com.acme.authorization.v1.WatchAccessPoliciesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.AnnotateJournalEntryVersionRequest;
import com.acme.ledger.v1.AnnotateJournalEntryVersionResponse;
import com.acme.ledger.v1.CompareJournalEntryVersionsRequest;
import com.acme.ledger.v1.CompareJournalEntryVersionsResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.ListJournalEntryVersionsRequest;
import com.acme.ledger.v1.ListJournalEntryVersionsResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.PruneJournalEntryVersionsRequest;
import com.acme.ledger.v1.PruneJournalEntryVersionsResponse;
import com.acme.ledger.v1.RecordJournalEntryVersionRequest;
import com.acme.ledger.v1.RecordJournalEntryVersionResponse;
import com.acme.ledger.v1.RevertJournalEntryRequest;
import com.acme.ledger.v1.RevertJournalEntryResponse;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeRequest;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeResponse;
import com.acme.ledger.v1.StreamPostingBatchNoticesRequest;
import com.acme.ledger.v1.StreamPostingBatchNoticesResponse;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Keeps every version of a journal entry and restores earlier ones. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class JournalEntryHistoryApplication {
    private static final Logger log = LoggerFactory.getLogger(JournalEntryHistoryApplication.class);

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking;

    private final PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking;

    private final LedgerRepository repository;

    public JournalEntryHistoryApplication(
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking,
            PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking,
            LedgerRepository repository) {
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.ledgerLedgerAccountNotificationServiceBlocking = ledgerLedgerAccountNotificationServiceBlocking;
        this.ledgerPostingBatchNotificationServiceBlocking = ledgerPostingBatchNotificationServiceBlocking;
        this.repository = repository;
    }

    /**
     * Records a journal entry.
     */
    public RecordJournalEntryVersionResponse recordJournalEntryVersion(RecordJournalEntryVersionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecordJournalEntryVersion")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveJournalEntry(request.getContext().getTenantId(), request.getJournalEntry());

        var results0 = new ArrayList<StreamPostingBatchNoticesResponse>();
        for (var element : request.getJournalEntry().getLinesList()) {
            StreamPostingBatchNoticesRequest itemRequest = StreamPostingBatchNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamPostingBatchNoticesRequest.Filter.newBuilder()
                        .putAllLabels(element.getAttributesMap())
                        .setOwnerId(element.getLineId())
                        .setRegion(request.getJournalEntry().getBillingAddress().getRegion())
                        .build())
                .build();
            var responses = ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamPostingBatchNotices(itemRequest);
            responses.forEachRemaining(results0::add);
        }
        var reply0 = results0.isEmpty() ? StreamPostingBatchNoticesResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getJournalEntry().getJournalEntryId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecordJournalEntryVersion", request.getJournalEntry().getJournalEntryId());
        return RecordJournalEntryVersionResponse.newBuilder()
                .setJournalEntry(JournalEntry.newBuilder()
                        .setJournalEntryId(request.getJournalEntryId())
                        .setNumber(request.getJournalEntry().getNumber())
                        .build())
                .build();
    }

    /**
     * Lists journal entries a page at a time.
     */
    public ListJournalEntryVersionsResponse listJournalEntryVersions(ListJournalEntryVersionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListJournalEntryVersions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "JournalEntry", request.getContext().getTraceId(), "ListJournalEntryVersions");

        var outbound0 = WatchAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchAccessPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .build();
        var stream0 = authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchAccessPolicies(outbound0);
        var reply0 = WatchAccessPoliciesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListJournalEntryVersions", request.getPage().getPageToken());
        return ListJournalEntryVersionsResponse.getDefaultInstance();
    }

    /**
     * Compares a journal entry.
     */
    public CompareJournalEntryVersionsResponse compareJournalEntryVersions(CompareJournalEntryVersionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareJournalEntryVersions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveJournalEntry(request.getContext().getTenantId(), request.getJournalEntry());

        var outbound0 = (SendLedgerAccountTestNoticeRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SendLedgerAccountTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getAuthorEmail())
                                .build())
                        .build())
                .setLedgerAccountId(request.getJournalEntry().getCounterparty().getLedgerAccountId())
                .setReason(request.getReason())
                .build()).toBuilder().build();
        var reply0 = ledgerLedgerAccountNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sendLedgerAccountTestNotice(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getJournalEntry().getJournalEntryId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareJournalEntryVersions", request.getJournalEntry().getJournalEntryId());
        return CompareJournalEntryVersionsResponse.newBuilder()
                .setJournalEntry(JournalEntry.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getLedgerAccount().getAudit().getCreatedBy())
                                .build())
                        .setDueAt(request.getJournalEntry().getDueAt())
                        .setJournalEntryId(request.getJournalEntryId())
                        .build())
                .build();
    }

    /**
     * Reverts a journal entry.
     */
    public RevertJournalEntryResponse revertJournalEntry(RevertJournalEntryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RevertJournalEntry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveJournalEntry(request.getContext().getTenantId(), request.getJournalEntry());

        var outbound0 = FindAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindAccessPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getJournalEntry().getAudit().getCreatedAt())
                        .setRegion(request.getJournalEntry().getBillingAddress().getRegion())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.authorization.v1.AccessPolicyQueryService/FindAccessPolicies", outbound0);
        var reply0 = authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findAccessPolicies(command0.payload());
        log.atDebug().addKeyValue("auditValue", request.getJournalEntry().getJournalEntryId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RevertJournalEntry", request.getJournalEntry().getJournalEntryId());
        return RevertJournalEntryResponse.newBuilder()
                .setJournalEntry(JournalEntry.newBuilder()
                        .setIssuedAt(request.getJournalEntry().getIssuedAt())
                        .setJournalEntryId(request.getJournalEntryId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Annotates a journal entry.
     */
    public AnnotateJournalEntryVersionResponse annotateJournalEntryVersion(AnnotateJournalEntryVersionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AnnotateJournalEntryVersion")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveJournalEntry(request.getContext().getTenantId(), request.getJournalEntry());
        log.atDebug().addKeyValue("auditValue", request.getJournalEntry().getJournalEntryId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AnnotateJournalEntryVersion", request.getJournalEntry().getJournalEntryId());
        return AnnotateJournalEntryVersionResponse.newBuilder()
                .setJournalEntry(JournalEntry.newBuilder()
                        .setJournalEntryId(request.getJournalEntryId())
                        .putAllMetadata(request.getJournalEntry().getMetadataMap())
                        .build())
                .build();
    }

    /**
     * Deletes a journal entry.
     */
    public PruneJournalEntryVersionsResponse pruneJournalEntryVersions(PruneJournalEntryVersionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PruneJournalEntryVersions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "JournalEntry", request.getJournalEntryId(), "PruneJournalEntryVersions");
        log.atDebug().addKeyValue("auditValue", request.getJournalEntryId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PruneJournalEntryVersions", request.getJournalEntryId());
        return PruneJournalEntryVersionsResponse.getDefaultInstance();
    }
}
