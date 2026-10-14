package com.acme.finance.domains.ledger;

import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.featureflags.v1.CheckFeatureFlagEligibilityRequest;
import com.acme.featureflags.v1.CheckFeatureFlagEligibilityResponse;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.AnnotatePostingBatchVersionRequest;
import com.acme.ledger.v1.AnnotatePostingBatchVersionResponse;
import com.acme.ledger.v1.ComparePostingBatchVersionsRequest;
import com.acme.ledger.v1.ComparePostingBatchVersionsResponse;
import com.acme.ledger.v1.GetPostingBatchVersionRequest;
import com.acme.ledger.v1.GetPostingBatchVersionResponse;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.ListPostingBatchVersionsRequest;
import com.acme.ledger.v1.ListPostingBatchVersionsResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.PrunePostingBatchVersionsRequest;
import com.acme.ledger.v1.PrunePostingBatchVersionsResponse;
import com.acme.ledger.v1.RecordPostingBatchVersionRequest;
import com.acme.ledger.v1.RecordPostingBatchVersionResponse;
import com.acme.ledger.v1.RevertPostingBatchRequest;
import com.acme.ledger.v1.RevertPostingBatchResponse;
import com.acme.ledger.v1.StreamLedgerAccountNoticesRequest;
import com.acme.ledger.v1.StreamLedgerAccountNoticesResponse;
import com.acme.ledger.v1.StreamPostingBatchNoticesRequest;
import com.acme.ledger.v1.StreamPostingBatchNoticesResponse;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Keeps every version of a posting batch and restores earlier ones. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class PostingBatchHistoryApplication {
    private static final Logger log = LoggerFactory.getLogger(PostingBatchHistoryApplication.class);

    private final FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking;

    private final LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking;

    private final PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking;

    private final LedgerRepository repository;

    public PostingBatchHistoryApplication(
            FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking,
            LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking,
            PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking,
            LedgerRepository repository) {
        this.featureflagsFeatureFlagValidationServiceBlocking = featureflagsFeatureFlagValidationServiceBlocking;
        this.ledgerLedgerAccountNotificationServiceBlocking = ledgerLedgerAccountNotificationServiceBlocking;
        this.ledgerPostingBatchNotificationServiceBlocking = ledgerPostingBatchNotificationServiceBlocking;
        this.repository = repository;
    }

    /**
     * Records a posting batch.
     */
    public RecordPostingBatchVersionResponse recordPostingBatchVersion(RecordPostingBatchVersionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecordPostingBatchVersion")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePostingBatch(request.getContext().getTenantId(), request.getPostingBatch());
        log.atDebug().addKeyValue("auditValue", request.getPostingBatch().getPostingBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecordPostingBatchVersion", request.getPostingBatch().getPostingBatchId());
        return RecordPostingBatchVersionResponse.newBuilder()
                .setPostingBatch(PostingBatch.newBuilder()
                        .setNumber(request.getPostingBatch().getNumber())
                        .setPostingBatchId(request.getPostingBatchId())
                        .build())
                .build();
    }

    /**
     * Lists posting batches a page at a time.
     */
    public ListPostingBatchVersionsResponse listPostingBatchVersions(ListPostingBatchVersionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPostingBatchVersions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PostingBatch", request.getContext().getTraceId(), "ListPostingBatchVersions");

        var outbound0 = StreamPostingBatchNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamPostingBatchNoticesRequest.Filter.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setQuery(request.getFilter().getQuery())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var stream0 = ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamPostingBatchNotices(outbound0);
        var reply0 = StreamPostingBatchNoticesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPostingBatchVersions", request.getPage().getPageToken());
        return ListPostingBatchVersionsResponse.getDefaultInstance();
    }

    /**
     * Returns a posting batch.
     */
    public GetPostingBatchVersionResponse getPostingBatchVersion(GetPostingBatchVersionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPostingBatchVersion")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PostingBatch", request.getPostingBatchId(), "GetPostingBatchVersion");

        var outbound0 = CheckFeatureFlagEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckFeatureFlagEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = featureflagsFeatureFlagValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkFeatureFlagEligibility(outbound0);
        return GetPostingBatchVersionResponse.newBuilder()
                .setPostingBatch(PostingBatch.newBuilder()
                        .setPostingBatchId(request.getPostingBatchId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Compares a posting batch.
     */
    public ComparePostingBatchVersionsResponse comparePostingBatchVersions(ComparePostingBatchVersionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ComparePostingBatchVersions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePostingBatch(request.getContext().getTenantId(), request.getPostingBatch());
        log.atDebug().addKeyValue("auditValue", request.getPostingBatch().getPostingBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ComparePostingBatchVersions", request.getPostingBatch().getPostingBatchId());
        return ComparePostingBatchVersionsResponse.newBuilder()
                .setPostingBatch(PostingBatch.newBuilder()
                        .setNumber(request.getPostingBatch().getNumber())
                        .setPostingBatchId(request.getPostingBatchId())
                        .build())
                .build();
    }

    /**
     * Reverts a posting batch.
     */
    public RevertPostingBatchResponse revertPostingBatch(RevertPostingBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RevertPostingBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePostingBatch(request.getContext().getTenantId(), request.getPostingBatch());
        log.atDebug().addKeyValue("auditValue", request.getPostingBatch().getPostingBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RevertPostingBatch", request.getPostingBatch().getPostingBatchId());
        return RevertPostingBatchResponse.newBuilder()
                .setPostingBatch(PostingBatch.newBuilder()
                        .setPostingBatchId(request.getPostingBatchId())
                        .setTotal(request.getPostingBatch().getTotal())
                        .build())
                .build();
    }

    /**
     * Annotates a posting batch.
     */
    public AnnotatePostingBatchVersionResponse annotatePostingBatchVersion(AnnotatePostingBatchVersionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AnnotatePostingBatchVersion")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePostingBatch(request.getContext().getTenantId(), request.getPostingBatch());

        var outbound0 = StreamLedgerAccountNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamLedgerAccountNoticesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPostingBatch().getIssuedAt())
                        .setRegion(request.getPostingBatch().getBillingAddress().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .build();
        var stream0 = ledgerLedgerAccountNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamLedgerAccountNotices(outbound0);
        var reply0 = StreamLedgerAccountNoticesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getPostingBatch().getPostingBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AnnotatePostingBatchVersion", request.getPostingBatch().getPostingBatchId());
        return AnnotatePostingBatchVersionResponse.newBuilder()
                .setPostingBatch(PostingBatch.newBuilder()
                        .setBillingAddress(request.getPostingBatch().getBillingAddress())
                        .setPostingBatchId(request.getPostingBatchId())
                        .build())
                .build();
    }

    /**
     * Deletes a posting batch.
     */
    public PrunePostingBatchVersionsResponse prunePostingBatchVersions(PrunePostingBatchVersionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PrunePostingBatchVersions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PostingBatch", request.getPostingBatchId(), "PrunePostingBatchVersions");
        log.atDebug().addKeyValue("auditValue", request.getPostingBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PrunePostingBatchVersions", request.getPostingBatchId());
        return PrunePostingBatchVersionsResponse.getDefaultInstance();
    }
}
