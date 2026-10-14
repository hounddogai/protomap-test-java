package com.acme.finance.domains.assets;

import com.acme.assets.v1.AdvanceDepreciationWorkflowRequest;
import com.acme.assets.v1.AdvanceDepreciationWorkflowResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferCardSettlement;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.CancelDepreciationWorkflowRequest;
import com.acme.assets.v1.CancelDepreciationWorkflowResponse;
import com.acme.assets.v1.CommentFixedAssetApprovalRequest;
import com.acme.assets.v1.CommentFixedAssetApprovalResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationApprovalServiceGrpc;
import com.acme.assets.v1.DepreciationReportingServiceGrpc;
import com.acme.assets.v1.FetchDepreciationReportRequest;
import com.acme.assets.v1.FetchDepreciationReportResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetApprovalServiceGrpc;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.InspectDepreciationWorkflowRequest;
import com.acme.assets.v1.InspectDepreciationWorkflowResponse;
import com.acme.assets.v1.ListDepreciationWorkflowsRequest;
import com.acme.assets.v1.ListDepreciationWorkflowsResponse;
import com.acme.assets.v1.PauseDepreciationWorkflowRequest;
import com.acme.assets.v1.PauseDepreciationWorkflowResponse;
import com.acme.assets.v1.PullAssetTransfersRequest;
import com.acme.assets.v1.PullAssetTransfersResponse;
import com.acme.assets.v1.RejectFixedAssetRequest;
import com.acme.assets.v1.RejectFixedAssetResponse;
import com.acme.assets.v1.ResumeDepreciationWorkflowRequest;
import com.acme.assets.v1.ResumeDepreciationWorkflowResponse;
import com.acme.assets.v1.RetryDepreciationWorkflowRequest;
import com.acme.assets.v1.RetryDepreciationWorkflowResponse;
import com.acme.assets.v1.SearchFixedAssetsRequest;
import com.acme.assets.v1.SearchFixedAssetsResponse;
import com.acme.assets.v1.StartDepreciationWorkflowRequest;
import com.acme.assets.v1.StartDepreciationWorkflowResponse;
import com.acme.assets.v1.SubmitDepreciationRequest;
import com.acme.assets.v1.SubmitDepreciationResponse;
import com.acme.assets.v1.WatchFixedAssetsRequest;
import com.acme.assets.v1.WatchFixedAssetsResponse;
import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.NotifyAuditTrailWatchersRequest;
import com.acme.audit.v1.NotifyAuditTrailWatchersResponse;
import com.acme.collections.v1.ListPendingWriteOffsRequest;
import com.acme.collections.v1.ListPendingWriteOffsResponse;
import com.acme.collections.v1.WriteOffApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.devices.v1.ExplainManagedDeviceCalculationRequest;
import com.acme.devices.v1.ExplainManagedDeviceCalculationResponse;
import com.acme.devices.v1.ManagedDeviceCalculationServiceGrpc;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.keymanagement.v1.CryptoKeyServiceGrpc;
import com.acme.keymanagement.v1.SearchCryptoKeysRequest;
import com.acme.keymanagement.v1.SearchCryptoKeysResponse;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.ValidatePostingBatchRequest;
import com.acme.ledger.v1.ValidatePostingBatchResponse;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a depreciation from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class DepreciationWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(DepreciationWorkflowApplication.class);

    private final AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking;

    private final DepreciationApprovalServiceGrpc.DepreciationApprovalServiceBlockingStub assetsDepreciationApprovalServiceBlocking;

    private final DepreciationReportingServiceGrpc.DepreciationReportingServiceBlockingStub assetsDepreciationReportingServiceBlocking;

    private final FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceBlockingStub assetsFixedAssetApprovalServiceBlocking;

    private final FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking;

    private final AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking;

    private final WriteOffApprovalServiceGrpc.WriteOffApprovalServiceBlockingStub collectionsWriteOffApprovalServiceBlocking;

    private final ManagedDeviceCalculationServiceGrpc.ManagedDeviceCalculationServiceBlockingStub devicesManagedDeviceCalculationServiceBlocking;

    private final CryptoKeyServiceGrpc.CryptoKeyServiceBlockingStub keymanagementCryptoKeyServiceBlocking;

    private final PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking;

    private final DepreciationWorkflowRequestMapper mapper;

    private final AssetsRepository repository;

    private final Executor rpcExecutor;

    public DepreciationWorkflowApplication(
            AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking,
            DepreciationApprovalServiceGrpc.DepreciationApprovalServiceBlockingStub assetsDepreciationApprovalServiceBlocking,
            DepreciationReportingServiceGrpc.DepreciationReportingServiceBlockingStub assetsDepreciationReportingServiceBlocking,
            FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceBlockingStub assetsFixedAssetApprovalServiceBlocking,
            FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking,
            AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking,
            WriteOffApprovalServiceGrpc.WriteOffApprovalServiceBlockingStub collectionsWriteOffApprovalServiceBlocking,
            ManagedDeviceCalculationServiceGrpc.ManagedDeviceCalculationServiceBlockingStub devicesManagedDeviceCalculationServiceBlocking,
            CryptoKeyServiceGrpc.CryptoKeyServiceBlockingStub keymanagementCryptoKeyServiceBlocking,
            PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking,
            DepreciationWorkflowRequestMapper mapper,
            AssetsRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsAssetTransferSyncServiceBlocking = assetsAssetTransferSyncServiceBlocking;
        this.assetsDepreciationApprovalServiceBlocking = assetsDepreciationApprovalServiceBlocking;
        this.assetsDepreciationReportingServiceBlocking = assetsDepreciationReportingServiceBlocking;
        this.assetsFixedAssetApprovalServiceBlocking = assetsFixedAssetApprovalServiceBlocking;
        this.assetsFixedAssetServiceBlocking = assetsFixedAssetServiceBlocking;
        this.auditAuditTrailNotificationServiceBlocking = auditAuditTrailNotificationServiceBlocking;
        this.collectionsWriteOffApprovalServiceBlocking = collectionsWriteOffApprovalServiceBlocking;
        this.devicesManagedDeviceCalculationServiceBlocking = devicesManagedDeviceCalculationServiceBlocking;
        this.keymanagementCryptoKeyServiceBlocking = keymanagementCryptoKeyServiceBlocking;
        this.ledgerPostingBatchValidationServiceBlocking = ledgerPostingBatchValidationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Starts a depreciation.
     */
    public StartDepreciationWorkflowResponse startDepreciationWorkflow(StartDepreciationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartDepreciationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var futures0 = request.getDepreciation().getParticipantsList().stream()
                .map(element -> CommentFixedAssetApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentFixedAssetApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getDepreciation().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> assetsFixedAssetApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentFixedAssetApproval(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? CommentFixedAssetApprovalResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ListPendingWriteOffsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingWriteOffsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDepreciation().getAudit().getCreatedAt())
                        .setRegion(switch (reply0.getFixedAsset().getLocatorCase()) {
                            case ADDRESS -> reply0.getFixedAsset().getAddress().getRegion();
                            default -> "";
                        })
                        .build())
                .build();
        var reply1 = collectionsWriteOffApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingWriteOffs(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartDepreciationWorkflow", request.getDepreciation().getDepreciationId());
        return StartDepreciationWorkflowResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .setWindow(request.getDepreciation().getWindow())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Advances a depreciation.
     */
    public AdvanceDepreciationWorkflowResponse advanceDepreciationWorkflow(AdvanceDepreciationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceDepreciationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var outbound0 = mapper.advanceDepreciationWorkflowStage0(request);
        var reply0 = assetsDepreciationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).fetchDepreciationReport(outbound0);

        var outbound1 = SubmitDepreciationRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getDepreciation().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.assets.v1.DepreciationApprovalService/SubmitDepreciation", outbound1);
        var reply1 = assetsDepreciationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitDepreciation(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getTitle()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceDepreciationWorkflow", request.getDepreciation().getTitle());
        return AdvanceDepreciationWorkflowResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getDepreciation().getAudit().getEtag())
                                .setUpdatedAt(reply1.getDepreciation().getAudit().getUpdatedAt())
                                .build())
                        .setDepreciationId(request.getDepreciationId())
                        .putAllMetadata(request.getDepreciation().getMetadataMap())
                        .build())
                .build();
    }

    /**
     * Cancels a depreciation.
     */
    public CancelDepreciationWorkflowResponse cancelDepreciationWorkflow(CancelDepreciationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelDepreciationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var outbound0 = SearchCryptoKeysRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchCryptoKeysRequest.Filter.newBuilder()
                        .setOwnerId(request.getDepreciation().getOrganizer().getUserId())
                        .setRegion(request.getDepreciation().getLocation().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> keymanagementCryptoKeyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchCryptoKeys(outbound0), rpcExecutor);

        var outbound1 = ValidatePostingBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getDepreciation().getLocation().getCountryCode())
                                .setPostalCode(request.getDepreciation().getLocation().getPostalCode())
                                .setRecipient(request.getDepreciation().getLocation().getRecipient())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> ledgerPostingBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePostingBatch(outbound1), rpcExecutor);

        var outbound2 = RejectFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RejectFixedAssetRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getDepreciation().getOrganizer().getDisplayName())
                                .setEmail(request.getDepreciation().getOrganizer().getEmail())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getDepreciation().getLocation().getPostalCode())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getDepreciation().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var pending2 = CompletableFuture.supplyAsync(() -> assetsFixedAssetApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectFixedAsset(outbound2), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1, pending2).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        var reply2 = pending2.join();
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelDepreciationWorkflow", request.getDepreciation().getDepreciationId());
        return CancelDepreciationWorkflowResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getPostingBatch().getAudit().getEtag())
                                .build())
                        .setDepreciationId(request.getDepreciationId())
                        .setLocation(request.getDepreciation().getLocation())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Retrys a depreciation.
     */
    public RetryDepreciationWorkflowResponse retryDepreciationWorkflow(RetryDepreciationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryDepreciationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var futures0 = request.getDepreciation().getParticipantsList().stream()
                .map(element -> NotifyAuditTrailWatchersRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NotifyAuditTrailWatchersRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .setName(element.getContact().getName())
                                .setPhone(element.getContact().getPhone())
                                .setPreferredChannel(element.getContact().getPreferredChannel())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> auditAuditTrailNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).notifyAuditTrailWatchers(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? NotifyAuditTrailWatchersResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = PullAssetTransfersRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getDepreciation().getLocation().getCountryCode())
                                .build())
                        .setCard(AssetTransferCardSettlement.newBuilder()
                                .setHolderName(switch (reply0.getAuditTrail().getSettlementCase()) {
                                    case CARD -> reply0.getAuditTrail().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(FixedAssetRef.newBuilder()
                                .setDisplayName(request.getDepreciation().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PullAssetTransfersRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply1 = assetsAssetTransferSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullAssetTransfers(outbound1);

        var results2 = new ArrayList<WatchFixedAssetsResponse>();
        for (var element : request.getDepreciation().getParticipantsList()) {
            WatchFixedAssetsRequest itemRequest = WatchFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setPreferredChannel(element.getContact().getPreferredChannel())
                        .build())
                .build();
            var responses = assetsFixedAssetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchFixedAssets(itemRequest);
            responses.forEachRemaining(results2::add);
        }
        var reply2 = results2.isEmpty() ? WatchFixedAssetsResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryDepreciationWorkflow", request.getDepreciation().getDepreciationId());
        return RetryDepreciationWorkflowResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .setTitle(request.getDepreciation().getTitle())
                        .build())
                .setOutcome(RetryDepreciationWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Pauses a depreciation.
     */
    public PauseDepreciationWorkflowResponse pauseDepreciationWorkflow(PauseDepreciationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseDepreciationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var outbound0 = ValidatePostingBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidatePostingBatchRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .build())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setCounterparty(LedgerAccountRef.newBuilder()
                                .setDisplayName(request.getDepreciation().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.ledger.v1.PostingBatchValidationService/ValidatePostingBatch", outbound0);
        var reply0 = ledgerPostingBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePostingBatch(command0.payload());
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseDepreciationWorkflow", request.getDepreciation().getDepreciationId());
        return PauseDepreciationWorkflowResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getPostingBatch().getAudit().getCreatedBy())
                                .build())
                        .setDepreciationId(request.getDepreciationId())
                        .putAllMetadata(request.getDepreciation().getMetadataMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Resumes a depreciation.
     */
    public ResumeDepreciationWorkflowResponse resumeDepreciationWorkflow(ResumeDepreciationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeDepreciationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var outbound0 = ExplainManagedDeviceCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainManagedDeviceCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> devicesManagedDeviceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainManagedDeviceCalculation(value));
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeDepreciationWorkflow", request.getDepreciation().getDepreciationId());
        return ResumeDepreciationWorkflowResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getManagedDevice().getAudit().getUpdatedAt())
                                .build())
                        .setDepreciationId(request.getDepreciationId())
                        .setStatus(request.getDepreciation().getStatus())
                        .build())
                .setOutcome(ResumeDepreciationWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a depreciation.
     */
    public InspectDepreciationWorkflowResponse inspectDepreciationWorkflow(InspectDepreciationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectDepreciationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getDepreciationId(), "InspectDepreciationWorkflow");

        var outbound0 = JsonCodec.roundTrip(ValidatePostingBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidatePostingBatchRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build(), ValidatePostingBatchRequest.class);
        var reply0 = ledgerPostingBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePostingBatch(outbound0);
        return InspectDepreciationWorkflowResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getPostingBatch().getAudit().getCreatedBy())
                                .build())
                        .setDepreciationId(request.getDepreciationId())
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(reply0.getPostingBatch().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists depreciations a page at a time.
     */
    public ListDepreciationWorkflowsResponse listDepreciationWorkflows(ListDepreciationWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListDepreciationWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getContext().getTraceId(), "ListDepreciationWorkflows");

        var outbound0 = SearchFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchFixedAssetsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .setPage(PageRequest.newBuilder()
                        .setPageSize(request.getPage().getPageSize())
                        .setPageToken(request.getPage().getPageToken())
                        .build())
                .build();
        RpcPort<SearchFixedAssetsRequest, SearchFixedAssetsResponse> port0 = RpcPort.adapt(assetsFixedAssetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::searchFixedAssets);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListDepreciationWorkflows", request.getFilter().getQuery());
        return ListDepreciationWorkflowsResponse.newBuilder()
                .setPageInfo(PageInfo.newBuilder()
                        .setTotalSize(reply0.getPageInfo().getTotalSize())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
