package com.acme.finance.domains.assets;

import com.acme.assets.v1.ApproveAssetTransferRequest;
import com.acme.assets.v1.ApproveAssetTransferResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferLine;
import com.acme.assets.v1.AssetTransferLineTax;
import com.acme.assets.v1.AssetTransferReportingServiceGrpc;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.DelegateAssetTransferApprovalRequest;
import com.acme.assets.v1.DelegateAssetTransferApprovalResponse;
import com.acme.assets.v1.DeleteAssetTransferReportRequest;
import com.acme.assets.v1.DeleteAssetTransferReportResponse;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.EscalateAssetTransferRequest;
import com.acme.assets.v1.EscalateAssetTransferResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetApprovalServiceGrpc;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.FixedAssetLifecycleServiceGrpc;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.GetAssetTransferApprovalStatusRequest;
import com.acme.assets.v1.GetAssetTransferApprovalStatusResponse;
import com.acme.assets.v1.GetFixedAssetApprovalStatusRequest;
import com.acme.assets.v1.GetFixedAssetApprovalStatusResponse;
import com.acme.assets.v1.ListPendingAssetTransfersRequest;
import com.acme.assets.v1.ListPendingAssetTransfersResponse;
import com.acme.assets.v1.RejectAssetTransferRequest;
import com.acme.assets.v1.RejectAssetTransferResponse;
import com.acme.assets.v1.ScheduleFixedAssetExpiryRequest;
import com.acme.assets.v1.ScheduleFixedAssetExpiryResponse;
import com.acme.assets.v1.ShareAssetTransferReportRequest;
import com.acme.assets.v1.ShareAssetTransferReportResponse;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.assets.v1.SubmitAssetTransferRequest;
import com.acme.assets.v1.SubmitAssetTransferResponse;
import com.acme.assets.v1.SyncAssetTransfersRequest;
import com.acme.assets.v1.SyncAssetTransfersResponse;
import com.acme.collections.v1.CommentWriteOffApprovalRequest;
import com.acme.collections.v1.CommentWriteOffApprovalResponse;
import com.acme.collections.v1.ListPendingPaymentPromisesRequest;
import com.acme.collections.v1.ListPendingPaymentPromisesResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.PaymentPromiseWorkflowServiceGrpc;
import com.acme.collections.v1.ResumePaymentPromiseWorkflowRequest;
import com.acme.collections.v1.ResumePaymentPromiseWorkflowResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.documents.v1.ListDocumentsRequest;
import com.acme.documents.v1.ListDocumentsResponse;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CommentPolicyApprovalRequest;
import com.acme.insurance.v1.CommentPolicyApprovalResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageApprovalServiceGrpc;
import com.acme.insurance.v1.EscalateCoverageRequest;
import com.acme.insurance.v1.EscalateCoverageResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.PolicyBankFunding;
import com.acme.ledger.v1.AnnotatePostingBatchVersionRequest;
import com.acme.ledger.v1.AnnotatePostingBatchVersionResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchHistoryServiceGrpc;
import com.acme.ledger.v1.PostingBatchLine;
import com.acme.ledger.v1.PostingBatchLineTax;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.lending.v1.ShareRepaymentReportRequest;
import com.acme.lending.v1.ShareRepaymentReportResponse;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsRequest;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsResponse;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.google.protobuf.Any;
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
 * Routes asset transfers through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class AssetTransferApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(AssetTransferApprovalApplication.class);

    private final AssetTransferReportingServiceGrpc.AssetTransferReportingServiceBlockingStub assetsAssetTransferReportingServiceBlocking;

    private final AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking;

    private final DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync;

    private final FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceBlockingStub assetsFixedAssetApprovalServiceBlocking;

    private final FixedAssetLifecycleServiceGrpc.FixedAssetLifecycleServiceBlockingStub assetsFixedAssetLifecycleServiceBlocking;

    private final PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceFutureStub collectionsPaymentPromiseApprovalServiceFuture;

    private final PaymentPromiseWorkflowServiceGrpc.PaymentPromiseWorkflowServiceBlockingStub collectionsPaymentPromiseWorkflowServiceBlocking;

    private final WriteOffApprovalServiceGrpc.WriteOffApprovalServiceBlockingStub collectionsWriteOffApprovalServiceBlocking;

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking;

    private final CoverageApprovalServiceGrpc.CoverageApprovalServiceBlockingStub insuranceCoverageApprovalServiceBlocking;

    private final PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking;

    private final PostingBatchHistoryServiceGrpc.PostingBatchHistoryServiceBlockingStub ledgerPostingBatchHistoryServiceBlocking;

    private final RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking;

    private final AssetTransferApprovalRequestMapper mapper;

    private final PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceStub procurementPurchaseRequisitionServiceAsync;

    private final AssetsRepository repository;

    private final Executor rpcExecutor;

    public AssetTransferApprovalApplication(
            AssetTransferReportingServiceGrpc.AssetTransferReportingServiceBlockingStub assetsAssetTransferReportingServiceBlocking,
            AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking,
            DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync,
            FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceBlockingStub assetsFixedAssetApprovalServiceBlocking,
            FixedAssetLifecycleServiceGrpc.FixedAssetLifecycleServiceBlockingStub assetsFixedAssetLifecycleServiceBlocking,
            PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceFutureStub collectionsPaymentPromiseApprovalServiceFuture,
            PaymentPromiseWorkflowServiceGrpc.PaymentPromiseWorkflowServiceBlockingStub collectionsPaymentPromiseWorkflowServiceBlocking,
            WriteOffApprovalServiceGrpc.WriteOffApprovalServiceBlockingStub collectionsWriteOffApprovalServiceBlocking,
            DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking,
            CoverageApprovalServiceGrpc.CoverageApprovalServiceBlockingStub insuranceCoverageApprovalServiceBlocking,
            PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking,
            PostingBatchHistoryServiceGrpc.PostingBatchHistoryServiceBlockingStub ledgerPostingBatchHistoryServiceBlocking,
            RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking,
            AssetTransferApprovalRequestMapper mapper,
            PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceStub procurementPurchaseRequisitionServiceAsync,
            AssetsRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsAssetTransferReportingServiceBlocking = assetsAssetTransferReportingServiceBlocking;
        this.assetsAssetTransferSyncServiceBlocking = assetsAssetTransferSyncServiceBlocking;
        this.assetsDisposalSyncServiceAsync = assetsDisposalSyncServiceAsync;
        this.assetsFixedAssetApprovalServiceBlocking = assetsFixedAssetApprovalServiceBlocking;
        this.assetsFixedAssetLifecycleServiceBlocking = assetsFixedAssetLifecycleServiceBlocking;
        this.collectionsPaymentPromiseApprovalServiceFuture = collectionsPaymentPromiseApprovalServiceFuture;
        this.collectionsPaymentPromiseWorkflowServiceBlocking = collectionsPaymentPromiseWorkflowServiceBlocking;
        this.collectionsWriteOffApprovalServiceBlocking = collectionsWriteOffApprovalServiceBlocking;
        this.documentsDocumentServiceBlocking = documentsDocumentServiceBlocking;
        this.insuranceCoverageApprovalServiceBlocking = insuranceCoverageApprovalServiceBlocking;
        this.insurancePolicyApprovalServiceBlocking = insurancePolicyApprovalServiceBlocking;
        this.ledgerPostingBatchHistoryServiceBlocking = ledgerPostingBatchHistoryServiceBlocking;
        this.lendingRepaymentReportingServiceBlocking = lendingRepaymentReportingServiceBlocking;
        this.mapper = mapper;
        this.procurementPurchaseRequisitionServiceAsync = procurementPurchaseRequisitionServiceAsync;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Submits an asset transfer.
     */
    public SubmitAssetTransferResponse submitAssetTransfer(SubmitAssetTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitAssetTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = mapper.submitAssetTransferStage0(request);
        var reply0 = insurancePolicyApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentPolicyApproval(outbound0);

        var outbound1 = ListPendingPaymentPromisesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingPaymentPromisesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getAssetTransfer().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getPolicy().getLabelsMap())
                        .setRegion(request.getAssetTransfer().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending1 = collectionsPaymentPromiseApprovalServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingPaymentPromises(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = BatchCreatePurchaseRequisitionsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreatePurchaseRequisitionsRequest.StreamHeader.newBuilder()
                        .setSource(request.getAssetTransfer().getAssetTransferId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreatePurchaseRequisitionsResponse.getDefaultInstance());
        var input2 = procurementPurchaseRequisitionServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreatePurchaseRequisitions(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamDisposalChangesRequest.newBuilder()
                .setChange(Disposal.newBuilder()
                        .setAssignee(request.getDetails().getApprover())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getAssetTransfer().getAudit().getCreatedAt())
                                .setCreatedBy(request.getAssetTransfer().getAudit().getCreatedBy())
                                .setEtag(request.getAssetTransfer().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamDisposalChangesResponse.getDefaultInstance());
        var input3 = assetsDisposalSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamDisposalChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitAssetTransfer", request.getAssetTransfer().getNumber());
        return SubmitAssetTransferResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setBillingAddress(request.getAssetTransfer().getBillingAddress())
                        .build())
                .setOutcome(SubmitAssetTransferResponse.Outcome.newBuilder()
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Approves an asset transfer.
     */
    public ApproveAssetTransferResponse approveAssetTransfer(ApproveAssetTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveAssetTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = DeleteAssetTransferReportRequest.newBuilder()
                .setAssetTransferId(request.getAssetTransfer().getAssetTransferId())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> assetsAssetTransferReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).deleteAssetTransferReport(outbound0), rpcExecutor);

        var outbound1 = ResumePaymentPromiseWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getAssetTransfer().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getAssetTransfer().getAudit().getUpdatedBy())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setPostalCode(request.getAssetTransfer().getBillingAddress().getPostalCode())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> collectionsPaymentPromiseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumePaymentPromiseWorkflow(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        List<AnnotatePostingBatchVersionResponse> results2 = request.getAssetTransfer().getLinesList().stream()
                .map(element -> AnnotatePostingBatchVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AnnotatePostingBatchVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setPostingBatch(PostingBatch.newBuilder()
                        .addLines(PostingBatchLine.newBuilder()
                                .setTax(PostingBatchLineTax.newBuilder()
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(ledgerPostingBatchHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotatePostingBatchVersion(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? AnnotatePostingBatchVersionResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveAssetTransfer", request.getAssetTransfer().getNumber());
        return ApproveAssetTransferResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .putAllMetadata(request.getAssetTransfer().getMetadataMap())
                        .build())
                .setOutcome(ApproveAssetTransferResponse.Outcome.newBuilder()
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Rejects an asset transfer.
     */
    public RejectAssetTransferResponse rejectAssetTransfer(RejectAssetTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectAssetTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        List<ScheduleFixedAssetExpiryResponse> results0 = request.getAssetTransfer().getLinesList().stream()
                .map(element -> ScheduleFixedAssetExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getAssetTransfer().getBillingAddress().getRecipient())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getAssetTransfer().getAudit().getUpdatedAt())
                                .build())
                        .putComponents("primary", FixedAssetComponent.newBuilder()
                                .setDescription(element.getDescription())
                                .setQuantity(element.getQuantity())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(assetsFixedAssetLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleFixedAssetExpiry(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? ScheduleFixedAssetExpiryResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = GetFixedAssetApprovalStatusRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAssetId(request.getAssetTransfer().getCounterparty().getFixedAssetId())
                .build();
        RpcPort<GetFixedAssetApprovalStatusRequest, GetFixedAssetApprovalStatusResponse> port1 = RpcPort.adapt(assetsFixedAssetApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::getFixedAssetApprovalStatus);
        var reply1 = port1.exchange(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectAssetTransfer", request.getAssetTransfer().getAssetTransferId());
        return RejectAssetTransferResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setCounterparty(FixedAssetRef.newBuilder()
                                .setDisplayName(reply1.getFixedAsset().getOwner().getDisplayName())
                                .build())
                        .setDueAt(request.getAssetTransfer().getDueAt())
                        .build())
                .build();
    }

    /**
     * Escalates an asset transfer.
     */
    public EscalateAssetTransferResponse escalateAssetTransfer(EscalateAssetTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateAssetTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = ShareRepaymentReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareRepaymentReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> lendingRepaymentReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareRepaymentReport(outbound0), rpcExecutor);

        var outbound1 = ShareAssetTransferReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setAssetTransferId(request.getAssetTransfer().getAssetTransferId())
                .setContext(request.getContext())
                .setDetails(ShareAssetTransferReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> assetsAssetTransferReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareAssetTransferReport(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = JsonCodec.roundTrip(ListDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListDocumentsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getAssetTransfer().getAudit().getCreatedAt())
                        .setOwnerId(reply1.getAssetTransfer().getCounterparty().getFixedAssetId())
                        .setRegion(reply1.getAssetTransfer().getBillingAddress().getRegion())
                        .build())
                .build(), ListDocumentsRequest.class);
        var reply2 = documentsDocumentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listDocuments(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateAssetTransfer", request.getAssetTransfer().getNumber());
        return EscalateAssetTransferResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setCounterparty(request.getAssetTransfer().getCounterparty())
                        .build())
                .setOutcome(EscalateAssetTransferResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists asset transfers a page at a time.
     */
    public ListPendingAssetTransfersResponse listPendingAssetTransfers(ListPendingAssetTransfersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingAssetTransfers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getContext().getTraceId(), "ListPendingAssetTransfers");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingAssetTransfers", request.getPage().getPageToken());
        return ListPendingAssetTransfersResponse.getDefaultInstance();
    }

    /**
     * Delegates an asset transfer.
     */
    public DelegateAssetTransferApprovalResponse delegateAssetTransferApproval(DelegateAssetTransferApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateAssetTransferApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = (CommentWriteOffApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getAssetTransfer().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getAssetTransfer().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = collectionsWriteOffApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentWriteOffApproval(outbound0);

        var outbound1 = EscalateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getWriteOff().getAudit().getCreatedBy())
                                .setEtag(reply0.getWriteOff().getAudit().getEtag())
                                .build())
                        .build())
                .setDetails(EscalateCoverageRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getAssetTransfer().getCounterparty().getDisplayName())
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> insuranceCoverageApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateCoverage(value));
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateAssetTransferApproval", request.getAssetTransfer().getAssetTransferId());
        return DelegateAssetTransferApprovalResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getWriteOff().getAudit().getCreatedBy())
                                .build())
                        .addLines(AssetTransferLine.newBuilder()
                                .setTax(AssetTransferLineTax.newBuilder()
                                        .setAmount(reply1.getCoverage().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .setOutcome(DelegateAssetTransferApprovalResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns an asset transfer.
     */
    public GetAssetTransferApprovalStatusResponse getAssetTransferApprovalStatus(GetAssetTransferApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAssetTransferApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getAssetTransferId(), "GetAssetTransferApprovalStatus");

        var outbound0 = SyncAssetTransfersRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .build())
                .setContext(request.getContext())
                .setDetails(SyncAssetTransfersRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        Function<SyncAssetTransfersRequest, SyncAssetTransfersResponse> operation0 = assetsAssetTransferSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::syncAssetTransfers;
        var reply0 = operation0.apply(outbound0);
        return GetAssetTransferApprovalStatusResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getAssetTransfer().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(reply0.getAssetTransfer().getBillingAddress())
                        .build())
                .build();
    }
}
