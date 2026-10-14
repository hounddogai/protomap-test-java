package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.DepreciationReportingServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.EstimateFixedAssetRequest;
import com.acme.assets.v1.EstimateFixedAssetResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.GetDisposalSyncStateRequest;
import com.acme.assets.v1.GetDisposalSyncStateResponse;
import com.acme.assets.v1.LintAssetTransfersRequest;
import com.acme.assets.v1.LintAssetTransfersResponse;
import com.acme.assets.v1.ListDisposalConflictsRequest;
import com.acme.assets.v1.ListDisposalConflictsResponse;
import com.acme.assets.v1.PushDisposalRequest;
import com.acme.assets.v1.PushDisposalResponse;
import com.acme.assets.v1.ReconcileDisposalsRequest;
import com.acme.assets.v1.ReconcileDisposalsResponse;
import com.acme.assets.v1.StreamDepreciationMetricsRequest;
import com.acme.assets.v1.StreamDepreciationMetricsResponse;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.assets.v1.SyncDisposalsRequest;
import com.acme.assets.v1.SyncDisposalsResponse;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.ScheduleAuditEventExportRequest;
import com.acme.audit.v1.ScheduleAuditEventExportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.dispatch.v1.DispatchOrderServiceGrpc;
import com.acme.dispatch.v1.SearchDispatchOrdersRequest;
import com.acme.dispatch.v1.SearchDispatchOrdersResponse;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.FindDocumentsRequest;
import com.acme.documents.v1.FindDocumentsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.SubmitClaimRequest;
import com.acme.insurance.v1.SubmitClaimResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodParticipant;
import com.acme.ledger.v1.RevertFiscalPeriodRequest;
import com.acme.ledger.v1.RevertFiscalPeriodResponse;
import com.acme.ledger.v1.StreamFiscalPeriodExportRequest;
import com.acme.ledger.v1.StreamFiscalPeriodExportResponse;
import com.acme.lending.v1.ApproveLoanRequest;
import com.acme.lending.v1.ApproveLoanResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.metrics.v1.CheckMetricSeriesEligibilityRequest;
import com.acme.metrics.v1.CheckMetricSeriesEligibilityResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Keeps disposals in step with the systems of record in other regions. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class DisposalSyncApplication {
    private static final Logger log = LoggerFactory.getLogger(DisposalSyncApplication.class);

    private final AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking;

    private final DepreciationReportingServiceGrpc.DepreciationReportingServiceBlockingStub assetsDepreciationReportingServiceBlocking;

    private final AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking;

    private final ChannelRegistry channels;

    private final DispatchOrderServiceGrpc.DispatchOrderServiceBlockingStub dispatchDispatchOrderServiceBlocking;

    private final DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking;

    private final ClaimApprovalServiceGrpc.ClaimApprovalServiceBlockingStub insuranceClaimApprovalServiceBlocking;

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking;

    private final FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking;

    private final LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking;

    private final MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking;

    private final AssetsRepository repository;

    private final Executor rpcExecutor;

    public DisposalSyncApplication(
            AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking,
            DepreciationReportingServiceGrpc.DepreciationReportingServiceBlockingStub assetsDepreciationReportingServiceBlocking,
            AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking,
            ChannelRegistry channels,
            DispatchOrderServiceGrpc.DispatchOrderServiceBlockingStub dispatchDispatchOrderServiceBlocking,
            DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking,
            ClaimApprovalServiceGrpc.ClaimApprovalServiceBlockingStub insuranceClaimApprovalServiceBlocking,
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking,
            FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking,
            LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking,
            MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking,
            AssetsRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsAssetTransferValidationServiceBlocking = assetsAssetTransferValidationServiceBlocking;
        this.assetsDepreciationReportingServiceBlocking = assetsDepreciationReportingServiceBlocking;
        this.auditAuditEventExportServiceBlocking = auditAuditEventExportServiceBlocking;
        this.channels = channels;
        this.dispatchDispatchOrderServiceBlocking = dispatchDispatchOrderServiceBlocking;
        this.documentsDocumentQueryServiceBlocking = documentsDocumentQueryServiceBlocking;
        this.insuranceClaimApprovalServiceBlocking = insuranceClaimApprovalServiceBlocking;
        this.ledgerFiscalPeriodExportServiceBlocking = ledgerFiscalPeriodExportServiceBlocking;
        this.ledgerFiscalPeriodHistoryServiceBlocking = ledgerFiscalPeriodHistoryServiceBlocking;
        this.lendingLoanApprovalServiceBlocking = lendingLoanApprovalServiceBlocking;
        this.metricsMetricSeriesValidationServiceBlocking = metricsMetricSeriesValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Syncs a disposal.
     */
    public SyncDisposalsResponse syncDisposals(SyncDisposalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SyncDisposals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDisposal(request.getContext().getTenantId(), request.getDisposal());

        var outbound0 = ApproveLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDisposal().getRequester().getName())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(lendingLoanApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveLoan(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getDisposal().getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SyncDisposals", request.getDisposal().getDisposalId());
        return SyncDisposalsResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getLoan().getAudit().getCreatedAt())
                                .build())
                        .setDisposalId(request.getDisposalId())
                        .setSubject(request.getDisposal().getSubject())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Pushs a disposal.
     */
    public PushDisposalResponse pushDisposal(PushDisposalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PushDisposal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDisposal(request.getContext().getTenantId(), request.getDisposal());

        var builder0 = RevertFiscalPeriodRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(RevertFiscalPeriodRequest.Details.newBuilder()
                .setSnapshot(JsonCodec.encode(request.getContext()))
                .build());
        builder0.setFiscalPeriod(FiscalPeriod.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedBy(request.getDisposal().getAudit().getUpdatedBy())
                        .build())
                .setOrganizer(UserRef.newBuilder()
                        .setDisplayName(request.getDisposal().getAssignee().getDisplayName())
                        .build())
                .addParticipants(FiscalPeriodParticipant.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDisposal().getAssignee().getEmail())
                                .setName(request.getDisposal().getRequester().getName())
                                .build())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = ledgerFiscalPeriodHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertFiscalPeriod(outbound0);

        var outbound1 = FindDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindDocumentsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDisposal().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .setRegion(reply0.getFiscalPeriod().getLocation().getRegion())
                        .build())
                .build();
        RpcPort<FindDocumentsRequest, FindDocumentsResponse> port1 = RpcPort.adapt(documentsDocumentQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::findDocuments);
        var reply1 = port1.exchange(outbound1);

        var futures2 = request.getDisposal().getCommentsList().stream()
                .map(element -> LintAssetTransfersRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getFiscalPeriod().getLocation().getLinesList())
                                .setRegion(reply0.getFiscalPeriod().getLocation().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(LintAssetTransfersRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> assetsAssetTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintAssetTransfers(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? LintAssetTransfersResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getDisposal().getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PushDisposal", request.getDisposal().getDisposalId());
        return PushDisposalResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(request.getDisposal().getAssignee())
                        .setDisposalId(request.getDisposalId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Reconciles a disposal.
     */
    public ReconcileDisposalsResponse reconcileDisposals(ReconcileDisposalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReconcileDisposals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDisposal(request.getContext().getTenantId(), request.getDisposal());

        var outbound0 = ScheduleAuditEventExportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .build();
        var reply0 = auditAuditEventExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleAuditEventExport(outbound0);

        var outbound1 = StreamFiscalPeriodExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamFiscalPeriodExportRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDisposal().getSla().getDueAt())
                        .build())
                .build();
        var stream1 = ledgerFiscalPeriodExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamFiscalPeriodExport(outbound1);
        var reply1 = StreamFiscalPeriodExportResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getDisposal().getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReconcileDisposals", request.getDisposal().getDisposalId());
        return ReconcileDisposalsResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(request.getDisposal().getAssignee())
                        .setDisposalId(request.getDisposalId())
                        .build())
                .setOutcome(ReconcileDisposalsResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Exchanges changes of disposals with a peer.
     */
    public StreamDisposalChangesResponse streamDisposalChanges(StreamDisposalChangesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamDisposalChanges")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasChange()) {
            repository.saveDisposal(request.getContext().getTenantId(), request.getChange());
        }

        var outbound0 = SearchDispatchOrdersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchDispatchOrdersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getChange().getAudit().getCreatedAt())
                        .putAllLabels(request.getChange().getCustomFieldsMap())
                        .setOwnerId(request.getChange().getAssignee().getUserId())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> dispatchDispatchOrderServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchDispatchOrders(value));

        var outbound1 = EstimateFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getChange().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getChange().getCustomer().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.assets.v1.FixedAssetCalculationService");
        var selected1 = FixedAssetCalculationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.estimateFixedAsset(outbound1);

        var results2 = new ArrayList<StreamDepreciationMetricsResponse>();
        for (var element : request.getChange().getCommentsList()) {
            StreamDepreciationMetricsRequest itemRequest = StreamDepreciationMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamDepreciationMetricsRequest.Filter.newBuilder()
                        .setCreatedAfter(element.getCreatedAt())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(element.getAuthorEmail())
                        .build())
                .build();
            var responses = assetsDepreciationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamDepreciationMetrics(itemRequest);
            responses.forEachRemaining(results2::add);
        }
        var reply2 = results2.isEmpty() ? StreamDepreciationMetricsResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = JsonCodec.roundTrip(SubmitClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getChange().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), SubmitClaimRequest.class);
        var reply3 = insuranceClaimApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitClaim(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getChange().getSubject()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamDisposalChanges", request.getChange().getSubject());
        return StreamDisposalChangesResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply1.getFixedAsset().getOwner().getDisplayName())
                                .build())
                        .setDisposalId(request.getChange().getDisposalId())
                        .setPriority(request.getChange().getPriority())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(reply3.getClaim().getRequester().getPhone())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a disposal.
     */
    public GetDisposalSyncStateResponse getDisposalSyncState(GetDisposalSyncStateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetDisposalSyncState")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Disposal", request.getDisposalId(), "GetDisposalSyncState");
        return GetDisposalSyncStateResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setDisposalId(request.getDisposalId())
                        .build())
                .build();
    }

    /**
     * Lists disposals a page at a time.
     */
    public ListDisposalConflictsResponse listDisposalConflicts(ListDisposalConflictsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListDisposalConflicts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Disposal", request.getContext().getTraceId(), "ListDisposalConflicts");

        var outbound0 = RpcSupport.unpack(Any.pack(CheckMetricSeriesEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckMetricSeriesEligibilityRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build()), CheckMetricSeriesEligibilityRequest.class);
        var reply0 = metricsMetricSeriesValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkMetricSeriesEligibility(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListDisposalConflicts", request.getPage().getPageToken());
        return ListDisposalConflictsResponse.newBuilder()
                .addDisposals(Disposal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getMetricSeries().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }
}
