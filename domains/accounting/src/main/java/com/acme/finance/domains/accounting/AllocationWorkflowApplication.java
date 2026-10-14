package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualApprovalServiceGrpc;
import com.acme.accounting.v1.AccrualLine;
import com.acme.accounting.v1.AccrualLineTax;
import com.acme.accounting.v1.AccrualValidationServiceGrpc;
import com.acme.accounting.v1.AdvanceAllocationWorkflowRequest;
import com.acme.accounting.v1.AdvanceAllocationWorkflowResponse;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationApprovalServiceGrpc;
import com.acme.accounting.v1.AllocationCalculationServiceGrpc;
import com.acme.accounting.v1.CalculateAllocationRequest;
import com.acme.accounting.v1.CalculateAllocationResponse;
import com.acme.accounting.v1.CancelAllocationWorkflowRequest;
import com.acme.accounting.v1.CancelAllocationWorkflowResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsApprovalServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsValidationServiceGrpc;
import com.acme.accounting.v1.CompareAllocationOptionsRequest;
import com.acme.accounting.v1.CompareAllocationOptionsResponse;
import com.acme.accounting.v1.DelegateChartOfAccountsApprovalRequest;
import com.acme.accounting.v1.DelegateChartOfAccountsApprovalResponse;
import com.acme.accounting.v1.GetAllocationApprovalStatusRequest;
import com.acme.accounting.v1.GetAllocationApprovalStatusResponse;
import com.acme.accounting.v1.InspectAllocationWorkflowRequest;
import com.acme.accounting.v1.InspectAllocationWorkflowResponse;
import com.acme.accounting.v1.ListAllocationWorkflowsRequest;
import com.acme.accounting.v1.ListAllocationWorkflowsResponse;
import com.acme.accounting.v1.ListChartOfAccountsesRequest;
import com.acme.accounting.v1.ListChartOfAccountsesResponse;
import com.acme.accounting.v1.ListPendingAllocationsRequest;
import com.acme.accounting.v1.ListPendingAllocationsResponse;
import com.acme.accounting.v1.PauseAllocationWorkflowRequest;
import com.acme.accounting.v1.PauseAllocationWorkflowResponse;
import com.acme.accounting.v1.PreviewAccrualRequest;
import com.acme.accounting.v1.PreviewAccrualResponse;
import com.acme.accounting.v1.RecallAccrualRequest;
import com.acme.accounting.v1.RecallAccrualResponse;
import com.acme.accounting.v1.RecallChartOfAccountsRequest;
import com.acme.accounting.v1.RecallChartOfAccountsResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationApprovalServiceGrpc;
import com.acme.accounting.v1.ReconciliationLifecycleServiceGrpc;
import com.acme.accounting.v1.RejectReconciliationRequest;
import com.acme.accounting.v1.RejectReconciliationResponse;
import com.acme.accounting.v1.ResumeAllocationWorkflowRequest;
import com.acme.accounting.v1.ResumeAllocationWorkflowResponse;
import com.acme.accounting.v1.RetryAllocationWorkflowRequest;
import com.acme.accounting.v1.RetryAllocationWorkflowResponse;
import com.acme.accounting.v1.ScheduleReconciliationExpiryRequest;
import com.acme.accounting.v1.ScheduleReconciliationExpiryResponse;
import com.acme.accounting.v1.ScoreChartOfAccountsRequest;
import com.acme.accounting.v1.ScoreChartOfAccountsResponse;
import com.acme.accounting.v1.SignalAllocationWorkflowRequest;
import com.acme.accounting.v1.SignalAllocationWorkflowResponse;
import com.acme.accounting.v1.StartAllocationWorkflowRequest;
import com.acme.accounting.v1.StartAllocationWorkflowResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.ResumeFixedAssetWorkflowRequest;
import com.acme.assets.v1.ResumeFixedAssetWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.documents.v1.SummarizeDocumentsRequest;
import com.acme.documents.v1.SummarizeDocumentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.FindUsersRequest;
import com.acme.identity.v1.FindUsersResponse;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.SubscribeLedgerAccountUpdatesRequest;
import com.acme.ledger.v1.SubscribeLedgerAccountUpdatesResponse;
import com.acme.metrics.v1.CloneMetricSeriesRequest;
import com.acme.metrics.v1.CloneMetricSeriesResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderApprovalServiceGrpc;
import com.acme.procurement.v1.RecallPurchaseOrderRequest;
import com.acme.procurement.v1.RecallPurchaseOrderResponse;
import com.google.protobuf.Any;
import java.util.Optional;
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
 * Runs the long-lived workflow that takes an allocation from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class AllocationWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(AllocationWorkflowApplication.class);

    private final AccrualApprovalServiceGrpc.AccrualApprovalServiceBlockingStub accountingAccrualApprovalServiceBlocking;

    private final AccrualValidationServiceGrpc.AccrualValidationServiceBlockingStub accountingAccrualValidationServiceBlocking;

    private final AllocationApprovalServiceGrpc.AllocationApprovalServiceBlockingStub accountingAllocationApprovalServiceBlocking;

    private final AllocationCalculationServiceGrpc.AllocationCalculationServiceBlockingStub accountingAllocationCalculationServiceBlocking;

    private final ChartOfAccountsApprovalServiceGrpc.ChartOfAccountsApprovalServiceBlockingStub accountingChartOfAccountsApprovalServiceBlocking;

    private final ChartOfAccountsServiceGrpc.ChartOfAccountsServiceBlockingStub accountingChartOfAccountsServiceBlocking;

    private final ChartOfAccountsValidationServiceGrpc.ChartOfAccountsValidationServiceBlockingStub accountingChartOfAccountsValidationServiceBlocking;

    private final ReconciliationApprovalServiceGrpc.ReconciliationApprovalServiceBlockingStub accountingReconciliationApprovalServiceBlocking;

    private final ReconciliationLifecycleServiceGrpc.ReconciliationLifecycleServiceBlockingStub accountingReconciliationLifecycleServiceBlocking;

    private final ApplicationEventPublisher events;

    private final UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking;

    private final LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking;

    private final AllocationWorkflowRequestMapper mapper;

    private final MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking;

    private final PurchaseOrderApprovalServiceGrpc.PurchaseOrderApprovalServiceBlockingStub procurementPurchaseOrderApprovalServiceBlocking;

    private final AccountingRepository repository;

    public AllocationWorkflowApplication(
            AccrualApprovalServiceGrpc.AccrualApprovalServiceBlockingStub accountingAccrualApprovalServiceBlocking,
            AccrualValidationServiceGrpc.AccrualValidationServiceBlockingStub accountingAccrualValidationServiceBlocking,
            AllocationApprovalServiceGrpc.AllocationApprovalServiceBlockingStub accountingAllocationApprovalServiceBlocking,
            AllocationCalculationServiceGrpc.AllocationCalculationServiceBlockingStub accountingAllocationCalculationServiceBlocking,
            ChartOfAccountsApprovalServiceGrpc.ChartOfAccountsApprovalServiceBlockingStub accountingChartOfAccountsApprovalServiceBlocking,
            ChartOfAccountsServiceGrpc.ChartOfAccountsServiceBlockingStub accountingChartOfAccountsServiceBlocking,
            ChartOfAccountsValidationServiceGrpc.ChartOfAccountsValidationServiceBlockingStub accountingChartOfAccountsValidationServiceBlocking,
            ReconciliationApprovalServiceGrpc.ReconciliationApprovalServiceBlockingStub accountingReconciliationApprovalServiceBlocking,
            ReconciliationLifecycleServiceGrpc.ReconciliationLifecycleServiceBlockingStub accountingReconciliationLifecycleServiceBlocking,
            ApplicationEventPublisher events,
            UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking,
            LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking,
            AllocationWorkflowRequestMapper mapper,
            MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking,
            PurchaseOrderApprovalServiceGrpc.PurchaseOrderApprovalServiceBlockingStub procurementPurchaseOrderApprovalServiceBlocking,
            AccountingRepository repository) {
        this.accountingAccrualApprovalServiceBlocking = accountingAccrualApprovalServiceBlocking;
        this.accountingAccrualValidationServiceBlocking = accountingAccrualValidationServiceBlocking;
        this.accountingAllocationApprovalServiceBlocking = accountingAllocationApprovalServiceBlocking;
        this.accountingAllocationCalculationServiceBlocking = accountingAllocationCalculationServiceBlocking;
        this.accountingChartOfAccountsApprovalServiceBlocking = accountingChartOfAccountsApprovalServiceBlocking;
        this.accountingChartOfAccountsServiceBlocking = accountingChartOfAccountsServiceBlocking;
        this.accountingChartOfAccountsValidationServiceBlocking = accountingChartOfAccountsValidationServiceBlocking;
        this.accountingReconciliationApprovalServiceBlocking = accountingReconciliationApprovalServiceBlocking;
        this.accountingReconciliationLifecycleServiceBlocking = accountingReconciliationLifecycleServiceBlocking;
        this.events = events;
        this.identityUserQueryServiceBlocking = identityUserQueryServiceBlocking;
        this.ledgerLedgerAccountNotificationServiceBlocking = ledgerLedgerAccountNotificationServiceBlocking;
        this.mapper = mapper;
        this.metricsMetricSeriesServiceBlocking = metricsMetricSeriesServiceBlocking;
        this.procurementPurchaseOrderApprovalServiceBlocking = procurementPurchaseOrderApprovalServiceBlocking;
        this.repository = repository;
    }

    public record StartAllocationWorkflowStage1Requested(ResumeFixedAssetWorkflowRequest request, CompletableFuture<ResumeFixedAssetWorkflowResponse> completion, io.grpc.Context context) {}

    public record ResumeAllocationWorkflowStage1Requested(SummarizeDocumentsRequest request, CompletableFuture<SummarizeDocumentsResponse> completion, io.grpc.Context context) {}

    /**
     * Starts an allocation.
     */
    public StartAllocationWorkflowResponse startAllocationWorkflow(StartAllocationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartAllocationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        var outbound0 = DelegateChartOfAccountsApprovalRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedAt(request.getAllocation().getAudit().getUpdatedAt())
                                                .build())
                                        .addChildren(ChartOfAccounts.newBuilder()
                                                .setName(request.getAllocation().getName())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setEmail(request.getDetails().getNotify().getEmail())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var adapter0 = new Function<DelegateChartOfAccountsApprovalRequest, DelegateChartOfAccountsApprovalResponse>() {
            @Override
            public DelegateChartOfAccountsApprovalResponse apply(DelegateChartOfAccountsApprovalRequest input) {
                return accountingChartOfAccountsApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateChartOfAccountsApproval(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = ResumeFixedAssetWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getChartOfAccounts().getLocation().getLinesList())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getAllocation().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<ResumeFixedAssetWorkflowResponse>();
        events.publishEvent(new StartAllocationWorkflowStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getAllocation().getAllocationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartAllocationWorkflow", request.getAllocation().getAllocationId());
        return StartAllocationWorkflowResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .setName(request.getAllocation().getName())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply0.getChartOfAccounts().getManager().getUserId())
                                .build())
                        .build())
                .setOutcome(StartAllocationWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Advances an allocation.
     */
    public AdvanceAllocationWorkflowResponse advanceAllocationWorkflow(AdvanceAllocationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceAllocationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        var outbound0 = ScoreChartOfAccountsRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setEtag(request.getAllocation().getAudit().getEtag())
                                                .setUpdatedAt(request.getAllocation().getAudit().getUpdatedAt())
                                                .setUpdatedBy(request.getAllocation().getAudit().getUpdatedBy())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setEmail(request.getAllocation().getOwner().getEmail())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage0Adapter {
            ScoreChartOfAccountsResponse fetch(ScoreChartOfAccountsRequest input) {
                return accountingChartOfAccountsValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreChartOfAccounts(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getAllocation().getAllocationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceAllocationWorkflow", request.getAllocation().getAllocationId());
        return AdvanceAllocationWorkflowResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getChartOfAccounts().getAudit().getCreatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply0.getChartOfAccounts().getManager().getUserId())
                                .build())
                        .putAllParameters(request.getDetails().getParametersMap())
                        .build())
                .build();
    }

    /**
     * Cancels an allocation.
     */
    public CancelAllocationWorkflowResponse cancelAllocationWorkflow(CancelAllocationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelAllocationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        var outbound0 = RecallAccrualRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecallAccrualRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getAllocation().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        RpcPort<RecallAccrualRequest, RecallAccrualResponse> port0 = RpcPort.adapt(accountingAccrualApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::recallAccrual);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = RecallPurchaseOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getAccrual().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.procurement.v1.PurchaseOrderApprovalService/RecallPurchaseOrder", outbound1);
        var reply1 = procurementPurchaseOrderApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallPurchaseOrder(command1.payload());

        var outbound2 = SubscribeLedgerAccountUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeLedgerAccountUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getAllocation().getOwner().getEmail())
                                .setName(request.getDetails().getNotify().getName())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(ledgerLedgerAccountNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeLedgerAccountUpdates(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getAllocation().getAllocationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelAllocationWorkflow", request.getAllocation().getAllocationId());
        return CancelAllocationWorkflowResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getPurchaseOrder().getCounterparty().getDisplayName())
                                .build())
                        .setPriority(request.getAllocation().getPriority())
                        .build())
                .setOutcome(CancelAllocationWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Retrys an allocation.
     */
    public RetryAllocationWorkflowResponse retryAllocationWorkflow(RetryAllocationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryAllocationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        var outbound0 = mapper.retryAllocationWorkflowStage0(request);
        var reply0 = accountingAllocationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getAllocationApprovalStatus(outbound0);

        var outbound1 = PreviewAccrualRequest.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getAllocation().getAudit().getCreatedAt())
                                .build())
                        .addLines(AccrualLine.newBuilder()
                                .setAmount(reply0.getAllocation().getDiscount().getAmount())
                                .setTax(AccrualLineTax.newBuilder()
                                        .setAmount(request.getAllocation().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewAccrualRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getAllocation().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        Function<PreviewAccrualRequest, PreviewAccrualResponse> operation1 = accountingAccrualValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewAccrual;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAllocation().getName()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryAllocationWorkflow", request.getAllocation().getName());
        return RetryAllocationWorkflowResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .putAllParameters(request.getDetails().getParametersMap())
                        .setPriority(reply0.getAllocation().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Pauses an allocation.
     */
    public PauseAllocationWorkflowResponse pauseAllocationWorkflow(PauseAllocationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseAllocationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        var outbound0 = (RejectReconciliationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RejectReconciliationRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getAllocation().getOwner().getUserId())
                                .build())
                        .build())
                .setReconciliation(Reconciliation.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = accountingReconciliationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectReconciliation(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getAllocation().getAllocationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseAllocationWorkflow", request.getAllocation().getAllocationId());
        return PauseAllocationWorkflowResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .setVersion(request.getAllocation().getVersion())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Resumes an allocation.
     */
    public ResumeAllocationWorkflowResponse resumeAllocationWorkflow(ResumeAllocationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeAllocationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        var builder0 = ScheduleReconciliationExpiryRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(ScheduleReconciliationExpiryRequest.Details.newBuilder()
                .setNotify(ContactInfo.newBuilder()
                        .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                        .build())
                .build());
        builder0.setReconciliation(Reconciliation.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedAt(request.getAllocation().getAudit().getCreatedAt())
                        .setUpdatedAt(request.getAllocation().getAudit().getUpdatedAt())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = accountingReconciliationLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleReconciliationExpiry(outbound0);

        var outbound1 = SummarizeDocumentsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getAllocation().getBlock().getReason())
                .build();
        var completion1 = new CompletableFuture<SummarizeDocumentsResponse>();
        events.publishEvent(new ResumeAllocationWorkflowStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = BeanCopies.copy(ListChartOfAccountsesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListChartOfAccountsesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getAllocation().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(request.getAllocation().getOwner().getUserId())
                        .build())
                .build(), ListChartOfAccountsesRequest.class);
        var reply2 = accountingChartOfAccountsServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listChartOfAccountses(outbound2);

        var outbound3 = ListPendingAllocationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingAllocationsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getReconciliation().getAudit().getCreatedAt())
                        .putAllLabels(request.getDetails().getParametersMap())
                        .setOwnerId(switch (reply0.getReconciliation().getOriginCase()) {
                            case CUSTOMER -> reply0.getReconciliation().getCustomer().getChartOfAccountsId();
                            default -> "";
                        })
                        .build())
                .build();
        var reply3 = accountingAllocationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingAllocations(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getAllocation().getAllocationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeAllocationWorkflow", request.getAllocation().getAllocationId());
        return ResumeAllocationWorkflowResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getReconciliation().getAssignee().getDisplayName())
                                .build())
                        .setVersion(request.getAllocation().getVersion())
                        .build())
                .setOutcome(ResumeAllocationWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns an allocation.
     */
    public InspectAllocationWorkflowResponse inspectAllocationWorkflow(InspectAllocationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectAllocationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Allocation", request.getAllocationId(), "InspectAllocationWorkflow");

        var outbound0 = CalculateAllocationRequest.newBuilder()
                .setAllocationId(request.getAllocationId())
                .setContext(request.getContext())
                .build();
        final class Stage0Adapter {
            CalculateAllocationResponse fetch(CalculateAllocationRequest input) {
                return accountingAllocationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateAllocation(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        return InspectAllocationWorkflowResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getAllocation().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists allocations a page at a time.
     */
    public ListAllocationWorkflowsResponse listAllocationWorkflows(ListAllocationWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListAllocationWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Allocation", request.getContext().getTraceId(), "ListAllocationWorkflows");

        var outbound0 = CloneMetricSeriesRequest.newBuilder()
                .setContext(request.getContext())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> metricsMetricSeriesServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneMetricSeries(value)).orElseThrow();

        var outbound1 = (FindUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindUsersRequest.Filter.newBuilder()
                        .setOwnerId(request.getFilter().getOwnerId())
                        .build())
                .build()).toBuilder().build();
        var reply1 = identityUserQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findUsers(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListAllocationWorkflows", request.getPage().getPageToken());
        return ListAllocationWorkflowsResponse.newBuilder()
                .setPageInfo(PageInfo.newBuilder()
                        .setNextPageToken(reply1.getPageInfo().getNextPageToken())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Signals an allocation.
     */
    public SignalAllocationWorkflowResponse signalAllocationWorkflow(SignalAllocationWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalAllocationWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        var outbound0 = JsonCodec.roundTrip(RecallChartOfAccountsRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedBy(request.getAllocation().getAudit().getUpdatedBy())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), RecallChartOfAccountsRequest.class);
        var reply0 = accountingChartOfAccountsApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallChartOfAccounts(outbound0);

        var outbound1 = JsonCodec.roundTrip(CompareAllocationOptionsRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getAllocation().getOwner().getEmail())
                                .setUserId(request.getAllocation().getOwner().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareAllocationOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getChartOfAccounts().getLocation().getPostalCode())
                                .setRecipient(reply0.getChartOfAccounts().getLocation().getRecipient())
                                .build())
                        .build())
                .build(), CompareAllocationOptionsRequest.class);
        var reply1 = accountingAllocationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareAllocationOptions(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAllocation().getAllocationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalAllocationWorkflow", request.getAllocation().getAllocationId());
        return SignalAllocationWorkflowResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getAllocation().getAudit().getEtag())
                                .build())
                        .setDescription(request.getAllocation().getDescription())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }
}
