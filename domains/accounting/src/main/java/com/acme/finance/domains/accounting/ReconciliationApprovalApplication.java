package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationApprovalServiceGrpc;
import com.acme.accounting.v1.ApproveChartOfAccountsRequest;
import com.acme.accounting.v1.ApproveChartOfAccountsResponse;
import com.acme.accounting.v1.ApproveReconciliationRequest;
import com.acme.accounting.v1.ApproveReconciliationResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsServiceGrpc;
import com.acme.accounting.v1.CommentReconciliationApprovalRequest;
import com.acme.accounting.v1.CommentReconciliationApprovalResponse;
import com.acme.accounting.v1.CompareReconciliationOptionsRequest;
import com.acme.accounting.v1.CompareReconciliationOptionsResponse;
import com.acme.accounting.v1.DelegateAllocationApprovalRequest;
import com.acme.accounting.v1.DelegateAllocationApprovalResponse;
import com.acme.accounting.v1.DelegateReconciliationApprovalRequest;
import com.acme.accounting.v1.DelegateReconciliationApprovalResponse;
import com.acme.accounting.v1.EscalateReconciliationRequest;
import com.acme.accounting.v1.EscalateReconciliationResponse;
import com.acme.accounting.v1.GetReconciliationApprovalStatusRequest;
import com.acme.accounting.v1.GetReconciliationApprovalStatusResponse;
import com.acme.accounting.v1.GetReconciliationRatesRequest;
import com.acme.accounting.v1.GetReconciliationRatesResponse;
import com.acme.accounting.v1.LintReconciliationsRequest;
import com.acme.accounting.v1.LintReconciliationsResponse;
import com.acme.accounting.v1.ListChartOfAccountsesRequest;
import com.acme.accounting.v1.ListChartOfAccountsesResponse;
import com.acme.accounting.v1.RecallReconciliationRequest;
import com.acme.accounting.v1.RecallReconciliationResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.accounting.v1.ReconciliationComment;
import com.acme.accounting.v1.RejectReconciliationRequest;
import com.acme.accounting.v1.RejectReconciliationResponse;
import com.acme.accounting.v1.SubmitReconciliationRequest;
import com.acme.accounting.v1.SubmitReconciliationResponse;
import com.acme.assets.v1.ListFixedAssetWorkflowsRequest;
import com.acme.assets.v1.ListFixedAssetWorkflowsResponse;
import com.acme.audit.v1.AnnotateAuditTrailVersionRequest;
import com.acme.audit.v1.AnnotateAuditTrailVersionResponse;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.ListRetentionPolicySubscriptionsRequest;
import com.acme.audit.v1.ListRetentionPolicySubscriptionsResponse;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesRequest;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesResponse;
import com.acme.returns.v1.CalculateReturnRequestRequest;
import com.acme.returns.v1.CalculateReturnRequestResponse;
import com.acme.returns.v1.ReturnRequest;
import com.acme.returns.v1.ReturnRequestCalculationServiceGrpc;
import com.acme.telehealth.v1.SearchVideoVisitsRequest;
import com.acme.telehealth.v1.SearchVideoVisitsResponse;
import com.acme.telehealth.v1.VideoVisitServiceGrpc;
import com.acme.treasury.v1.AdvanceCashPositionWorkflowRequest;
import com.acme.treasury.v1.AdvanceCashPositionWorkflowResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionWorkflowServiceGrpc;
import com.acme.treasury.v1.FxHedgeRef;
import com.acme.treasury.v1.SignalCashPositionWorkflowRequest;
import com.acme.treasury.v1.SignalCashPositionWorkflowResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Routes reconciliations through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ReconciliationApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(ReconciliationApprovalApplication.class);

    private final AllocationApprovalServiceGrpc.AllocationApprovalServiceBlockingStub accountingAllocationApprovalServiceBlocking;

    private final ChartOfAccountsServiceGrpc.ChartOfAccountsServiceBlockingStub accountingChartOfAccountsServiceBlocking;

    private final ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceBlockingStub accountingReconciliationCalculationServiceBlocking;

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking;

    private final RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking;

    private final VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final AccountingRepository repository;

    private final ReturnRequestCalculationServiceGrpc.ReturnRequestCalculationServiceBlockingStub returnsReturnRequestCalculationServiceBlocking;

    private final Executor rpcExecutor;

    private final VideoVisitServiceGrpc.VideoVisitServiceBlockingStub telehealthVideoVisitServiceBlocking;

    private final CashPositionWorkflowServiceGrpc.CashPositionWorkflowServiceBlockingStub treasuryCashPositionWorkflowServiceBlocking;

    public ReconciliationApprovalApplication(
            AllocationApprovalServiceGrpc.AllocationApprovalServiceBlockingStub accountingAllocationApprovalServiceBlocking,
            ChartOfAccountsServiceGrpc.ChartOfAccountsServiceBlockingStub accountingChartOfAccountsServiceBlocking,
            ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceBlockingStub accountingReconciliationCalculationServiceBlocking,
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking,
            RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking,
            VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            AccountingRepository repository,
            ReturnRequestCalculationServiceGrpc.ReturnRequestCalculationServiceBlockingStub returnsReturnRequestCalculationServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            VideoVisitServiceGrpc.VideoVisitServiceBlockingStub telehealthVideoVisitServiceBlocking,
            CashPositionWorkflowServiceGrpc.CashPositionWorkflowServiceBlockingStub treasuryCashPositionWorkflowServiceBlocking) {
        this.accountingAllocationApprovalServiceBlocking = accountingAllocationApprovalServiceBlocking;
        this.accountingChartOfAccountsServiceBlocking = accountingChartOfAccountsServiceBlocking;
        this.accountingReconciliationCalculationServiceBlocking = accountingReconciliationCalculationServiceBlocking;
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditAuditTrailHistoryServiceBlocking = auditAuditTrailHistoryServiceBlocking;
        this.auditRetentionPolicyNotificationServiceBlocking = auditRetentionPolicyNotificationServiceBlocking;
        this.budgetingVarianceSyncServiceAsync = budgetingVarianceSyncServiceAsync;
        this.channels = channels;
        this.events = events;
        this.repository = repository;
        this.returnsReturnRequestCalculationServiceBlocking = returnsReturnRequestCalculationServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.telehealthVideoVisitServiceBlocking = telehealthVideoVisitServiceBlocking;
        this.treasuryCashPositionWorkflowServiceBlocking = treasuryCashPositionWorkflowServiceBlocking;
    }

    public record EscalateReconciliationStage1Requested(ApproveChartOfAccountsRequest request, CompletableFuture<ApproveChartOfAccountsResponse> completion, io.grpc.Context context) {}

    public record DelegateReconciliationApprovalStage0Requested(ListFixedAssetWorkflowsRequest request, CompletableFuture<ListFixedAssetWorkflowsResponse> completion, io.grpc.Context context) {}

    /**
     * Submits a reconciliation.
     */
    public SubmitReconciliationResponse submitReconciliation(SubmitReconciliationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitReconciliation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReconciliation(request.getContext().getTenantId(), request.getReconciliation());

        var outbound0 = ListRetentionPolicySubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListRetentionPolicySubscriptionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getReconciliation().getAudit().getCreatedAt())
                        .putAllLabels(request.getReconciliation().getCustomFieldsMap())
                        .setOwnerId(request.getReconciliation().getAssignee().getUserId())
                        .build())
                .build();
        var reply0 = RpcSupport.retryRead(() -> auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listRetentionPolicySubscriptions(outbound0));

        var futures1 = request.getReconciliation().getCommentsList().stream()
                .map(element -> CompareReconciliationOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .addComments(ReconciliationComment.newBuilder()
                                .setBody(element.getBody())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> accountingReconciliationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareReconciliationOptions(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? CompareReconciliationOptionsResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getReconciliation().getRequester())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateAuditEventsResponse.getDefaultInstance());
        var input2 = auditAuditEventServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateAuditEvents(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamVarianceChangesRequest.newBuilder()
                .setChange(Variance.newBuilder()
                        .setAssignee(request.getReconciliation().getAssignee())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getReconciliation().getAudit().getCreatedAt())
                                .setCreatedBy(request.getReconciliation().getAudit().getCreatedBy())
                                .setEtag(request.getReconciliation().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamVarianceChangesResponse.getDefaultInstance());
        var input3 = budgetingVarianceSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamVarianceChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getReconciliation().getReconciliationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitReconciliation", request.getReconciliation().getReconciliationId());
        return SubmitReconciliationResponse.newBuilder()
                .setReconciliation(Reconciliation.newBuilder()
                        .setReconciliationId(request.getReconciliationId())
                        .setSubject(request.getReconciliation().getSubject())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Approves a reconciliation.
     */
    public ApproveReconciliationResponse approveReconciliation(ApproveReconciliationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveReconciliation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReconciliation(request.getContext().getTenantId(), request.getReconciliation());

        var futures0 = request.getReconciliation().getCommentsList().stream()
                .map(element -> CalculateReturnRequestRequest.newBuilder()
                .setContext(request.getContext())
                .setReturnRequest(ReturnRequest.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> returnsReturnRequestCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateReturnRequest(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? CalculateReturnRequestResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ListChartOfAccountsesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListChartOfAccountsesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getReconciliation().getAudit().getUpdatedAt())
                        .setRegion(reply0.getReturnRequest().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = RpcSupport.retryRead(() -> accountingChartOfAccountsServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listChartOfAccountses(outbound1));
        log.atDebug().addKeyValue("auditValue", request.getReconciliation().getReconciliationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveReconciliation", request.getReconciliation().getReconciliationId());
        return ApproveReconciliationResponse.newBuilder()
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(request.getReconciliation().getAssignee())
                        .setReconciliationId(request.getReconciliationId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Rejects a reconciliation.
     */
    public RejectReconciliationResponse rejectReconciliation(RejectReconciliationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectReconciliation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReconciliation(request.getContext().getTenantId(), request.getReconciliation());

        var results0 = new ArrayList<SignalCashPositionWorkflowResponse>();
        for (var element : request.getReconciliation().getCommentsList()) {
            SignalCashPositionWorkflowRequest itemRequest = SignalCashPositionWorkflowRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getReconciliation().getRequester().getName())
                                .build())
                        .setHolder(FxHedgeRef.newBuilder()
                                .setDisplayName(request.getReconciliation().getAssignee().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(SignalCashPositionWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .build();
            results0.add(treasuryCashPositionWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalCashPositionWorkflow(itemRequest));
        }
        var reply0 = results0.isEmpty() ? SignalCashPositionWorkflowResponse.getDefaultInstance()
                : results0.getLast();

        var futures1 = request.getReconciliation().getCommentsList().stream()
                .map(element -> DelegateAllocationApprovalRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(DelegateAllocationApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getReconciliation().getRequester().getEmail())
                                .build())
                        .setApproverEmail(element.getAuthorEmail())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> accountingAllocationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateAllocationApproval(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? DelegateAllocationApprovalResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getReconciliation().getReconciliationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectReconciliation", request.getReconciliation().getReconciliationId());
        return RejectReconciliationResponse.newBuilder()
                .setReconciliation(Reconciliation.newBuilder()
                        .setReconciliationId(request.getReconciliationId())
                        .setSubject(request.getReconciliation().getSubject())
                        .build())
                .build();
    }

    /**
     * Escalates a reconciliation.
     */
    public EscalateReconciliationResponse escalateReconciliation(EscalateReconciliationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateReconciliation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReconciliation(request.getContext().getTenantId(), request.getReconciliation());

        var builder0 = AnnotateAuditTrailVersionRequest.newBuilder();
        builder0.setAuditTrail(AuditTrail.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedAt(request.getReconciliation().getAudit().getUpdatedAt())
                        .build())
                .setCounterparty(RetentionPolicyRef.newBuilder()
                        .setDisplayName(request.getReconciliation().getAssignee().getDisplayName())
                        .build())
                .build());
        builder0.setContext(request.getContext());
        builder0.setDetails(AnnotateAuditTrailVersionRequest.Details.newBuilder()
                .setSnapshot(JsonCodec.encode(request.getContext()))
                .build());
        var outbound0 = builder0.build();
        var reply0 = auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotateAuditTrailVersion(outbound0);

        var outbound1 = ApproveChartOfAccountsRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRecipient(reply0.getAuditTrail().getBillingAddress().getRecipient())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getDetails().getApprover().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var completion1 = new CompletableFuture<ApproveChartOfAccountsResponse>();
        events.publishEvent(new EscalateReconciliationStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = GetReconciliationRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setReconciliationId(request.getReconciliation().getReconciliationId())
                .build();
        var reply2 = accountingReconciliationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getReconciliationRates(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getReconciliation().getSubject()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateReconciliation", request.getReconciliation().getSubject());
        return EscalateReconciliationResponse.newBuilder()
                .setReconciliation(Reconciliation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getReconciliation().getAudit().getUpdatedBy())
                                .build())
                        .addComments(ReconciliationComment.newBuilder()
                                .setCreatedAt(reply2.getReconciliation().getAudit().getCreatedAt())
                                .build())
                        .setReconciliationId(request.getReconciliationId())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply1.getChartOfAccounts().getManager().getEmail())
                                .build())
                        .setStatus(request.getReconciliation().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Delegates a reconciliation.
     */
    public DelegateReconciliationApprovalResponse delegateReconciliationApproval(DelegateReconciliationApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateReconciliationApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReconciliation(request.getContext().getTenantId(), request.getReconciliation());

        var outbound0 = ListFixedAssetWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListFixedAssetWorkflowsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getReconciliation().getAudit().getUpdatedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getReconciliation().getCustomer().getChartOfAccountsId())
                        .build())
                .build();
        var completion0 = new CompletableFuture<ListFixedAssetWorkflowsResponse>();
        events.publishEvent(new DelegateReconciliationApprovalStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = LintReconciliationsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintReconciliationsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .setPhone(request.getReconciliation().getRequester().getPhone())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.accounting.v1.ReconciliationValidationService");
        var serviceName1 = "acme.accounting.v1" + "." + "ReconciliationValidationService";
        var methodName1 = "Lin" + "tReconciliations";
        var descriptor1 = MethodDescriptor.<LintReconciliationsRequest, LintReconciliationsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(LintReconciliationsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(LintReconciliationsResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getReconciliation().getReconciliationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateReconciliationApproval", request.getReconciliation().getReconciliationId());
        return DelegateReconciliationApprovalResponse.newBuilder()
                .setOutcome(DelegateReconciliationApprovalResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setReconciliation(Reconciliation.newBuilder()
                        .setReconciliationId(request.getReconciliationId())
                        .addAllTags(reply1.getReconciliation().getTagsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Recalls a reconciliation.
     */
    public RecallReconciliationResponse recallReconciliation(RecallReconciliationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallReconciliation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReconciliation(request.getContext().getTenantId(), request.getReconciliation());

        var outbound0 = AdvanceCashPositionWorkflowRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getReconciliation().getRequester().getPhone())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var command0 = new DispatchCommand<>("acme.treasury.v1.CashPositionWorkflowService/AdvanceCashPositionWorkflow", outbound0);
        var reply0 = treasuryCashPositionWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceCashPositionWorkflow(command0.payload());

        var outbound1 = SubscribeFiscalPeriodUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeFiscalPeriodUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getReconciliation().getAssignee().getEmail())
                                .setPreferredChannel(request.getReconciliation().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.ledger.v1.FiscalPeriodNotificationService");
        var serviceName1 = "acme.ledger.v1" + "." + "FiscalPeriodNotificationService";
        var methodName1 = "Sub" + "scribeFiscalPeriodUpdates";
        var descriptor1 = MethodDescriptor.<SubscribeFiscalPeriodUpdatesRequest, SubscribeFiscalPeriodUpdatesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(SubscribeFiscalPeriodUpdatesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SubscribeFiscalPeriodUpdatesResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getReconciliation().getReconciliationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallReconciliation", request.getReconciliation().getReconciliationId());
        return RecallReconciliationResponse.newBuilder()
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply1.getFiscalPeriod().getOrganizer().getEmail())
                                .build())
                        .setReconciliationId(request.getReconciliationId())
                        .setSubject(request.getReconciliation().getSubject())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a reconciliation.
     */
    public GetReconciliationApprovalStatusResponse getReconciliationApprovalStatus(GetReconciliationApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetReconciliationApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Reconciliation", request.getReconciliationId(), "GetReconciliationApprovalStatus");
        return GetReconciliationApprovalStatusResponse.newBuilder()
                .setReconciliation(Reconciliation.newBuilder()
                        .setReconciliationId(request.getReconciliationId())
                        .build())
                .build();
    }

    /**
     * Comments a reconciliation.
     */
    public CommentReconciliationApprovalResponse commentReconciliationApproval(CommentReconciliationApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentReconciliationApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReconciliation(request.getContext().getTenantId(), request.getReconciliation());

        var results0 = new ArrayList<SearchVideoVisitsResponse>();
        for (var element : request.getReconciliation().getHistoryList()) {
            SearchVideoVisitsRequest itemRequest = SearchVideoVisitsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchVideoVisitsRequest.Filter.newBuilder()
                        .setCreatedAfter(element.getChangedAt())
                        .putAllLabels(request.getReconciliation().getCustomFieldsMap())
                        .setOwnerId(request.getReconciliation().getAssignee().getUserId())
                        .build())
                .build();
            results0.add(telehealthVideoVisitServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchVideoVisits(itemRequest));
        }
        var reply0 = results0.isEmpty() ? SearchVideoVisitsResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getReconciliation().getReconciliationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentReconciliationApproval", request.getReconciliation().getReconciliationId());
        return CommentReconciliationApprovalResponse.newBuilder()
                .setReconciliation(Reconciliation.newBuilder()
                        .setReconciliationId(request.getReconciliationId())
                        .setSubject(request.getReconciliation().getSubject())
                        .build())
                .build();
    }
}
