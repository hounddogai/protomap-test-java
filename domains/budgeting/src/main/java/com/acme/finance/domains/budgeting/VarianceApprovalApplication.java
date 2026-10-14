package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.AdvanceDepreciationWorkflowRequest;
import com.acme.assets.v1.AdvanceDepreciationWorkflowResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.DepreciationWorkflowServiceGrpc;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.ListAuditEventsRequest;
import com.acme.audit.v1.ListAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.audit.v1.ScoreAuditTrailRequest;
import com.acme.audit.v1.ScoreAuditTrailResponse;
import com.acme.audit.v1.SearchAuditEventsRequest;
import com.acme.audit.v1.SearchAuditEventsResponse;
import com.acme.budgeting.v1.ApproveVarianceRequest;
import com.acme.budgeting.v1.ApproveVarianceResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetWorkflowServiceGrpc;
import com.acme.budgeting.v1.CommentVarianceApprovalRequest;
import com.acme.budgeting.v1.CommentVarianceApprovalResponse;
import com.acme.budgeting.v1.DelegateVarianceApprovalRequest;
import com.acme.budgeting.v1.DelegateVarianceApprovalResponse;
import com.acme.budgeting.v1.EscalateVarianceRequest;
import com.acme.budgeting.v1.EscalateVarianceResponse;
import com.acme.budgeting.v1.ExpireVarianceRequest;
import com.acme.budgeting.v1.ExpireVarianceResponse;
import com.acme.budgeting.v1.ExplainBudgetCalculationRequest;
import com.acme.budgeting.v1.ExplainBudgetCalculationResponse;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.ForecastReportingServiceGrpc;
import com.acme.budgeting.v1.GenerateForecastReportRequest;
import com.acme.budgeting.v1.GenerateForecastReportResponse;
import com.acme.budgeting.v1.GetVarianceApprovalStatusRequest;
import com.acme.budgeting.v1.GetVarianceApprovalStatusResponse;
import com.acme.budgeting.v1.ListPendingVariancesRequest;
import com.acme.budgeting.v1.ListPendingVariancesResponse;
import com.acme.budgeting.v1.PreviewVarianceRequest;
import com.acme.budgeting.v1.PreviewVarianceResponse;
import com.acme.budgeting.v1.RecallVarianceRequest;
import com.acme.budgeting.v1.RecallVarianceResponse;
import com.acme.budgeting.v1.RenewVarianceRequest;
import com.acme.budgeting.v1.RenewVarianceResponse;
import com.acme.budgeting.v1.RetryBudgetWorkflowRequest;
import com.acme.budgeting.v1.RetryBudgetWorkflowResponse;
import com.acme.budgeting.v1.ShareForecastReportRequest;
import com.acme.budgeting.v1.ShareForecastReportResponse;
import com.acme.budgeting.v1.SubmitVarianceRequest;
import com.acme.budgeting.v1.SubmitVarianceResponse;
import com.acme.budgeting.v1.SyncVariancesRequest;
import com.acme.budgeting.v1.SyncVariancesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceChannelOrigin;
import com.acme.budgeting.v1.VarianceComment;
import com.acme.budgeting.v1.VarianceLifecycleServiceGrpc;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.budgeting.v1.VarianceValidationServiceGrpc;
import com.acme.careplans.v1.CarePlanQueryServiceGrpc;
import com.acme.careplans.v1.ResolveCarePlansRequest;
import com.acme.careplans.v1.ResolveCarePlansResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.corporateevents.v1.CorporateEvent;
import com.acme.corporateevents.v1.CorporateEventCalculationServiceGrpc;
import com.acme.corporateevents.v1.CorporateEventParticipant;
import com.acme.corporateevents.v1.ExplainCorporateEventCalculationRequest;
import com.acme.corporateevents.v1.ExplainCorporateEventCalculationResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.giftcards.v1.GiftCardQueryServiceGrpc;
import com.acme.giftcards.v1.ResolveGiftCardsRequest;
import com.acme.giftcards.v1.ResolveGiftCardsResponse;
import com.acme.identity.v1.UserRef;
import com.acme.metrics.v1.CloneMetricSeriesRequest;
import com.acme.metrics.v1.CloneMetricSeriesResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.suppliers.v1.CreateSupplierRequest;
import com.acme.suppliers.v1.CreateSupplierResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.WatchSuppliersRequest;
import com.acme.suppliers.v1.WatchSuppliersResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.Optional;
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
 * Routes variances through approval, with escalation and delegation. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class VarianceApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(VarianceApprovalApplication.class);

    private final DepreciationWorkflowServiceGrpc.DepreciationWorkflowServiceBlockingStub assetsDepreciationWorkflowServiceBlocking;

    private final AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking;

    private final AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking;

    private final BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking;

    private final BudgetWorkflowServiceGrpc.BudgetWorkflowServiceBlockingStub budgetingBudgetWorkflowServiceBlocking;

    private final ForecastReportingServiceGrpc.ForecastReportingServiceBlockingStub budgetingForecastReportingServiceBlocking;

    private final VarianceLifecycleServiceGrpc.VarianceLifecycleServiceBlockingStub budgetingVarianceLifecycleServiceBlocking;

    private final VarianceSyncServiceGrpc.VarianceSyncServiceBlockingStub budgetingVarianceSyncServiceBlocking;

    private final VarianceValidationServiceGrpc.VarianceValidationServiceBlockingStub budgetingVarianceValidationServiceBlocking;

    private final CarePlanQueryServiceGrpc.CarePlanQueryServiceBlockingStub careplansCarePlanQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final CorporateEventCalculationServiceGrpc.CorporateEventCalculationServiceBlockingStub corporateeventsCorporateEventCalculationServiceBlocking;

    private final GiftCardQueryServiceGrpc.GiftCardQueryServiceBlockingStub giftcardsGiftCardQueryServiceBlocking;

    private final VarianceApprovalRequestMapper mapper;

    private final BudgetingRepository repository;

    private final Executor rpcExecutor;

    private final SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking;

    public VarianceApprovalApplication(
            DepreciationWorkflowServiceGrpc.DepreciationWorkflowServiceBlockingStub assetsDepreciationWorkflowServiceBlocking,
            AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking,
            AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking,
            BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking,
            BudgetWorkflowServiceGrpc.BudgetWorkflowServiceBlockingStub budgetingBudgetWorkflowServiceBlocking,
            ForecastReportingServiceGrpc.ForecastReportingServiceBlockingStub budgetingForecastReportingServiceBlocking,
            VarianceLifecycleServiceGrpc.VarianceLifecycleServiceBlockingStub budgetingVarianceLifecycleServiceBlocking,
            VarianceSyncServiceGrpc.VarianceSyncServiceBlockingStub budgetingVarianceSyncServiceBlocking,
            VarianceValidationServiceGrpc.VarianceValidationServiceBlockingStub budgetingVarianceValidationServiceBlocking,
            CarePlanQueryServiceGrpc.CarePlanQueryServiceBlockingStub careplansCarePlanQueryServiceBlocking,
            ChannelRegistry channels,
            CorporateEventCalculationServiceGrpc.CorporateEventCalculationServiceBlockingStub corporateeventsCorporateEventCalculationServiceBlocking,
            GiftCardQueryServiceGrpc.GiftCardQueryServiceBlockingStub giftcardsGiftCardQueryServiceBlocking,
            VarianceApprovalRequestMapper mapper,
            BudgetingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking) {
        this.assetsDepreciationWorkflowServiceBlocking = assetsDepreciationWorkflowServiceBlocking;
        this.auditAuditEventServiceBlocking = auditAuditEventServiceBlocking;
        this.auditAuditTrailValidationServiceBlocking = auditAuditTrailValidationServiceBlocking;
        this.budgetingBudgetCalculationServiceBlocking = budgetingBudgetCalculationServiceBlocking;
        this.budgetingBudgetWorkflowServiceBlocking = budgetingBudgetWorkflowServiceBlocking;
        this.budgetingForecastReportingServiceBlocking = budgetingForecastReportingServiceBlocking;
        this.budgetingVarianceLifecycleServiceBlocking = budgetingVarianceLifecycleServiceBlocking;
        this.budgetingVarianceSyncServiceBlocking = budgetingVarianceSyncServiceBlocking;
        this.budgetingVarianceValidationServiceBlocking = budgetingVarianceValidationServiceBlocking;
        this.careplansCarePlanQueryServiceBlocking = careplansCarePlanQueryServiceBlocking;
        this.channels = channels;
        this.corporateeventsCorporateEventCalculationServiceBlocking = corporateeventsCorporateEventCalculationServiceBlocking;
        this.giftcardsGiftCardQueryServiceBlocking = giftcardsGiftCardQueryServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierServiceBlocking = suppliersSupplierServiceBlocking;
    }

    /**
     * Submits a variance.
     */
    public SubmitVarianceResponse submitVariance(SubmitVarianceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitVariance")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var outbound0 = ShareForecastReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareForecastReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .setPhone(request.getVariance().getRequester().getPhone())
                                .setPreferredChannel(request.getVariance().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = budgetingForecastReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareForecastReport(outbound0);

        var outbound1 = (ExplainCorporateEventCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setCorporateEvent(CorporateEvent.newBuilder()
                        .addParticipants(CorporateEventParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getVariance().getAssignee().getEmail())
                                        .setName(request.getVariance().getRequester().getName())
                                        .setPhone(request.getVariance().getRequester().getPhone())
                                        .build())
                                .build())
                        .build())
                .setDetails(ExplainCorporateEventCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getVariance().getRequester().getEmail())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = corporateeventsCorporateEventCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCorporateEventCalculation(outbound1);

        var outbound2 = CreateSupplierRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplier(Supplier.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getForecast().getAudit().getEtag())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply1.getCorporateEvent().getLocation().getPostalCode())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(request.getVariance().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<CreateSupplierRequest, CreateSupplierResponse> port2 = RpcPort.adapt(suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::createSupplier);
        var reply2 = port2.exchange(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitVariance", request.getVariance().getVarianceId());
        return SubmitVarianceResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getSupplier().getAudit().getCreatedAt())
                                .build())
                        .addComments(VarianceComment.newBuilder()
                                .setCreatedAt(reply0.getForecast().getAudit().getCreatedAt())
                                .build())
                        .setRequester(request.getVariance().getRequester())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Approves a variance.
     */
    public ApproveVarianceResponse approveVariance(ApproveVarianceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveVariance")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        List<SearchAuditEventsResponse> results0 = request.getVariance().getHistoryList().stream()
                .map(element -> SearchAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(element.getChangedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getVariance().getCustomer().getBudgetLineId())
                        .build())
                .build())
                .flatMap(value -> Stream.of(auditAuditEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchAuditEvents(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? SearchAuditEventsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ExpireVarianceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExpireVarianceRequest.Details.newBuilder()
                        .setNewOwner(UserRef.newBuilder()
                                .setEmail(request.getVariance().getAssignee().getEmail())
                                .setUserId(request.getVariance().getAssignee().getUserId())
                                .build())
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setEmail(request.getVariance().getRequester().getEmail())
                                .setPreferredChannel(request.getVariance().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .setVarianceId(request.getVarianceId())
                .build();
        var command1 = new DispatchCommand<>("acme.budgeting.v1.VarianceLifecycleService/ExpireVariance", outbound1);
        var reply1 = budgetingVarianceLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).expireVariance(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveVariance", request.getVariance().getVarianceId());
        return ApproveVarianceResponse.newBuilder()
                .setVariance(Variance.newBuilder()
                        .setStatus(request.getVariance().getStatus())
                        .addAllTags(reply1.getVariance().getTagsList())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Escalates a variance.
     */
    public EscalateVarianceResponse escalateVariance(EscalateVarianceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateVariance")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var outbound0 = CloneMetricSeriesRequest.newBuilder()
                .setContext(request.getContext())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getVariance().getAssignee().getDisplayName())
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.metrics.v1.MetricSeriesService");
        var serviceName0 = "acme.metrics.v1" + "." + "MetricSeriesService";
        var methodName0 = "Clo" + "neMetricSeries";
        var descriptor0 = MethodDescriptor.<CloneMetricSeriesRequest, CloneMetricSeriesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(CloneMetricSeriesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CloneMetricSeriesResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var futures1 = request.getVariance().getCommentsList().stream()
                .map(element -> PreviewVarianceRequest.newBuilder()
                .setContext(request.getContext())
                .setVariance(Variance.newBuilder()
                        .addComments(VarianceComment.newBuilder()
                                .setCommentId(element.getCommentId())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> budgetingVarianceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewVariance(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? PreviewVarianceResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateVariance", request.getVariance().getVarianceId());
        return EscalateVarianceResponse.newBuilder()
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply0.getMetricSeries().getOwner().getUserId())
                                .build())
                        .setStatus(request.getVariance().getStatus())
                        .addAllTags(reply0.getMetricSeries().getTagsList())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Lists variances a page at a time.
     */
    public ListPendingVariancesResponse listPendingVariances(ListPendingVariancesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingVariances")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getContext().getTraceId(), "ListPendingVariances");

        var outbound0 = ResolveGiftCardsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply0 = giftcardsGiftCardQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveGiftCards(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingVariances", request.getPage().getPageToken());
        return ListPendingVariancesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Delegates a variance.
     */
    public DelegateVarianceApprovalResponse delegateVarianceApproval(DelegateVarianceApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateVarianceApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var outbound0 = RetryBudgetWorkflowRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setHolder(ForecastRef.newBuilder()
                                .setDisplayName(switch (request.getVariance().getOriginCase()) {
                                    case CUSTOMER -> request.getVariance().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .setNumber(switch (request.getVariance().getOriginCase()) {
                                    case CUSTOMER -> request.getVariance().getCustomer().getNumber();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RetryBudgetWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPhone(request.getVariance().getRequester().getPhone())
                                .build())
                        .build())
                .build();
        var reply0 = budgetingBudgetWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryBudgetWorkflow(outbound0);

        var outbound1 = AdvanceDepreciationWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(reply0.getBudget().getHolder().getDisplayName())
                                .build())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPreferredChannel(request.getVariance().getRequester().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> assetsDepreciationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceDepreciationWorkflow(value)).orElseThrow();

        var outbound2 = GenerateForecastReportRequest.newBuilder()
                .setContext(request.getContext())
                .setForecastId(reply0.getBudget().getHolder().getForecastId())
                .setReason(request.getReason())
                .build();
        RpcPort<GenerateForecastReportRequest, GenerateForecastReportResponse> port2 = RpcPort.adapt(budgetingForecastReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::generateForecastReport);
        var reply2 = port2.exchange(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateVarianceApproval", request.getVariance().getVarianceId());
        return DelegateVarianceApprovalResponse.newBuilder()
                .setOutcome(DelegateVarianceApprovalResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply1.getDepreciation().getOrganizer().getUserId())
                                .build())
                        .addComments(VarianceComment.newBuilder()
                                .setCreatedAt(reply0.getBudget().getAudit().getCreatedAt())
                                .build())
                        .setStatus(request.getVariance().getStatus())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Recalls a variance.
     */
    public RecallVarianceResponse recallVariance(RecallVarianceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallVariance")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var outbound0 = mapper.recallVarianceStage0(request);
        var reply0 = budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainBudgetCalculation(outbound0);

        var outbound1 = WatchSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getApprover().getEmail())
                        .setName(request.getVariance().getRequester().getName())
                        .setPhone(reply0.getBudget().getContact().getPhone())
                        .build())
                .build();
        var stream1 = suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchSuppliers(outbound1);
        var reply1 = WatchSuppliersResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = ScoreAuditTrailRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getVariance().getAudit().getUpdatedAt())
                                .build())
                        .setCard(AuditTrailCardSettlement.newBuilder()
                                .setToken(switch (reply0.getBudget().getFundingCase()) {
                                    case CARD -> reply0.getBudget().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(RetentionPolicyRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = auditAuditTrailValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreAuditTrail(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallVariance", request.getVariance().getVarianceId());
        return RecallVarianceResponse.newBuilder()
                .setVariance(Variance.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getAuditTrail().getAudit().getCreatedAt())
                                .setUpdatedAt(reply2.getAuditTrail().getAudit().getUpdatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getBudget().getContact().getEmail())
                                .setPhone(reply0.getBudget().getContact().getPhone())
                                .build())
                        .setStatus(request.getVariance().getStatus())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Returns a variance.
     */
    public GetVarianceApprovalStatusResponse getVarianceApprovalStatus(GetVarianceApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetVarianceApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getVarianceId(), "GetVarianceApprovalStatus");
        return GetVarianceApprovalStatusResponse.newBuilder()
                .setVariance(Variance.newBuilder()
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Comments a variance.
     */
    public CommentVarianceApprovalResponse commentVarianceApproval(CommentVarianceApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentVarianceApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var outbound0 = ResolveCarePlansRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> careplansCarePlanQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveCarePlans(outbound0), rpcExecutor);

        var outbound1 = RenewVarianceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RenewVarianceRequest.Details.newBuilder()
                        .setNewOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setEmail(request.getVariance().getRequester().getEmail())
                                .setPreferredChannel(request.getVariance().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .setVarianceId(request.getVarianceId())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> budgetingVarianceLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).renewVariance(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = ListAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getVariance().getSla().getDueAt())
                        .putAllLabels(reply1.getResult().getDetailsMap())
                        .setOwnerId(reply1.getVariance().getAssignee().getUserId())
                        .build())
                .build();
        var reply2 = Optional.of(outbound2).map(value -> value.toBuilder().build())
                .map(value -> auditAuditEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditEvents(value)).orElseThrow();

        var outbound3 = SyncVariancesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SyncVariancesRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setVariance(Variance.newBuilder()
                        .setChannel(VarianceChannelOrigin.newBuilder()
                                .setAddress(switch (reply1.getVariance().getOriginCase()) {
                                    case CHANNEL -> reply1.getVariance().getChannel().getAddress();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply3 = budgetingVarianceSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).syncVariances(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentVarianceApproval", request.getVariance().getVarianceId());
        return CommentVarianceApprovalResponse.newBuilder()
                .setOutcome(CommentVarianceApprovalResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply3.getOutcome().getEffectiveAt())
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply3.getVariance().getAssignee().getEmail())
                                .build())
                        .setRequester(request.getVariance().getRequester())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }
}
