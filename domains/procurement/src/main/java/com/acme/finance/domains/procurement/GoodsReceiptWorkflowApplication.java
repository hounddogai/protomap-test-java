package com.acme.finance.domains.procurement;

import com.acme.accounting.v1.BatchCreateChartOfAccountsesRequest;
import com.acme.accounting.v1.BatchCreateChartOfAccountsesResponse;
import com.acme.accounting.v1.ChartOfAccountsServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.FixedAssetLifecycleServiceGrpc;
import com.acme.assets.v1.ScheduleFixedAssetExpiryRequest;
import com.acme.assets.v1.ScheduleFixedAssetExpiryResponse;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailAttachment;
import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.PreviewAuditTrailRequest;
import com.acme.audit.v1.PreviewAuditTrailResponse;
import com.acme.audit.v1.StreamAuditTrailNoticesRequest;
import com.acme.audit.v1.StreamAuditTrailNoticesResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetApprovalServiceGrpc;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.EscalateBudgetRequest;
import com.acme.budgeting.v1.EscalateBudgetResponse;
import com.acme.collections.v1.PauseWriteOffWorkflowRequest;
import com.acme.collections.v1.PauseWriteOffWorkflowResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.EstimateConsentRecordRequest;
import com.acme.customers.v1.EstimateConsentRecordResponse;
import com.acme.customers.v1.FindHouseholdsRequest;
import com.acme.customers.v1.FindHouseholdsResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.RecalculateHouseholdsRequest;
import com.acme.customers.v1.RecalculateHouseholdsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.CollateralWorkflowServiceGrpc;
import com.acme.lending.v1.ListCollateralWorkflowsRequest;
import com.acme.lending.v1.ListCollateralWorkflowsResponse;
import com.acme.media.v1.MediaAssetServiceGrpc;
import com.acme.media.v1.WatchMediaAssetsRequest;
import com.acme.media.v1.WatchMediaAssetsResponse;
import com.acme.notifications.v1.FindNotificationsRequest;
import com.acme.notifications.v1.FindNotificationsResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.onboarding.v1.ListOnboardingPlansRequest;
import com.acme.onboarding.v1.ListOnboardingPlansResponse;
import com.acme.onboarding.v1.OnboardingPlanServiceGrpc;
import com.acme.performers.v1.ClonePerformerRequest;
import com.acme.performers.v1.ClonePerformerResponse;
import com.acme.performers.v1.Performer;
import com.acme.performers.v1.PerformerServiceGrpc;
import com.acme.procurement.v1.AdvanceGoodsReceiptWorkflowRequest;
import com.acme.procurement.v1.AdvanceGoodsReceiptWorkflowResponse;
import com.acme.procurement.v1.CancelGoodsReceiptWorkflowRequest;
import com.acme.procurement.v1.CancelGoodsReceiptWorkflowResponse;
import com.acme.procurement.v1.GenerateRfqReportRequest;
import com.acme.procurement.v1.GenerateRfqReportResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.ListGoodsReceiptWorkflowsRequest;
import com.acme.procurement.v1.ListGoodsReceiptWorkflowsResponse;
import com.acme.procurement.v1.ListPendingGoodsReceiptsRequest;
import com.acme.procurement.v1.ListPendingGoodsReceiptsResponse;
import com.acme.procurement.v1.PauseGoodsReceiptWorkflowRequest;
import com.acme.procurement.v1.PauseGoodsReceiptWorkflowResponse;
import com.acme.procurement.v1.PreviewPurchaseRequisitionRequest;
import com.acme.procurement.v1.PreviewPurchaseRequisitionResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderLifecycleServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionLine;
import com.acme.procurement.v1.PurchaseRequisitionLineTax;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.procurement.v1.RecallRfqRequest;
import com.acme.procurement.v1.RecallRfqResponse;
import com.acme.procurement.v1.ResumeGoodsReceiptWorkflowRequest;
import com.acme.procurement.v1.ResumeGoodsReceiptWorkflowResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqApprovalServiceGrpc;
import com.acme.procurement.v1.RfqReportingServiceGrpc;
import com.acme.procurement.v1.SchedulePurchaseOrderExpiryRequest;
import com.acme.procurement.v1.SchedulePurchaseOrderExpiryResponse;
import com.acme.procurement.v1.ScheduleRfqReportRequest;
import com.acme.procurement.v1.ScheduleRfqReportResponse;
import com.acme.procurement.v1.SignalGoodsReceiptWorkflowRequest;
import com.acme.procurement.v1.SignalGoodsReceiptWorkflowResponse;
import com.acme.procurement.v1.StartGoodsReceiptWorkflowRequest;
import com.acme.procurement.v1.StartGoodsReceiptWorkflowResponse;
import com.acme.procurement.v1.ValidatePurchaseOrderRequest;
import com.acme.procurement.v1.ValidatePurchaseOrderResponse;
import com.acme.scheduling.v1.ShiftQueryServiceGrpc;
import com.acme.scheduling.v1.SuggestShiftsRequest;
import com.acme.scheduling.v1.SuggestShiftsResponse;
import com.acme.sellers.v1.SuggestSellersRequest;
import com.acme.sellers.v1.SuggestSellersResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
 * Runs the long-lived workflow that takes a goods receipt from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class GoodsReceiptWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(GoodsReceiptWorkflowApplication.class);

    private final ChartOfAccountsServiceGrpc.ChartOfAccountsServiceStub accountingChartOfAccountsServiceAsync;

    private final DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync;

    private final FixedAssetLifecycleServiceGrpc.FixedAssetLifecycleServiceBlockingStub assetsFixedAssetLifecycleServiceBlocking;

    private final AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking;

    private final AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking;

    private final BudgetApprovalServiceGrpc.BudgetApprovalServiceBlockingStub budgetingBudgetApprovalServiceBlocking;

    private final ChannelRegistry channels;

    private final WriteOffWorkflowServiceGrpc.WriteOffWorkflowServiceBlockingStub collectionsWriteOffWorkflowServiceBlocking;

    private final ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking;

    private final CollateralWorkflowServiceGrpc.CollateralWorkflowServiceBlockingStub lendingCollateralWorkflowServiceBlocking;

    private final MediaAssetServiceGrpc.MediaAssetServiceBlockingStub mediaMediaAssetServiceBlocking;

    private final NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking;

    private final OnboardingPlanServiceGrpc.OnboardingPlanServiceBlockingStub onboardingOnboardingPlanServiceBlocking;

    private final PerformerServiceGrpc.PerformerServiceBlockingStub performersPerformerServiceBlocking;

    private final PurchaseOrderLifecycleServiceGrpc.PurchaseOrderLifecycleServiceBlockingStub procurementPurchaseOrderLifecycleServiceBlocking;

    private final PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking;

    private final RfqApprovalServiceGrpc.RfqApprovalServiceBlockingStub procurementRfqApprovalServiceBlocking;

    private final RfqReportingServiceGrpc.RfqReportingServiceBlockingStub procurementRfqReportingServiceBlocking;

    private final ProcurementRepository repository;

    private final Executor rpcExecutor;

    private final ShiftQueryServiceGrpc.ShiftQueryServiceBlockingStub schedulingShiftQueryServiceBlocking;

    public GoodsReceiptWorkflowApplication(
            ChartOfAccountsServiceGrpc.ChartOfAccountsServiceStub accountingChartOfAccountsServiceAsync,
            DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync,
            FixedAssetLifecycleServiceGrpc.FixedAssetLifecycleServiceBlockingStub assetsFixedAssetLifecycleServiceBlocking,
            AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking,
            AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking,
            BudgetApprovalServiceGrpc.BudgetApprovalServiceBlockingStub budgetingBudgetApprovalServiceBlocking,
            ChannelRegistry channels,
            WriteOffWorkflowServiceGrpc.WriteOffWorkflowServiceBlockingStub collectionsWriteOffWorkflowServiceBlocking,
            ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking,
            CollateralWorkflowServiceGrpc.CollateralWorkflowServiceBlockingStub lendingCollateralWorkflowServiceBlocking,
            MediaAssetServiceGrpc.MediaAssetServiceBlockingStub mediaMediaAssetServiceBlocking,
            NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking,
            OnboardingPlanServiceGrpc.OnboardingPlanServiceBlockingStub onboardingOnboardingPlanServiceBlocking,
            PerformerServiceGrpc.PerformerServiceBlockingStub performersPerformerServiceBlocking,
            PurchaseOrderLifecycleServiceGrpc.PurchaseOrderLifecycleServiceBlockingStub procurementPurchaseOrderLifecycleServiceBlocking,
            PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking,
            RfqApprovalServiceGrpc.RfqApprovalServiceBlockingStub procurementRfqApprovalServiceBlocking,
            RfqReportingServiceGrpc.RfqReportingServiceBlockingStub procurementRfqReportingServiceBlocking,
            ProcurementRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            ShiftQueryServiceGrpc.ShiftQueryServiceBlockingStub schedulingShiftQueryServiceBlocking) {
        this.accountingChartOfAccountsServiceAsync = accountingChartOfAccountsServiceAsync;
        this.assetsDisposalSyncServiceAsync = assetsDisposalSyncServiceAsync;
        this.assetsFixedAssetLifecycleServiceBlocking = assetsFixedAssetLifecycleServiceBlocking;
        this.auditAuditTrailNotificationServiceBlocking = auditAuditTrailNotificationServiceBlocking;
        this.auditAuditTrailValidationServiceBlocking = auditAuditTrailValidationServiceBlocking;
        this.budgetingBudgetApprovalServiceBlocking = budgetingBudgetApprovalServiceBlocking;
        this.channels = channels;
        this.collectionsWriteOffWorkflowServiceBlocking = collectionsWriteOffWorkflowServiceBlocking;
        this.customersConsentRecordCalculationServiceBlocking = customersConsentRecordCalculationServiceBlocking;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.customersHouseholdQueryServiceBlocking = customersHouseholdQueryServiceBlocking;
        this.lendingCollateralWorkflowServiceBlocking = lendingCollateralWorkflowServiceBlocking;
        this.mediaMediaAssetServiceBlocking = mediaMediaAssetServiceBlocking;
        this.notificationsNotificationQueryServiceBlocking = notificationsNotificationQueryServiceBlocking;
        this.onboardingOnboardingPlanServiceBlocking = onboardingOnboardingPlanServiceBlocking;
        this.performersPerformerServiceBlocking = performersPerformerServiceBlocking;
        this.procurementPurchaseOrderLifecycleServiceBlocking = procurementPurchaseOrderLifecycleServiceBlocking;
        this.procurementPurchaseRequisitionValidationServiceBlocking = procurementPurchaseRequisitionValidationServiceBlocking;
        this.procurementRfqApprovalServiceBlocking = procurementRfqApprovalServiceBlocking;
        this.procurementRfqReportingServiceBlocking = procurementRfqReportingServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.schedulingShiftQueryServiceBlocking = schedulingShiftQueryServiceBlocking;
    }

    /**
     * Starts a goods receipt.
     */
    public StartGoodsReceiptWorkflowResponse startGoodsReceiptWorkflow(StartGoodsReceiptWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartGoodsReceiptWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var outbound0 = EscalateBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setCard(BudgetCardFunding.newBuilder()
                                .setHolderName(switch (request.getGoodsReceipt().getSettlementCase()) {
                                    case CARD -> request.getGoodsReceipt().getCard().getHolderName();
                                    default -> "";
                                })
                                .setToken(switch (request.getGoodsReceipt().getSettlementCase()) {
                                    case CARD -> request.getGoodsReceipt().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(EscalateBudgetRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = budgetingBudgetApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateBudget(outbound0);

        var results1 = new ArrayList<PreviewPurchaseRequisitionResponse>();
        for (var element : request.getGoodsReceipt().getLinesList()) {
            PreviewPurchaseRequisitionRequest itemRequest = PreviewPurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .addLines(PurchaseRequisitionLine.newBuilder()
                                .setSku(element.getSku())
                                .setTax(PurchaseRequisitionLineTax.newBuilder()
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .build())
                                .setUnitPrice(element.getUnitPrice())
                                .build())
                        .build())
                .build();
            results1.add(procurementPurchaseRequisitionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewPurchaseRequisition(itemRequest));
        }
        var reply1 = results1.isEmpty() ? PreviewPurchaseRequisitionResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = BatchCreateChartOfAccountsesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateChartOfAccountsesRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotify())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateChartOfAccountsesResponse.getDefaultInstance());
        var input2 = accountingChartOfAccountsServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateChartOfAccountses(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamDisposalChangesRequest.newBuilder()
                .setChange(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getGoodsReceipt().getCounterparty().getDisplayName())
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getGoodsReceipt().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamDisposalChangesResponse.getDefaultInstance());
        var input3 = assetsDisposalSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamDisposalChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartGoodsReceiptWorkflow", request.getGoodsReceipt().getGoodsReceiptId());
        return StartGoodsReceiptWorkflowResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setBillingAddress(request.getGoodsReceipt().getBillingAddress())
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setNumber(reply0.getBudget().getHolder().getNumber())
                                .build())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .build())
                .build();
    }

    /**
     * Advances a goods receipt.
     */
    public AdvanceGoodsReceiptWorkflowResponse advanceGoodsReceiptWorkflow(AdvanceGoodsReceiptWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceGoodsReceiptWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        List<PreviewAuditTrailResponse> results0 = request.getGoodsReceipt().getAttachmentsList().stream()
                .map(element -> PreviewAuditTrailRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .addAttachments(AuditTrailAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setContentType(element.getContentType())
                                .setSizeBytes(element.getSizeBytes())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .flatMap(value -> Stream.of(auditAuditTrailValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewAuditTrail(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? PreviewAuditTrailResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = JsonCodec.roundTrip(GenerateRfqReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(GenerateRfqReportRequest.Details.newBuilder()
                        .putAllFilters(request.getGoodsReceipt().getMetadataMap())
                        .build())
                .setReason(request.getReason())
                .build(), GenerateRfqReportRequest.class);
        var reply1 = procurementRfqReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generateRfqReport(outbound1);

        var outbound2 = FindHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindHouseholdsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getGoodsReceipt().getAudit().getUpdatedAt())
                        .putAllLabels(reply1.getResult().getDetailsMap())
                        .setOwnerId(request.getGoodsReceipt().getGoodsReceiptId())
                        .setRegion(reply0.getAuditTrail().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = Optional.of(outbound2).map(value -> value.toBuilder().build())
                .map(value -> customersHouseholdQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findHouseholds(value)).orElseThrow();

        var outbound3 = SuggestSellersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestSellersRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getAuditTrail().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(request.getGoodsReceipt().getGoodsReceiptId())
                        .setRegion(request.getGoodsReceipt().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.sellers.v1.SellerQueryService");
        var serviceName3 = "acme.sellers.v1" + "." + "SellerQueryService";
        var methodName3 = "Sug" + "gestSellers";
        var descriptor3 = MethodDescriptor.<SuggestSellersRequest, SuggestSellersResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName3, methodName3))
                .setRequestMarshaller(ProtoUtils.marshaller(SuggestSellersRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SuggestSellersResponse.getDefaultInstance()))
                .build();
        var reply3 = ClientCalls.blockingUnaryCall(channel3, descriptor3,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound3);
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceGoodsReceiptWorkflow", request.getGoodsReceipt().getNumber());
        return AdvanceGoodsReceiptWorkflowResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setBillingAddress(request.getGoodsReceipt().getBillingAddress())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .build())
                .setOutcome(AdvanceGoodsReceiptWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Cancels a goods receipt.
     */
    public CancelGoodsReceiptWorkflowResponse cancelGoodsReceiptWorkflow(CancelGoodsReceiptWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelGoodsReceiptWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var outbound0 = WatchMediaAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setName(request.getDetails().getNotify().getName())
                        .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                        .build())
                .build();
        var stream0 = mediaMediaAssetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchMediaAssets(outbound0);
        var reply0 = WatchMediaAssetsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = JsonCodec.roundTrip(ClonePerformerRequest.newBuilder()
                .setContext(request.getContext())
                .setPerformer(Performer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getGoodsReceipt().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build(), ClonePerformerRequest.class);
        var reply1 = performersPerformerServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).clonePerformer(outbound1);

        var outbound2 = ListOnboardingPlansRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListOnboardingPlansRequest.Filter.newBuilder()
                        .setRegion(request.getGoodsReceipt().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(onboardingOnboardingPlanServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listOnboardingPlans(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound3 = (PauseWriteOffWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getGoodsReceipt().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply3 = collectionsWriteOffWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseWriteOffWorkflow(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelGoodsReceiptWorkflow", request.getGoodsReceipt().getGoodsReceiptId());
        return CancelGoodsReceiptWorkflowResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .setIssuedAt(reply3.getWriteOff().getIssuedAt())
                        .setNumber(reply1.getPerformer().getPassport().getNumber())
                        .setStatus(request.getGoodsReceipt().getStatus())
                        .build())
                .setOutcome(CancelGoodsReceiptWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Pauses a goods receipt.
     */
    public PauseGoodsReceiptWorkflowResponse pauseGoodsReceiptWorkflow(PauseGoodsReceiptWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseGoodsReceiptWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var outbound0 = ScheduleFixedAssetExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getGoodsReceipt().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getGoodsReceipt().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getGoodsReceipt().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        Function<ScheduleFixedAssetExpiryRequest, ScheduleFixedAssetExpiryResponse> operation0 = assetsFixedAssetLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scheduleFixedAssetExpiry;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = FindNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindNotificationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getGoodsReceipt().getAudit().getUpdatedAt())
                        .setOwnerId(reply0.getFixedAsset().getComponentsOrDefault("primary", FixedAssetComponent.getDefaultInstance()).getComponentId())
                        .setRegion(request.getGoodsReceipt().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> notificationsNotificationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findNotifications(value)).orElseThrow();

        var outbound2 = SuggestShiftsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestShiftsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getGoodsReceipt().getAudit().getUpdatedAt())
                        .setOwnerId(request.getGoodsReceipt().getCounterparty().getPurchaseRequisitionId())
                        .setRegion(reply0.getFixedAsset().getAddress().getRegion())
                        .build())
                .build();
        final class Stage2Adapter {
            SuggestShiftsResponse fetch(SuggestShiftsRequest input) {
                return schedulingShiftQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestShifts(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);

        var outbound3 = RpcSupport.unpack(Any.pack(ListCollateralWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListCollateralWorkflowsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getFixedAsset().getAudit().getUpdatedAt())
                        .setRegion(request.getGoodsReceipt().getBillingAddress().getRegion())
                        .build())
                .build()), ListCollateralWorkflowsRequest.class);
        var reply3 = lendingCollateralWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listCollateralWorkflows(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseGoodsReceiptWorkflow", request.getGoodsReceipt().getGoodsReceiptId());
        return PauseGoodsReceiptWorkflowResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .setNumber(request.getGoodsReceipt().getNumber())
                        .build())
                .setOutcome(PauseGoodsReceiptWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Resumes a goods receipt.
     */
    public ResumeGoodsReceiptWorkflowResponse resumeGoodsReceiptWorkflow(ResumeGoodsReceiptWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeGoodsReceiptWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var outbound0 = ScheduleRfqReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(ScheduleRfqReportRequest.Details.newBuilder()
                        .putAllFilters(request.getDetails().getParametersMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> procurementRfqReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleRfqReport(outbound0), rpcExecutor);

        var outbound1 = RecallRfqRequest.newBuilder()
                .setContext(request.getContext())
                .setRfq(Rfq.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotify().getName())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> procurementRfqApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallRfq(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = StreamAuditTrailNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setName(reply1.getRfq().getRequester().getName())
                        .setPhone(request.getDetails().getNotify().getPhone())
                        .build())
                .build();
        var stream2 = auditAuditTrailNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditTrailNotices(outbound2);
        var reply2 = StreamAuditTrailNoticesResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }

        var outbound3 = RpcSupport.unpack(Any.pack(RecalculateHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setCountryCode(request.getGoodsReceipt().getBillingAddress().getCountryCode())
                                                .setRecipient(request.getGoodsReceipt().getBillingAddress().getRecipient())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(reply1.getRfq().getRequester().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build()), RecalculateHouseholdsRequest.class);
        var reply3 = customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateHouseholds(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeGoodsReceiptWorkflow", request.getGoodsReceipt().getGoodsReceiptId());
        return ResumeGoodsReceiptWorkflowResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply3.getHousehold().getAudit().getCreatedBy())
                                .build())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .setTotal(request.getGoodsReceipt().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists goods receipts a page at a time.
     */
    public ListGoodsReceiptWorkflowsResponse listGoodsReceiptWorkflows(ListGoodsReceiptWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListGoodsReceiptWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "GoodsReceipt", request.getContext().getTraceId(), "ListGoodsReceiptWorkflows");

        var outbound0 = (SchedulePurchaseOrderExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SchedulePurchaseOrderExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = procurementPurchaseOrderLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).schedulePurchaseOrderExpiry(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListGoodsReceiptWorkflows", request.getPage().getPageToken());
        return ListGoodsReceiptWorkflowsResponse.newBuilder()
                .addGoodsReceipts(GoodsReceipt.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getPurchaseOrder().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
    }

    /**
     * Signals a goods receipt.
     */
    public SignalGoodsReceiptWorkflowResponse signalGoodsReceiptWorkflow(SignalGoodsReceiptWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalGoodsReceiptWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var outbound0 = ValidatePurchaseOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setDisplayName(request.getGoodsReceipt().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.procurement.v1.PurchaseOrderValidationService");
        var serviceName0 = "acme.procurement.v1" + "." + "PurchaseOrderValidationService";
        var methodName0 = "Val" + "idatePurchaseOrder";
        var descriptor0 = MethodDescriptor.<ValidatePurchaseOrderRequest, ValidatePurchaseOrderResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ValidatePurchaseOrderRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ValidatePurchaseOrderResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = EstimateConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(EstimateConsentRecordRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getPurchaseOrder().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var reply1 = customersConsentRecordCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateConsentRecord(outbound1);

        var outbound2 = ListPendingGoodsReceiptsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingGoodsReceiptsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getPurchaseOrder().getAudit().getCreatedAt())
                        .setRegion(reply0.getPurchaseOrder().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.procurement.v1.GoodsReceiptApprovalService");
        var serviceName2 = "acme.procurement.v1" + "." + "GoodsReceiptApprovalService";
        var methodName2 = "Lis" + "tPendingGoodsReceipts";
        var descriptor2 = MethodDescriptor.<ListPendingGoodsReceiptsRequest, ListPendingGoodsReceiptsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(ListPendingGoodsReceiptsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ListPendingGoodsReceiptsResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalGoodsReceiptWorkflow", request.getGoodsReceipt().getGoodsReceiptId());
        return SignalGoodsReceiptWorkflowResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .putAllMetadata(request.getGoodsReceipt().getMetadataMap())
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
