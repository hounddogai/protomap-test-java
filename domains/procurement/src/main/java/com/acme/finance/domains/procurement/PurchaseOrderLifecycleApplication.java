package com.acme.finance.domains.procurement;

import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.CloneAccessPolicyRequest;
import com.acme.authorization.v1.CloneAccessPolicyResponse;
import com.acme.budgeting.v1.BudgetLineRef;
import com.acme.budgeting.v1.EscalateVarianceRequest;
import com.acme.budgeting.v1.EscalateVarianceResponse;
import com.acme.budgeting.v1.SubmitVarianceRequest;
import com.acme.budgeting.v1.SubmitVarianceResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceApprovalServiceGrpc;
import com.acme.budgeting.v1.VarianceComment;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationBankSettlement;
import com.acme.compliance.v1.AttestationWorkflowServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseRef;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlBlockAction;
import com.acme.compliance.v1.ControlWorkflowServiceGrpc;
import com.acme.compliance.v1.ListAttestationWorkflowsRequest;
import com.acme.compliance.v1.ListAttestationWorkflowsResponse;
import com.acme.compliance.v1.PauseAttestationWorkflowRequest;
import com.acme.compliance.v1.PauseAttestationWorkflowResponse;
import com.acme.compliance.v1.StartControlWorkflowRequest;
import com.acme.compliance.v1.StartControlWorkflowResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.FindCustomersRequest;
import com.acme.customers.v1.FindCustomersResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.RecalculatePreferencesRequest;
import com.acme.customers.v1.RecalculatePreferencesResponse;
import com.acme.customers.v1.ResolveHouseholdsRequest;
import com.acme.customers.v1.ResolveHouseholdsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.PolicyLifecycleServiceGrpc;
import com.acme.insurance.v1.SchedulePolicyExpiryRequest;
import com.acme.insurance.v1.SchedulePolicyExpiryResponse;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionRequest;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.PreviewNotificationRequest;
import com.acme.notifications.v1.PreviewNotificationResponse;
import com.acme.procurement.v1.ActivatePurchaseOrderRequest;
import com.acme.procurement.v1.ActivatePurchaseOrderResponse;
import com.acme.procurement.v1.CalculatePurchaseRequisitionRequest;
import com.acme.procurement.v1.CalculatePurchaseRequisitionResponse;
import com.acme.procurement.v1.ExpirePurchaseOrderRequest;
import com.acme.procurement.v1.ExpirePurchaseOrderResponse;
import com.acme.procurement.v1.ExportPurchaseRequisitionsRequest;
import com.acme.procurement.v1.ExportPurchaseRequisitionsResponse;
import com.acme.procurement.v1.FindPurchaseOrdersRequest;
import com.acme.procurement.v1.FindPurchaseOrdersResponse;
import com.acme.procurement.v1.GetPurchaseOrderLifecycleRequest;
import com.acme.procurement.v1.GetPurchaseOrderLifecycleResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptLine;
import com.acme.procurement.v1.GoodsReceiptValidationServiceGrpc;
import com.acme.procurement.v1.ListPurchaseRequisitionsRequest;
import com.acme.procurement.v1.ListPurchaseRequisitionsResponse;
import com.acme.procurement.v1.PreviewPurchaseOrderRequest;
import com.acme.procurement.v1.PreviewPurchaseOrderResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderQueryServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionBankSettlement;
import com.acme.procurement.v1.PurchaseRequisitionCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionQueryServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.acme.procurement.v1.ReinstatePurchaseOrderRequest;
import com.acme.procurement.v1.ReinstatePurchaseOrderResponse;
import com.acme.procurement.v1.RenewPurchaseOrderRequest;
import com.acme.procurement.v1.RenewPurchaseOrderResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.procurement.v1.SchedulePurchaseOrderExpiryRequest;
import com.acme.procurement.v1.SchedulePurchaseOrderExpiryResponse;
import com.acme.procurement.v1.ScoreGoodsReceiptRequest;
import com.acme.procurement.v1.ScoreGoodsReceiptResponse;
import com.acme.procurement.v1.SearchPurchaseRequisitionsRequest;
import com.acme.procurement.v1.SearchPurchaseRequisitionsResponse;
import com.acme.procurement.v1.SimulateRfqRequest;
import com.acme.procurement.v1.SimulateRfqResponse;
import com.acme.procurement.v1.SummarizePurchaseRequisitionsRequest;
import com.acme.procurement.v1.SummarizePurchaseRequisitionsResponse;
import com.acme.procurement.v1.SuspendPurchaseOrderRequest;
import com.acme.procurement.v1.SuspendPurchaseOrderResponse;
import com.acme.procurement.v1.TerminatePurchaseOrderRequest;
import com.acme.procurement.v1.TerminatePurchaseOrderResponse;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Moves a purchase order between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class PurchaseOrderLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(PurchaseOrderLifecycleApplication.class);

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final VarianceApprovalServiceGrpc.VarianceApprovalServiceBlockingStub budgetingVarianceApprovalServiceBlocking;

    private final ChannelRegistry channels;

    private final AttestationWorkflowServiceGrpc.AttestationWorkflowServiceBlockingStub complianceAttestationWorkflowServiceBlocking;

    private final ControlWorkflowServiceGrpc.ControlWorkflowServiceBlockingStub complianceControlWorkflowServiceBlocking;

    private final PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final PolicyLifecycleServiceGrpc.PolicyLifecycleServiceBlockingStub insurancePolicyLifecycleServiceBlocking;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking;

    private final PurchaseOrderLifecycleRequestMapper mapper;

    private final NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking;

    private final GoodsReceiptValidationServiceGrpc.GoodsReceiptValidationServiceBlockingStub procurementGoodsReceiptValidationServiceBlocking;

    private final PurchaseOrderQueryServiceGrpc.PurchaseOrderQueryServiceBlockingStub procurementPurchaseOrderQueryServiceBlocking;

    private final PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceBlockingStub procurementPurchaseRequisitionCalculationServiceBlocking;

    private final PurchaseRequisitionQueryServiceGrpc.PurchaseRequisitionQueryServiceBlockingStub procurementPurchaseRequisitionQueryServiceBlocking;

    private final PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceBlockingStub procurementPurchaseRequisitionServiceBlocking;

    private final PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceFutureStub procurementPurchaseRequisitionServiceFuture;

    private final RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking;

    private final ProcurementRepository repository;

    private final Executor rpcExecutor;

    public PurchaseOrderLifecycleApplication(
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            VarianceApprovalServiceGrpc.VarianceApprovalServiceBlockingStub budgetingVarianceApprovalServiceBlocking,
            ChannelRegistry channels,
            AttestationWorkflowServiceGrpc.AttestationWorkflowServiceBlockingStub complianceAttestationWorkflowServiceBlocking,
            ControlWorkflowServiceGrpc.ControlWorkflowServiceBlockingStub complianceControlWorkflowServiceBlocking,
            PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking,
            ApplicationEventPublisher events,
            PolicyLifecycleServiceGrpc.PolicyLifecycleServiceBlockingStub insurancePolicyLifecycleServiceBlocking,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking,
            PurchaseOrderLifecycleRequestMapper mapper,
            NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking,
            GoodsReceiptValidationServiceGrpc.GoodsReceiptValidationServiceBlockingStub procurementGoodsReceiptValidationServiceBlocking,
            PurchaseOrderQueryServiceGrpc.PurchaseOrderQueryServiceBlockingStub procurementPurchaseOrderQueryServiceBlocking,
            PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceBlockingStub procurementPurchaseRequisitionCalculationServiceBlocking,
            PurchaseRequisitionQueryServiceGrpc.PurchaseRequisitionQueryServiceBlockingStub procurementPurchaseRequisitionQueryServiceBlocking,
            PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceBlockingStub procurementPurchaseRequisitionServiceBlocking,
            PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceFutureStub procurementPurchaseRequisitionServiceFuture,
            RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking,
            ProcurementRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.budgetingVarianceApprovalServiceBlocking = budgetingVarianceApprovalServiceBlocking;
        this.channels = channels;
        this.complianceAttestationWorkflowServiceBlocking = complianceAttestationWorkflowServiceBlocking;
        this.complianceControlWorkflowServiceBlocking = complianceControlWorkflowServiceBlocking;
        this.customersPreferenceCalculationServiceBlocking = customersPreferenceCalculationServiceBlocking;
        this.events = events;
        this.insurancePolicyLifecycleServiceBlocking = insurancePolicyLifecycleServiceBlocking;
        this.ledgerLedgerAccountHistoryServiceBlocking = ledgerLedgerAccountHistoryServiceBlocking;
        this.mapper = mapper;
        this.notificationsNotificationValidationServiceBlocking = notificationsNotificationValidationServiceBlocking;
        this.procurementGoodsReceiptValidationServiceBlocking = procurementGoodsReceiptValidationServiceBlocking;
        this.procurementPurchaseOrderQueryServiceBlocking = procurementPurchaseOrderQueryServiceBlocking;
        this.procurementPurchaseRequisitionCalculationServiceBlocking = procurementPurchaseRequisitionCalculationServiceBlocking;
        this.procurementPurchaseRequisitionQueryServiceBlocking = procurementPurchaseRequisitionQueryServiceBlocking;
        this.procurementPurchaseRequisitionServiceBlocking = procurementPurchaseRequisitionServiceBlocking;
        this.procurementPurchaseRequisitionServiceFuture = procurementPurchaseRequisitionServiceFuture;
        this.procurementRfqCalculationServiceBlocking = procurementRfqCalculationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    public record ActivatePurchaseOrderStage1Requested(PreviewPurchaseOrderRequest request, CompletableFuture<PreviewPurchaseOrderResponse> completion, io.grpc.Context context) {}

    /**
     * Activates a purchase order.
     */
    public ActivatePurchaseOrderResponse activatePurchaseOrder(ActivatePurchaseOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivatePurchaseOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseOrder", request.getPurchaseOrderId(), "ActivatePurchaseOrder");

        var outbound0 = SimulateRfqRequest.newBuilder()
                .setContext(request.getContext())
                .setRfq(Rfq.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> procurementRfqCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateRfq(value));

        var outbound1 = PreviewPurchaseOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewPurchaseOrderRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setName(reply0.getRfq().getRequester().getName())
                                .setPhone(reply0.getRfq().getRequester().getPhone())
                                .build())
                        .build())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getRfq().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<PreviewPurchaseOrderResponse>();
        events.publishEvent(new ActivatePurchaseOrderStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = PauseAttestationWorkflowRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setBank(AttestationBankSettlement.newBuilder()
                                .setBic(switch (reply1.getPurchaseOrder().getSettlementCase()) {
                                    case BANK -> reply1.getPurchaseOrder().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(ComplianceCaseRef.newBuilder()
                                .setDisplayName(reply1.getPurchaseOrder().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PauseAttestationWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setName(reply0.getRfq().getRequester().getName())
                                .build())
                        .build())
                .build();
        var reply2 = complianceAttestationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseAttestationWorkflow(outbound2);

        var outbound3 = ListAttestationWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAttestationWorkflowsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply2.getAttestation().getAudit().getCreatedAt())
                        .setRegion(reply1.getPurchaseOrder().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.compliance.v1.AttestationWorkflowService");
        var selected3 = AttestationWorkflowServiceGrpc.newBlockingStub(channel3)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply3 = selected3.listAttestationWorkflows(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivatePurchaseOrder", request.getDetails().getReasonCode());
        return ActivatePurchaseOrderResponse.newBuilder()
                .setOutcome(ActivatePurchaseOrderResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply2.getAttestation().getBillingAddress().getCountryCode())
                                .addAllLines(reply1.getPurchaseOrder().getBillingAddress().getLinesList())
                                .build())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Suspends a purchase order.
     */
    public SuspendPurchaseOrderResponse suspendPurchaseOrder(SuspendPurchaseOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendPurchaseOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseOrder", request.getPurchaseOrderId(), "SuspendPurchaseOrder");

        var outbound0 = ExportPurchaseRequisitionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportPurchaseRequisitionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var stream0 = procurementPurchaseRequisitionQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportPurchaseRequisitions(outbound0);
        var reply0 = ExportPurchaseRequisitionsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = SearchPurchaseRequisitionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchPurchaseRequisitionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> procurementPurchaseRequisitionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchPurchaseRequisitions(value));

        var outbound2 = SummarizePurchaseRequisitionsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizePurchaseRequisitionsRequest.Details.newBuilder()
                        .putAllFilters(reply1.getResult().getDetailsMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var channel2 = channels.lookup("acme.procurement.v1.PurchaseRequisitionQueryService");
        var serviceName2 = "acme.procurement.v1" + "." + "PurchaseRequisitionQueryService";
        var methodName2 = "Sum" + "marizePurchaseRequisitions";
        var descriptor2 = MethodDescriptor.<SummarizePurchaseRequisitionsRequest, SummarizePurchaseRequisitionsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(SummarizePurchaseRequisitionsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SummarizePurchaseRequisitionsResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendPurchaseOrder", request.getDetails().getReasonCode());
        return SuspendPurchaseOrderResponse.newBuilder()
                .setOutcome(SuspendPurchaseOrderResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Reinstates a purchase order.
     */
    public ReinstatePurchaseOrderResponse reinstatePurchaseOrder(ReinstatePurchaseOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstatePurchaseOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseOrder", request.getPurchaseOrderId(), "ReinstatePurchaseOrder");

        var outbound0 = (PreviewNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = notificationsNotificationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewNotification(outbound0);

        var outbound1 = (EscalateVarianceRequest.newBuilder()
                .setContext(request.getContext())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getNotification().getAssignee().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .setCustomer(BudgetLineRef.newBuilder()
                                .setDisplayName(reply0.getNotification().getAssignee().getDisplayName())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = budgetingVarianceApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateVariance(outbound1);

        var outbound2 = ResolveHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var channel2 = channels.lookup("acme.customers.v1.HouseholdQueryService");
        var serviceName2 = "acme.customers.v1" + "." + "HouseholdQueryService";
        var methodName2 = "Res" + "olveHouseholds";
        var descriptor2 = MethodDescriptor.<ResolveHouseholdsRequest, ResolveHouseholdsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(ResolveHouseholdsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ResolveHouseholdsResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = RecalculatePreferencesRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getNotification().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        RpcPort<RecalculatePreferencesRequest, RecalculatePreferencesResponse> port3 = RpcPort.adapt(customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::recalculatePreferences);
        var reply3 = port3.exchange(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstatePurchaseOrder", request.getDetails().getReasonCode());
        return ReinstatePurchaseOrderResponse.newBuilder()
                .setOutcome(ReinstatePurchaseOrderResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setDueAt(reply1.getVariance().getSla().getDueAt())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Terminates a purchase order.
     */
    public TerminatePurchaseOrderResponse terminatePurchaseOrder(TerminatePurchaseOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminatePurchaseOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseOrder", request.getPurchaseOrderId(), "TerminatePurchaseOrder");

        var builder0 = AnnotateLedgerAccountVersionRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(AnnotateLedgerAccountVersionRequest.Details.newBuilder()
                .setSnapshot(JsonCodec.encode(request.getContext()))
                .build());
        builder0.setLedgerAccount(LedgerAccount.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setPhone(request.getDetails().getNotifyContact().getPhone())
                        .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                        .build())
                .setHolder(PostingBatchRef.newBuilder()
                        .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = ledgerLedgerAccountHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotateLedgerAccountVersion(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminatePurchaseOrder", request.getDetails().getReasonCode());
        return TerminatePurchaseOrderResponse.newBuilder()
                .setOutcome(TerminatePurchaseOrderResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getLedgerAccount().getAudit().getUpdatedAt())
                                .build())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .build();
    }

    /**
     * Renews a purchase order.
     */
    public RenewPurchaseOrderResponse renewPurchaseOrder(RenewPurchaseOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewPurchaseOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseOrder", request.getPurchaseOrderId(), "RenewPurchaseOrder");

        var outbound0 = ListPurchaseRequisitionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPurchaseRequisitionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .build())
                .build();
        var pending0 = procurementPurchaseRequisitionServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).listPurchaseRequisitions(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = StartControlWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setBlock(ControlBlockAction.newBuilder()
                                .setReason(request.getReason())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setDetails(StartControlWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> complianceControlWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startControlWorkflow(value)).orElseThrow();

        var outbound2 = FindPurchaseOrdersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindPurchaseOrdersRequest.Filter.newBuilder()
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(procurementPurchaseOrderQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findPurchaseOrders(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewPurchaseOrder", request.getDetails().getReasonCode());
        return RenewPurchaseOrderResponse.newBuilder()
                .setOutcome(RenewPurchaseOrderResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply1.getOutcome().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Expires a purchase order.
     */
    public ExpirePurchaseOrderResponse expirePurchaseOrder(ExpirePurchaseOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpirePurchaseOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseOrder", request.getPurchaseOrderId(), "ExpirePurchaseOrder");

        var outbound0 = BeanCopies.copy(SchedulePolicyExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SchedulePolicyExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .build(), SchedulePolicyExpiryRequest.class);
        var reply0 = insurancePolicyLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).schedulePolicyExpiry(outbound0);

        var outbound1 = mapper.expirePurchaseOrderStage1(request, reply0);
        var reply1 = procurementPurchaseOrderQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findPurchaseOrders(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpirePurchaseOrder", request.getDetails().getReasonCode());
        return ExpirePurchaseOrderResponse.newBuilder()
                .setOutcome(ExpirePurchaseOrderResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a purchase order.
     */
    public GetPurchaseOrderLifecycleResponse getPurchaseOrderLifecycle(GetPurchaseOrderLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPurchaseOrderLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseOrder", request.getPurchaseOrderId(), "GetPurchaseOrderLifecycle");

        var outbound0 = FindCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindCustomersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getPurchaseOrderId())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.customers.v1.CustomerQueryService");
        var serviceName0 = "acme.customers.v1" + "." + "CustomerQueryService";
        var methodName0 = "Fin" + "dCustomers";
        var descriptor0 = MethodDescriptor.<FindCustomersRequest, FindCustomersResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(FindCustomersRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(FindCustomersResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        return GetPurchaseOrderLifecycleResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Schedules a purchase order.
     */
    public SchedulePurchaseOrderExpiryResponse schedulePurchaseOrderExpiry(SchedulePurchaseOrderExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SchedulePurchaseOrderExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var futures0 = request.getPurchaseOrder().getLinesList().stream()
                .map(element -> ScoreGoodsReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .addLines(GoodsReceiptLine.newBuilder()
                                .setQuantity(element.getQuantity())
                                .setSku(element.getSku())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> procurementGoodsReceiptValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreGoodsReceipt(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? ScoreGoodsReceiptResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = SubmitVarianceRequest.newBuilder()
                .setContext(request.getContext())
                .setVariance(Variance.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getGoodsReceipt().getAudit().getEtag())
                                .build())
                        .addComments(VarianceComment.newBuilder()
                                .setCreatedAt(request.getPurchaseOrder().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var reply1 = budgetingVarianceApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitVariance(outbound1);

        var outbound2 = CalculatePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBank(PurchaseRequisitionBankSettlement.newBuilder()
                                .setIban(switch (reply0.getGoodsReceipt().getSettlementCase()) {
                                    case BANK -> reply0.getGoodsReceipt().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getGoodsReceipt().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var reply2 = procurementPurchaseRequisitionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePurchaseRequisition(outbound2);

        var outbound3 = (CloneAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getVariance().getAudit().getCreatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply2.getPurchaseRequisition().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply3 = authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneAccessPolicy(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SchedulePurchaseOrderExpiry", request.getPurchaseOrder().getPurchaseOrderId());
        return SchedulePurchaseOrderExpiryResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setBillingAddress(reply2.getPurchaseRequisition().getBillingAddress())
                        .setDueAt(reply2.getPurchaseRequisition().getDueAt())
                        .setIssuedAt(request.getPurchaseOrder().getIssuedAt())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }
}
