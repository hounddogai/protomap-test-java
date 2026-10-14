package com.acme.finance.domains.procurement;

import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetLifecycleServiceGrpc;
import com.acme.assets.v1.ScheduleFixedAssetExpiryRequest;
import com.acme.assets.v1.ScheduleFixedAssetExpiryResponse;
import com.acme.budgeting.v1.DelegateForecastApprovalRequest;
import com.acme.budgeting.v1.DelegateForecastApprovalResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.ExportCustomersRequest;
import com.acme.customers.v1.ExportCustomersResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.QuotePreferenceRequest;
import com.acme.customers.v1.QuotePreferenceResponse;
import com.acme.customers.v1.SimulatePreferenceRequest;
import com.acme.customers.v1.SimulatePreferenceResponse;
import com.acme.customers.v1.ValidatePreferenceRequest;
import com.acme.customers.v1.ValidatePreferenceResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentCardSettlement;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.ValidateDocumentRequest;
import com.acme.documents.v1.ValidateDocumentResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageApprovalServiceGrpc;
import com.acme.insurance.v1.CoverageRouteAction;
import com.acme.insurance.v1.SubmitCoverageRequest;
import com.acme.insurance.v1.SubmitCoverageResponse;
import com.acme.notifications.v1.CloneNotificationRequest;
import com.acme.notifications.v1.CloneNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.procurement.v1.ApprovePurchaseOrderRequest;
import com.acme.procurement.v1.ApprovePurchaseOrderResponse;
import com.acme.procurement.v1.CommentPurchaseOrderApprovalRequest;
import com.acme.procurement.v1.CommentPurchaseOrderApprovalResponse;
import com.acme.procurement.v1.EscalatePurchaseOrderRequest;
import com.acme.procurement.v1.EscalatePurchaseOrderResponse;
import com.acme.procurement.v1.ExpirePurchaseOrderRequest;
import com.acme.procurement.v1.ExpirePurchaseOrderResponse;
import com.acme.procurement.v1.FindPurchaseRequisitionsRequest;
import com.acme.procurement.v1.FindPurchaseRequisitionsResponse;
import com.acme.procurement.v1.GetPurchaseOrderApprovalStatusRequest;
import com.acme.procurement.v1.GetPurchaseOrderApprovalStatusResponse;
import com.acme.procurement.v1.LintPurchaseOrdersRequest;
import com.acme.procurement.v1.LintPurchaseOrdersResponse;
import com.acme.procurement.v1.ListPendingPurchaseOrdersRequest;
import com.acme.procurement.v1.ListPendingPurchaseOrdersResponse;
import com.acme.procurement.v1.NormalizeRfqRequest;
import com.acme.procurement.v1.NormalizeRfqResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderLifecycleServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderValidationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionQueryServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.QuotePurchaseRequisitionRequest;
import com.acme.procurement.v1.QuotePurchaseRequisitionResponse;
import com.acme.procurement.v1.RecallPurchaseOrderRequest;
import com.acme.procurement.v1.RecallPurchaseOrderResponse;
import com.acme.procurement.v1.RejectPurchaseOrderRequest;
import com.acme.procurement.v1.RejectPurchaseOrderResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqComment;
import com.acme.procurement.v1.RfqReportingServiceGrpc;
import com.acme.procurement.v1.RfqValidationServiceGrpc;
import com.acme.procurement.v1.StreamRfqMetricsRequest;
import com.acme.procurement.v1.StreamRfqMetricsResponse;
import com.acme.procurement.v1.SubmitPurchaseOrderRequest;
import com.acme.procurement.v1.SubmitPurchaseOrderResponse;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Routes purchase orders through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class PurchaseOrderApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(PurchaseOrderApprovalApplication.class);

    private final FixedAssetLifecycleServiceGrpc.FixedAssetLifecycleServiceBlockingStub assetsFixedAssetLifecycleServiceBlocking;

    private final ForecastApprovalServiceGrpc.ForecastApprovalServiceBlockingStub budgetingForecastApprovalServiceBlocking;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking;

    private final PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking;

    private final DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking;

    private final CoverageApprovalServiceGrpc.CoverageApprovalServiceBlockingStub insuranceCoverageApprovalServiceBlocking;

    private final PurchaseOrderApprovalRequestMapper mapper;

    private final NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking;

    private final PurchaseOrderLifecycleServiceGrpc.PurchaseOrderLifecycleServiceBlockingStub procurementPurchaseOrderLifecycleServiceBlocking;

    private final PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceBlockingStub procurementPurchaseOrderValidationServiceBlocking;

    private final PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceBlockingStub procurementPurchaseRequisitionCalculationServiceBlocking;

    private final PurchaseRequisitionQueryServiceGrpc.PurchaseRequisitionQueryServiceBlockingStub procurementPurchaseRequisitionQueryServiceBlocking;

    private final RfqReportingServiceGrpc.RfqReportingServiceBlockingStub procurementRfqReportingServiceBlocking;

    private final RfqValidationServiceGrpc.RfqValidationServiceBlockingStub procurementRfqValidationServiceBlocking;

    private final ProcurementRepository repository;

    public PurchaseOrderApprovalApplication(
            FixedAssetLifecycleServiceGrpc.FixedAssetLifecycleServiceBlockingStub assetsFixedAssetLifecycleServiceBlocking,
            ForecastApprovalServiceGrpc.ForecastApprovalServiceBlockingStub budgetingForecastApprovalServiceBlocking,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking,
            PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking,
            DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking,
            CoverageApprovalServiceGrpc.CoverageApprovalServiceBlockingStub insuranceCoverageApprovalServiceBlocking,
            PurchaseOrderApprovalRequestMapper mapper,
            NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking,
            PurchaseOrderLifecycleServiceGrpc.PurchaseOrderLifecycleServiceBlockingStub procurementPurchaseOrderLifecycleServiceBlocking,
            PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceBlockingStub procurementPurchaseOrderValidationServiceBlocking,
            PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceBlockingStub procurementPurchaseRequisitionCalculationServiceBlocking,
            PurchaseRequisitionQueryServiceGrpc.PurchaseRequisitionQueryServiceBlockingStub procurementPurchaseRequisitionQueryServiceBlocking,
            RfqReportingServiceGrpc.RfqReportingServiceBlockingStub procurementRfqReportingServiceBlocking,
            RfqValidationServiceGrpc.RfqValidationServiceBlockingStub procurementRfqValidationServiceBlocking,
            ProcurementRepository repository) {
        this.assetsFixedAssetLifecycleServiceBlocking = assetsFixedAssetLifecycleServiceBlocking;
        this.budgetingForecastApprovalServiceBlocking = budgetingForecastApprovalServiceBlocking;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.customersPreferenceCalculationServiceBlocking = customersPreferenceCalculationServiceBlocking;
        this.customersPreferenceValidationServiceBlocking = customersPreferenceValidationServiceBlocking;
        this.documentsDocumentValidationServiceBlocking = documentsDocumentValidationServiceBlocking;
        this.insuranceCoverageApprovalServiceBlocking = insuranceCoverageApprovalServiceBlocking;
        this.mapper = mapper;
        this.notificationsNotificationServiceBlocking = notificationsNotificationServiceBlocking;
        this.procurementPurchaseOrderLifecycleServiceBlocking = procurementPurchaseOrderLifecycleServiceBlocking;
        this.procurementPurchaseOrderValidationServiceBlocking = procurementPurchaseOrderValidationServiceBlocking;
        this.procurementPurchaseRequisitionCalculationServiceBlocking = procurementPurchaseRequisitionCalculationServiceBlocking;
        this.procurementPurchaseRequisitionQueryServiceBlocking = procurementPurchaseRequisitionQueryServiceBlocking;
        this.procurementRfqReportingServiceBlocking = procurementRfqReportingServiceBlocking;
        this.procurementRfqValidationServiceBlocking = procurementRfqValidationServiceBlocking;
        this.repository = repository;
    }

    /**
     * Submits a purchase order.
     */
    public SubmitPurchaseOrderResponse submitPurchaseOrder(SubmitPurchaseOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitPurchaseOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var outbound0 = LintPurchaseOrdersRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getPurchaseOrder().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var reply0 = procurementPurchaseOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintPurchaseOrders(outbound0);

        var outbound1 = mapper.submitPurchaseOrderStage1(request, reply0);
        var reply1 = customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulatePreference(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitPurchaseOrder", request.getPurchaseOrder().getPurchaseOrderId());
        return SubmitPurchaseOrderResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getPreference().getAudit().getUpdatedAt())
                                .build())
                        .setDueAt(request.getPurchaseOrder().getDueAt())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .setTotal(reply0.getPurchaseOrder().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Approves a purchase order.
     */
    public ApprovePurchaseOrderResponse approvePurchaseOrder(ApprovePurchaseOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApprovePurchaseOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var outbound0 = QuotePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuotePurchaseRequisitionRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getPurchaseOrder().getBillingAddress().getLocality())
                                .setPostalCode(request.getPurchaseOrder().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getPurchaseOrder().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var reply0 = procurementPurchaseRequisitionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePurchaseRequisition(outbound0);

        var outbound1 = JsonCodec.roundTrip(ValidateDocumentRequest.newBuilder()
                .setContext(request.getContext())
                .setDocument(Document.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getPurchaseOrder().getBillingAddress().getLocality())
                                .setPostalCode(reply0.getPurchaseRequisition().getBillingAddress().getPostalCode())
                                .build())
                        .setCard(DocumentCardSettlement.newBuilder()
                                .setHolderName(request.getPurchaseOrder().getCard().getHolderName())
                                .setNetwork(reply0.getPurchaseRequisition().getCard().getNetwork())
                                .build())
                        .build())
                .build(), ValidateDocumentRequest.class);
        var reply1 = documentsDocumentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateDocument(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApprovePurchaseOrder", request.getPurchaseOrder().getPurchaseOrderId());
        return ApprovePurchaseOrderResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply1.getDocument().getBillingAddress().getLinesList())
                                .build())
                        .setDueAt(reply0.getPurchaseRequisition().getDueAt())
                        .putAllMetadata(reply1.getDocument().getMetadataMap())
                        .setNotes(reply0.getPurchaseRequisition().getNotes())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .setStatus(request.getPurchaseOrder().getStatus())
                        .build())
                .build();
    }

    /**
     * Rejects a purchase order.
     */
    public RejectPurchaseOrderResponse rejectPurchaseOrder(RejectPurchaseOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectPurchaseOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var builder0 = NormalizeRfqRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setRfq(Rfq.newBuilder()
                .setAssignee(UserRef.newBuilder()
                        .setDisplayName(request.getPurchaseOrder().getCounterparty().getDisplayName())
                        .build())
                .addComments(RfqComment.newBuilder()
                        .setCreatedAt(request.getPurchaseOrder().getAudit().getCreatedAt())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = procurementRfqValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeRfq(outbound0);

        var outbound1 = ScheduleFixedAssetExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScheduleFixedAssetExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(switch (reply0.getRfq().getOriginCase()) {
                                    case CUSTOMER -> reply0.getRfq().getCustomer().getEmail();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply0.getRfq().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
        var reply1 = assetsFixedAssetLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleFixedAssetExpiry(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectPurchaseOrder", request.getPurchaseOrder().getPurchaseOrderId());
        return RejectPurchaseOrderResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getRfq().getAudit().getCreatedBy())
                                .build())
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setDisplayName(reply1.getFixedAsset().getOwner().getDisplayName())
                                .build())
                        .setNotes(request.getPurchaseOrder().getNotes())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .build();
    }

    /**
     * Escalates a purchase order.
     */
    public EscalatePurchaseOrderResponse escalatePurchaseOrder(EscalatePurchaseOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalatePurchaseOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var outbound0 = ExportCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportCustomersRequest.Filter.newBuilder()
                        .setRegion(request.getPurchaseOrder().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream0 = customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportCustomers(outbound0);
        var reply0 = ExportCustomersResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = FindPurchaseRequisitionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindPurchaseRequisitionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPurchaseOrder().getDueAt())
                        .setRegion(request.getPurchaseOrder().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> procurementPurchaseRequisitionQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findPurchaseRequisitions(value));
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalatePurchaseOrder", request.getPurchaseOrder().getPurchaseOrderId());
        return EscalatePurchaseOrderResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .setStatus(request.getPurchaseOrder().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Lists purchase orders a page at a time.
     */
    public ListPendingPurchaseOrdersResponse listPendingPurchaseOrders(ListPendingPurchaseOrdersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingPurchaseOrders")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseOrder", request.getContext().getTraceId(), "ListPendingPurchaseOrders");

        var outbound0 = ValidatePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidatePreferenceRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        RpcPort<ValidatePreferenceRequest, ValidatePreferenceResponse> port0 = RpcPort.adapt(customersPreferenceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::validatePreference);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = JsonCodec.roundTrip(CloneNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply0.getPreference().getOwner().getUserId())
                                .build())
                        .build())
                .build(), CloneNotificationRequest.class);
        var reply1 = notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneNotification(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingPurchaseOrders", request.getPage().getPageToken());
        return ListPendingPurchaseOrdersResponse.newBuilder()
                .addPurchaseOrders(PurchaseOrder.newBuilder()
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setDisplayName(reply0.getPreference().getOwner().getDisplayName())
                                .build())
                        .setNumber(reply1.getNotification().getCustomer().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Recalls a purchase order.
     */
    public RecallPurchaseOrderResponse recallPurchaseOrder(RecallPurchaseOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallPurchaseOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var outbound0 = JsonCodec.roundTrip(QuotePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getPurchaseOrder().getCounterparty().getDisplayName())
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build(), QuotePreferenceRequest.class);
        var reply0 = customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePreference(outbound0);

        var outbound1 = SubmitCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setRoute(CoverageRouteAction.newBuilder()
                                .setWeight(reply0.getPreference().getRoute().getWeight())
                                .build())
                        .build())
                .setDetails(SubmitCoverageRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(reply0.getPreference().getOwner().getUserId())
                                .build())
                        .build())
                .build();
        Function<SubmitCoverageRequest, SubmitCoverageResponse> operation1 = insuranceCoverageApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::submitCoverage;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallPurchaseOrder", request.getPurchaseOrder().getPurchaseOrderId());
        return RecallPurchaseOrderResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getCoverage().getAudit().getEtag())
                                .build())
                        .setNotes(request.getPurchaseOrder().getNotes())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .setTotal(reply0.getBreakdown().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a purchase order.
     */
    public GetPurchaseOrderApprovalStatusResponse getPurchaseOrderApprovalStatus(GetPurchaseOrderApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPurchaseOrderApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseOrder", request.getPurchaseOrderId(), "GetPurchaseOrderApprovalStatus");
        return GetPurchaseOrderApprovalStatusResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .build();
    }

    /**
     * Comments a purchase order.
     */
    public CommentPurchaseOrderApprovalResponse commentPurchaseOrderApproval(CommentPurchaseOrderApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentPurchaseOrderApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var outbound0 = DelegateForecastApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getPurchaseOrder().getAudit().getEtag())
                                .setUpdatedBy(request.getPurchaseOrder().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        RpcPort<DelegateForecastApprovalRequest, DelegateForecastApprovalResponse> port0 = RpcPort.adapt(budgetingForecastApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::delegateForecastApproval);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = BeanCopies.copy(ExpirePurchaseOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExpirePurchaseOrderRequest.Details.newBuilder()
                        .setNewOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getForecast().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setPurchaseOrderId(request.getPurchaseOrder().getPurchaseOrderId())
                .build(), ExpirePurchaseOrderRequest.class);
        var reply1 = procurementPurchaseOrderLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).expirePurchaseOrder(outbound1);

        var outbound2 = StreamRfqMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamRfqMetricsRequest.Filter.newBuilder()
                        .setRegion(request.getPurchaseOrder().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream2 = procurementRfqReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamRfqMetrics(outbound2);
        var reply2 = StreamRfqMetricsResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentPurchaseOrderApproval", request.getPurchaseOrder().getNumber());
        return CommentPurchaseOrderApprovalResponse.newBuilder()
                .setOutcome(CommentPurchaseOrderApprovalResponse.Outcome.newBuilder()
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setBillingAddress(request.getPurchaseOrder().getBillingAddress())
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setNumber(reply0.getForecast().getNumber())
                                .build())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
