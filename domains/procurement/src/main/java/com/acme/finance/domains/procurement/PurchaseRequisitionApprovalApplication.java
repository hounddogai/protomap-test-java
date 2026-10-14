package com.acme.finance.domains.procurement;

import com.acme.appointments.v1.AppointmentQueryServiceGrpc;
import com.acme.appointments.v1.ResolveAppointmentsRequest;
import com.acme.appointments.v1.ResolveAppointmentsResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailBankSettlement;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.CompareAuditTrailVersionsRequest;
import com.acme.audit.v1.CompareAuditTrailVersionsResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.collections.v1.PaymentPromiseRef;
import com.acme.collections.v1.RetryCollectionCaseWorkflowRequest;
import com.acme.collections.v1.RetryCollectionCaseWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.PreviewCustomerRequest;
import com.acme.customers.v1.PreviewCustomerResponse;
import com.acme.customers.v1.QuoteCustomerSegmentRequest;
import com.acme.customers.v1.QuoteCustomerSegmentResponse;
import com.acme.customers.v1.RecalculateCustomersRequest;
import com.acme.customers.v1.RecalculateCustomersResponse;
import com.acme.documents.v1.BatchGetDocumentsRequest;
import com.acme.documents.v1.BatchGetDocumentsResponse;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.LintLedgerAccountsRequest;
import com.acme.ledger.v1.LintLedgerAccountsResponse;
import com.acme.ledger.v1.NormalizeLedgerAccountRequest;
import com.acme.ledger.v1.NormalizeLedgerAccountResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationTemplateRef;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.PreviewNotificationRequest;
import com.acme.notifications.v1.PreviewNotificationResponse;
import com.acme.procurement.v1.ApprovePurchaseRequisitionRequest;
import com.acme.procurement.v1.ApprovePurchaseRequisitionResponse;
import com.acme.procurement.v1.CommentPurchaseRequisitionApprovalRequest;
import com.acme.procurement.v1.CommentPurchaseRequisitionApprovalResponse;
import com.acme.procurement.v1.CountRfqTotalsRequest;
import com.acme.procurement.v1.CountRfqTotalsResponse;
import com.acme.procurement.v1.DelegatePurchaseRequisitionApprovalRequest;
import com.acme.procurement.v1.DelegatePurchaseRequisitionApprovalResponse;
import com.acme.procurement.v1.EscalatePurchaseRequisitionRequest;
import com.acme.procurement.v1.EscalatePurchaseRequisitionResponse;
import com.acme.procurement.v1.GenerateRfqReportRequest;
import com.acme.procurement.v1.GenerateRfqReportResponse;
import com.acme.procurement.v1.GetPurchaseRequisitionApprovalStatusRequest;
import com.acme.procurement.v1.GetPurchaseRequisitionApprovalStatusResponse;
import com.acme.procurement.v1.ListPendingPurchaseRequisitionsRequest;
import com.acme.procurement.v1.ListPendingPurchaseRequisitionsResponse;
import com.acme.procurement.v1.PurchaseOrderRef;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.RecallPurchaseRequisitionRequest;
import com.acme.procurement.v1.RecallPurchaseRequisitionResponse;
import com.acme.procurement.v1.RejectPurchaseRequisitionRequest;
import com.acme.procurement.v1.RejectPurchaseRequisitionResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqComment;
import com.acme.procurement.v1.RfqLifecycleServiceGrpc;
import com.acme.procurement.v1.RfqReportingServiceGrpc;
import com.acme.procurement.v1.ScheduleRfqExpiryRequest;
import com.acme.procurement.v1.ScheduleRfqExpiryResponse;
import com.acme.procurement.v1.SubmitPurchaseRequisitionRequest;
import com.acme.procurement.v1.SubmitPurchaseRequisitionResponse;
import com.acme.sellers.v1.Seller;
import com.acme.sellers.v1.SellerServiceGrpc;
import com.acme.sellers.v1.UpdateSellerRequest;
import com.acme.sellers.v1.UpdateSellerResponse;
import com.google.protobuf.Any;
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
 * Routes purchase requisitions through approval, with escalation and delegation. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class PurchaseRequisitionApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(PurchaseRequisitionApprovalApplication.class);

    private final AppointmentQueryServiceGrpc.AppointmentQueryServiceBlockingStub appointmentsAppointmentQueryServiceBlocking;

    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking;

    private final CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceBlockingStub collectionsCollectionCaseWorkflowServiceBlocking;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceFutureStub customersCustomerCalculationServiceFuture;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking;

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking;

    private final LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking;

    private final NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking;

    private final RfqLifecycleServiceGrpc.RfqLifecycleServiceBlockingStub procurementRfqLifecycleServiceBlocking;

    private final RfqReportingServiceGrpc.RfqReportingServiceBlockingStub procurementRfqReportingServiceBlocking;

    private final ProcurementRepository repository;

    private final Executor rpcExecutor;

    private final SellerServiceGrpc.SellerServiceBlockingStub sellersSellerServiceBlocking;

    public PurchaseRequisitionApprovalApplication(
            AppointmentQueryServiceGrpc.AppointmentQueryServiceBlockingStub appointmentsAppointmentQueryServiceBlocking,
            AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking,
            CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceBlockingStub collectionsCollectionCaseWorkflowServiceBlocking,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceFutureStub customersCustomerCalculationServiceFuture,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking,
            DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking,
            LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking,
            NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking,
            RfqLifecycleServiceGrpc.RfqLifecycleServiceBlockingStub procurementRfqLifecycleServiceBlocking,
            RfqReportingServiceGrpc.RfqReportingServiceBlockingStub procurementRfqReportingServiceBlocking,
            ProcurementRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SellerServiceGrpc.SellerServiceBlockingStub sellersSellerServiceBlocking) {
        this.appointmentsAppointmentQueryServiceBlocking = appointmentsAppointmentQueryServiceBlocking;
        this.auditAuditTrailHistoryServiceBlocking = auditAuditTrailHistoryServiceBlocking;
        this.collectionsCollectionCaseWorkflowServiceBlocking = collectionsCollectionCaseWorkflowServiceBlocking;
        this.customersCustomerCalculationServiceFuture = customersCustomerCalculationServiceFuture;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersCustomerValidationServiceBlocking = customersCustomerValidationServiceBlocking;
        this.documentsDocumentServiceBlocking = documentsDocumentServiceBlocking;
        this.ledgerLedgerAccountValidationServiceBlocking = ledgerLedgerAccountValidationServiceBlocking;
        this.notificationsNotificationValidationServiceBlocking = notificationsNotificationValidationServiceBlocking;
        this.procurementRfqLifecycleServiceBlocking = procurementRfqLifecycleServiceBlocking;
        this.procurementRfqReportingServiceBlocking = procurementRfqReportingServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.sellersSellerServiceBlocking = sellersSellerServiceBlocking;
    }

    /**
     * Submits a purchase requisition.
     */
    public SubmitPurchaseRequisitionResponse submitPurchaseRequisition(SubmitPurchaseRequisitionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitPurchaseRequisition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var outbound0 = PreviewCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setLocality(request.getPurchaseRequisition().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> customersCustomerValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewCustomer(outbound0), rpcExecutor);

        var outbound1 = BatchGetDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> documentsDocumentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetDocuments(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitPurchaseRequisition", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return SubmitPurchaseRequisitionResponse.newBuilder()
                .setOutcome(SubmitPurchaseRequisitionResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setCounterparty(PurchaseOrderRef.newBuilder()
                                .setDisplayName(reply0.getCustomer().getRelationshipManager().getDisplayName())
                                .build())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .setTotal(request.getPurchaseRequisition().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Approves a purchase requisition.
     */
    public ApprovePurchaseRequisitionResponse approvePurchaseRequisition(ApprovePurchaseRequisitionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApprovePurchaseRequisition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var outbound0 = RpcSupport.unpack(Any.pack(UpdateSellerRequest.newBuilder()
                .setContext(request.getContext())
                .setSeller(Seller.newBuilder()
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setLocality(request.getPurchaseRequisition().getBillingAddress().getLocality())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build()), UpdateSellerRequest.class);
        var reply0 = sellersSellerServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateSeller(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(CountRfqTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountRfqTotalsRequest.Filter.newBuilder()
                        .setRegion(reply0.getSeller().getPrimaryAddress().getRegion())
                        .build())
                .build()), CountRfqTotalsRequest.class);
        var reply1 = procurementRfqReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countRfqTotals(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApprovePurchaseRequisition", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return ApprovePurchaseRequisitionResponse.newBuilder()
                .setOutcome(ApprovePurchaseRequisitionResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getSeller().getPrimaryAddress().getLocality())
                                .build())
                        .setCounterparty(PurchaseOrderRef.newBuilder()
                                .setNumber(request.getPurchaseRequisition().getNumber())
                                .build())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Rejects a purchase requisition.
     */
    public RejectPurchaseRequisitionResponse rejectPurchaseRequisition(RejectPurchaseRequisitionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectPurchaseRequisition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var outbound0 = LintLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getPurchaseRequisition().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            LintLedgerAccountsResponse fetch(LintLedgerAccountsRequest input) {
                return ledgerLedgerAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintLedgerAccounts(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = RetryCollectionCaseWorkflowRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setCustomer(PaymentPromiseRef.newBuilder()
                                .setDisplayName(reply0.getLedgerAccount().getHolder().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        RpcPort<RetryCollectionCaseWorkflowRequest, RetryCollectionCaseWorkflowResponse> port1 = RpcPort.adapt(collectionsCollectionCaseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::retryCollectionCaseWorkflow);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = GenerateRfqReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(GenerateRfqReportRequest.Details.newBuilder()
                        .putAllFilters(reply0.getLedgerAccount().getLabelsMap())
                        .build())
                .setReason(request.getReason())
                .build();
        final class Stage2Adapter {
            GenerateRfqReportResponse fetch(GenerateRfqReportRequest input) {
                return procurementRfqReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generateRfqReport(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectPurchaseRequisition", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return RejectPurchaseRequisitionResponse.newBuilder()
                .setOutcome(RejectPurchaseRequisitionResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBillingAddress(request.getPurchaseRequisition().getBillingAddress())
                        .setCounterparty(PurchaseOrderRef.newBuilder()
                                .setDisplayName(reply1.getCollectionCase().getCustomer().getDisplayName())
                                .build())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Escalates a purchase requisition.
     */
    public EscalatePurchaseRequisitionResponse escalatePurchaseRequisition(EscalatePurchaseRequisitionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalatePurchaseRequisition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var outbound0 = RecalculateCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPurchaseRequisition().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var pending0 = customersCustomerCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateCustomers(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalatePurchaseRequisition", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return EscalatePurchaseRequisitionResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .putAllMetadata(request.getPurchaseRequisition().getMetadataMap())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Lists purchase requisitions a page at a time.
     */
    public ListPendingPurchaseRequisitionsResponse listPendingPurchaseRequisitions(ListPendingPurchaseRequisitionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingPurchaseRequisitions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseRequisition", request.getContext().getTraceId(), "ListPendingPurchaseRequisitions");

        var outbound0 = ResolveAppointmentsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> appointmentsAppointmentQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveAppointments(outbound0), rpcExecutor);

        var outbound1 = QuoteCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setDetails(QuoteCustomerSegmentRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCustomerSegment(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingPurchaseRequisitions", request.getPage().getPageToken());
        return ListPendingPurchaseRequisitionsResponse.newBuilder()
                .addPurchaseRequisitions(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getCustomerSegment().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Delegates a purchase requisition.
     */
    public DelegatePurchaseRequisitionApprovalResponse delegatePurchaseRequisitionApproval(DelegatePurchaseRequisitionApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegatePurchaseRequisitionApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var outbound0 = CompareAuditTrailVersionsRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getPurchaseRequisition().getAudit().getUpdatedBy())
                                .build())
                        .setBank(AuditTrailBankSettlement.newBuilder()
                                .setIban(switch (request.getPurchaseRequisition().getSettlementCase()) {
                                    case BANK -> request.getPurchaseRequisition().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getPurchaseRequisition().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareAuditTrailVersionsRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply0 = auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareAuditTrailVersions(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegatePurchaseRequisitionApproval", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return DelegatePurchaseRequisitionApprovalResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBillingAddress(request.getPurchaseRequisition().getBillingAddress())
                        .setIssuedAt(reply0.getAuditTrail().getIssuedAt())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .build();
    }

    /**
     * Recalls a purchase requisition.
     */
    public RecallPurchaseRequisitionResponse recallPurchaseRequisition(RecallPurchaseRequisitionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallPurchaseRequisition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var outbound0 = PreviewNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPurchaseRequisition().getAudit().getCreatedAt())
                                .setCreatedBy(request.getPurchaseRequisition().getAudit().getCreatedBy())
                                .build())
                        .setCustomer(NotificationTemplateRef.newBuilder()
                                .setDisplayName(request.getPurchaseRequisition().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(notificationsNotificationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewNotification(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallPurchaseRequisition", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return RecallPurchaseRequisitionResponse.newBuilder()
                .setOutcome(RecallPurchaseRequisitionResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getNotification().getAudit().getEtag())
                                .build())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .setStatus(request.getPurchaseRequisition().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a purchase requisition.
     */
    public GetPurchaseRequisitionApprovalStatusResponse getPurchaseRequisitionApprovalStatus(GetPurchaseRequisitionApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPurchaseRequisitionApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseRequisition", request.getPurchaseRequisitionId(), "GetPurchaseRequisitionApprovalStatus");

        var outbound0 = NormalizeLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeLedgerAccountRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(ledgerLedgerAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeLedgerAccount(value)))
                .collect(Collectors.toList()).getFirst();
        return GetPurchaseRequisitionApprovalStatusResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Comments a purchase requisition.
     */
    public CommentPurchaseRequisitionApprovalResponse commentPurchaseRequisitionApproval(CommentPurchaseRequisitionApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentPurchaseRequisitionApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var outbound0 = ScheduleRfqExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setRfq(Rfq.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getPurchaseRequisition().getAudit().getCreatedBy())
                                .setEtag(request.getPurchaseRequisition().getAudit().getEtag())
                                .build())
                        .addComments(RfqComment.newBuilder()
                                .setCreatedAt(request.getPurchaseRequisition().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<ScheduleRfqExpiryRequest, ScheduleRfqExpiryResponse> port0 = RpcPort.adapt(procurementRfqLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scheduleRfqExpiry);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentPurchaseRequisitionApproval", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return CommentPurchaseRequisitionApprovalResponse.newBuilder()
                .setOutcome(CommentPurchaseRequisitionApprovalResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setCounterparty(PurchaseOrderRef.newBuilder()
                                .setDisplayName(reply0.getRfq().getCustomer().getDisplayName())
                                .build())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .build();
    }
}
