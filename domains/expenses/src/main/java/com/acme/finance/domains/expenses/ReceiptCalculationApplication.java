package com.acme.finance.domains.expenses;

import com.acme.assets.v1.CompareAssetTransferOptionsRequest;
import com.acme.assets.v1.CompareAssetTransferOptionsResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.CheckAuditTrailEligibilityRequest;
import com.acme.audit.v1.CheckAuditTrailEligibilityResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.expenses.v1.CalculateExpenseReportRequest;
import com.acme.expenses.v1.CalculateExpenseReportResponse;
import com.acme.expenses.v1.CalculateReceiptRequest;
import com.acme.expenses.v1.CalculateReceiptResponse;
import com.acme.expenses.v1.CompareExpenseReportOptionsRequest;
import com.acme.expenses.v1.CompareExpenseReportOptionsResponse;
import com.acme.expenses.v1.CompareReceiptOptionsRequest;
import com.acme.expenses.v1.CompareReceiptOptionsResponse;
import com.acme.expenses.v1.EstimateReceiptRequest;
import com.acme.expenses.v1.EstimateReceiptResponse;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ExpenseReportCardSettlement;
import com.acme.expenses.v1.ExplainReceiptCalculationRequest;
import com.acme.expenses.v1.ExplainReceiptCalculationResponse;
import com.acme.expenses.v1.GetReceiptRatesRequest;
import com.acme.expenses.v1.GetReceiptRatesResponse;
import com.acme.expenses.v1.MileageClaimRef;
import com.acme.expenses.v1.QuoteExpenseReportRequest;
import com.acme.expenses.v1.QuoteExpenseReportResponse;
import com.acme.expenses.v1.RecalculateExpenseReportsRequest;
import com.acme.expenses.v1.RecalculateExpenseReportsResponse;
import com.acme.expenses.v1.RecalculateReceiptsRequest;
import com.acme.expenses.v1.RecalculateReceiptsResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.SimulateReceiptRequest;
import com.acme.expenses.v1.SimulateReceiptResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.PreviewCollateralRequest;
import com.acme.lending.v1.PreviewCollateralResponse;
import com.acme.lending.v1.SimulateCollateralRequest;
import com.acme.lending.v1.SimulateCollateralResponse;
import com.acme.notifications.v1.CloneNotificationRequest;
import com.acme.notifications.v1.CloneNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.notifications.v1.NotificationTemplateRef;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsRequest;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsResponse;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.acme.suppliers.v1.BrowseSuppliersRequest;
import com.acme.suppliers.v1.BrowseSuppliersResponse;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.webhooks.v1.SearchWebhookEndpointsRequest;
import com.acme.webhooks.v1.SearchWebhookEndpointsResponse;
import com.acme.webhooks.v1.WebhookEndpointServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a receipt, such as totals, taxes, and estimates. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ReceiptCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(ReceiptCalculationApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceBlockingStub expensesExpenseReportCalculationServiceBlocking;

    private final JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking;

    private final CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking;

    private final CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking;

    private final NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking;

    private final PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceStub procurementPurchaseRequisitionServiceAsync;

    private final ExpensesRepository repository;

    private final SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking;

    private final WebhookEndpointServiceGrpc.WebhookEndpointServiceBlockingStub webhooksWebhookEndpointServiceBlocking;

    public ReceiptCalculationApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceBlockingStub expensesExpenseReportCalculationServiceBlocking,
            JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking,
            CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking,
            CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking,
            NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking,
            PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceStub procurementPurchaseRequisitionServiceAsync,
            ExpensesRepository repository,
            SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking,
            WebhookEndpointServiceGrpc.WebhookEndpointServiceBlockingStub webhooksWebhookEndpointServiceBlocking) {
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.channels = channels;
        this.events = events;
        this.expensesExpenseReportCalculationServiceBlocking = expensesExpenseReportCalculationServiceBlocking;
        this.ledgerJournalEntryNotificationServiceBlocking = ledgerJournalEntryNotificationServiceBlocking;
        this.lendingCollateralCalculationServiceBlocking = lendingCollateralCalculationServiceBlocking;
        this.lendingCollateralValidationServiceBlocking = lendingCollateralValidationServiceBlocking;
        this.notificationsNotificationServiceBlocking = notificationsNotificationServiceBlocking;
        this.procurementPurchaseRequisitionServiceAsync = procurementPurchaseRequisitionServiceAsync;
        this.repository = repository;
        this.suppliersSupplierQueryServiceBlocking = suppliersSupplierQueryServiceBlocking;
        this.webhooksWebhookEndpointServiceBlocking = webhooksWebhookEndpointServiceBlocking;
    }

    public record ExplainReceiptCalculationStage1Requested(CheckAuditTrailEligibilityRequest request, CompletableFuture<CheckAuditTrailEligibilityResponse> completion, io.grpc.Context context) {}

    /**
     * Calculates a receipt.
     */
    public CalculateReceiptResponse calculateReceipt(CalculateReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var outbound0 = BrowseSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseSuppliersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getReceipt().getIssuedAt())
                        .putAllLabels(request.getReceipt().getMetadataMap())
                        .setOwnerId(request.getReceipt().getCounterparty().getMileageClaimId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var adapter0 = new Function<BrowseSuppliersRequest, BrowseSuppliersResponse>() {
            @Override
            public BrowseSuppliersResponse apply(BrowseSuppliersRequest input) {
                return suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseSuppliers(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = SubscribeJournalEntryUpdatesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SubscribeJournalEntryUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .build())
                .build();
        var reply1 = ledgerJournalEntryNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeJournalEntryUpdates(outbound1);

        var outbound2 = BatchCreatePurchaseRequisitionsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreatePurchaseRequisitionsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
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

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getReceipt().getAudit().getCreatedAt())
                                .setCreatedBy(request.getReceipt().getAudit().getCreatedBy())
                                .setEtag(request.getReceipt().getAudit().getEtag())
                                .setUpdatedAt(request.getReceipt().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateReceipt", request.getReceipt().getReceiptId());
        return CalculateReceiptResponse.newBuilder()
                .setBreakdown(CalculateReceiptResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .setTotal(reply1.getJournalEntry().getTotal())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setReceiptId(request.getReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Estimates a receipt.
     */
    public EstimateReceiptResponse estimateReceipt(EstimateReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var outbound0 = BeanCopies.copy(SearchWebhookEndpointsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchWebhookEndpointsRequest.Filter.newBuilder()
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build(), SearchWebhookEndpointsRequest.class);
        var reply0 = webhooksWebhookEndpointServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchWebhookEndpoints(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateReceipt", request.getReceipt().getReceiptId());
        return EstimateReceiptResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .setReceiptId(request.getReceiptId())
                        .setTotal(request.getReceipt().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Compares a receipt.
     */
    public CompareReceiptOptionsResponse compareReceiptOptions(CompareReceiptOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareReceiptOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var outbound0 = CompareAssetTransferOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareAssetTransferOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.assets.v1.AssetTransferCalculationService");
        var serviceName0 = "acme.assets.v1" + "." + "AssetTransferCalculationService";
        var methodName0 = "Com" + "pareAssetTransferOptions";
        var descriptor0 = MethodDescriptor.<CompareAssetTransferOptionsRequest, CompareAssetTransferOptionsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(CompareAssetTransferOptionsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CompareAssetTransferOptionsResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = (QuoteExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuoteExpenseReportRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = expensesExpenseReportCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteExpenseReport(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareReceiptOptions", request.getReceipt().getReceiptId());
        return CompareReceiptOptionsResponse.newBuilder()
                .setBreakdown(CompareReceiptOptionsResponse.Breakdown.newBuilder()
                        .setTotal(request.getReceipt().getTotal())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setBillingAddress(reply1.getExpenseReport().getBillingAddress())
                        .putAllMetadata(reply0.getAssetTransfer().getMetadataMap())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .build();
    }

    /**
     * Recalculates a receipt.
     */
    public RecalculateReceiptsResponse recalculateReceipts(RecalculateReceiptsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateReceipts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var outbound0 = JsonCodec.roundTrip(CloneNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getReceipt().getCounterparty().getDisplayName())
                                .build())
                        .setCustomer(NotificationTemplateRef.newBuilder()
                                .setNumber(request.getReceipt().getCounterparty().getNumber())
                                .build())
                        .build())
                .build(), CloneNotificationRequest.class);
        var reply0 = notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneNotification(outbound0);

        var outbound1 = CompareExpenseReportOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .build())
                .build();
        var reply1 = expensesExpenseReportCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareExpenseReportOptions(outbound1);

        var outbound2 = PreviewCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply1.getExpenseReport().getBillingAddress().getPostalCode())
                                .setRecipient(request.getReceipt().getBillingAddress().getRecipient())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getNotification().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getNotification().getRequester().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = lendingCollateralValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewCollateral(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateReceipts", request.getReceipt().getReceiptId());
        return RecalculateReceiptsResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getNotification().getAudit().getCreatedBy())
                                .setEtag(reply0.getNotification().getAudit().getEtag())
                                .setUpdatedBy(reply1.getExpenseReport().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply2.getCollateral().getAddress().getCountryCode())
                                .build())
                        .setReceiptId(request.getReceiptId())
                        .setTotal(request.getReceipt().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Simulates a receipt.
     */
    public SimulateReceiptResponse simulateReceipt(SimulateReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var outbound0 = SimulateCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getReceipt().getBillingAddress().getLinesList())
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(SimulateCollateralRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getReceipt().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<SimulateCollateralRequest, SimulateCollateralResponse>() {
            @Override
            public SimulateCollateralResponse apply(SimulateCollateralRequest input) {
                return lendingCollateralCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateCollateral(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = RecalculateExpenseReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateExpenseReportsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .build())
                .build();
        RpcPort<RecalculateExpenseReportsRequest, RecalculateExpenseReportsResponse> port1 = RpcPort.adapt(expensesExpenseReportCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::recalculateExpenseReports);
        var reply1 = port1.exchange(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateReceipt", request.getReceipt().getReceiptId());
        return SimulateReceiptResponse.newBuilder()
                .setBreakdown(SimulateReceiptResponse.Breakdown.newBuilder()
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setCounterparty(MileageClaimRef.newBuilder()
                                .setNumber(request.getReceipt().getNumber())
                                .build())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Explains a receipt.
     */
    public ExplainReceiptCalculationResponse explainReceiptCalculation(ExplainReceiptCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainReceiptCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var outbound0 = CalculateExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getReceipt().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getReceipt().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getReceipt().getBillingAddress().getLocality())
                                .build())
                        .setCard(ExpenseReportCardSettlement.newBuilder()
                                .setNetwork(switch (request.getReceipt().getSettlementCase()) {
                                    case CARD -> request.getReceipt().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = expensesExpenseReportCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateExpenseReport(outbound0);

        var outbound1 = CheckAuditTrailEligibilityRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getExpenseReport().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getExpenseReport().getBillingAddress().getCountryCode())
                                .setLocality(reply0.getExpenseReport().getBillingAddress().getLocality())
                                .build())
                        .setCard(AuditTrailCardSettlement.newBuilder()
                                .setToken(reply0.getExpenseReport().getCard().getToken())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var completion1 = new CompletableFuture<CheckAuditTrailEligibilityResponse>();
        events.publishEvent(new ExplainReceiptCalculationStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainReceiptCalculation", request.getReceipt().getReceiptId());
        return ExplainReceiptCalculationResponse.newBuilder()
                .setOutcome(ExplainReceiptCalculationResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setDueAt(request.getReceipt().getDueAt())
                        .setReceiptId(request.getReceiptId())
                        .setTotal(reply0.getExpenseReport().getTotal())
                        .build())
                .build();
    }

    /**
     * Returns a receipt.
     */
    public GetReceiptRatesResponse getReceiptRates(GetReceiptRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetReceiptRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Receipt", request.getReceiptId(), "GetReceiptRates");

        var outbound0 = RecalculateExpenseReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = expensesExpenseReportCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateExpenseReports(outbound0);
        return GetReceiptRatesResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getExpenseReport().getAudit().getUpdatedAt())
                                .build())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .build();
    }
}
