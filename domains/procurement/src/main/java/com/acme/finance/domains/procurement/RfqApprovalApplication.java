package com.acme.finance.domains.procurement;

import com.acme.budgeting.v1.CancelForecastWorkflowRequest;
import com.acme.budgeting.v1.CancelForecastWorkflowResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationWorkflowServiceGrpc;
import com.acme.compliance.v1.CancelAttestationWorkflowRequest;
import com.acme.compliance.v1.CancelAttestationWorkflowResponse;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlApprovalServiceGrpc;
import com.acme.compliance.v1.SubmitControlRequest;
import com.acme.compliance.v1.SubmitControlResponse;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityRequest;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationRequest;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationResponse;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.RecalculateConsentRecordsRequest;
import com.acme.customers.v1.RecalculateConsentRecordsResponse;
import com.acme.customers.v1.RecalculateCustomersRequest;
import com.acme.customers.v1.RecalculateCustomersResponse;
import com.acme.devices.v1.ManagedDevice;
import com.acme.devices.v1.ManagedDeviceCalculationServiceGrpc;
import com.acme.devices.v1.SimulateManagedDeviceRequest;
import com.acme.devices.v1.SimulateManagedDeviceResponse;
import com.acme.documents.v1.ListDocumentsRequest;
import com.acme.documents.v1.ListDocumentsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.forecasting.v1.DemandForecastQueryServiceGrpc;
import com.acme.forecasting.v1.ResolveDemandForecastsRequest;
import com.acme.forecasting.v1.ResolveDemandForecastsResponse;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.ListLedgerAccountSubscriptionsRequest;
import com.acme.ledger.v1.ListLedgerAccountSubscriptionsResponse;
import com.acme.lending.v1.GenerateRepaymentReportRequest;
import com.acme.lending.v1.GenerateRepaymentReportResponse;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.metrics.v1.BrowseMetricSeriesesRequest;
import com.acme.metrics.v1.BrowseMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.notifications.v1.ListNotificationsRequest;
import com.acme.notifications.v1.ListNotificationsResponse;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.procurement.v1.ApproveRfqRequest;
import com.acme.procurement.v1.ApproveRfqResponse;
import com.acme.procurement.v1.CommentPurchaseOrderApprovalRequest;
import com.acme.procurement.v1.CommentPurchaseOrderApprovalResponse;
import com.acme.procurement.v1.CommentRfqApprovalRequest;
import com.acme.procurement.v1.CommentRfqApprovalResponse;
import com.acme.procurement.v1.DelegateRfqApprovalRequest;
import com.acme.procurement.v1.DelegateRfqApprovalResponse;
import com.acme.procurement.v1.EscalateGoodsReceiptRequest;
import com.acme.procurement.v1.EscalateGoodsReceiptResponse;
import com.acme.procurement.v1.EscalateRfqRequest;
import com.acme.procurement.v1.EscalateRfqResponse;
import com.acme.procurement.v1.GetRfqApprovalStatusRequest;
import com.acme.procurement.v1.GetRfqApprovalStatusResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptApprovalServiceGrpc;
import com.acme.procurement.v1.ListPendingRfqsRequest;
import com.acme.procurement.v1.ListPendingRfqsResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderApprovalServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderLine;
import com.acme.procurement.v1.PurchaseOrderLineTax;
import com.acme.procurement.v1.PurchaseOrderRef;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionWorkflowServiceGrpc;
import com.acme.procurement.v1.QuotePurchaseOrderRequest;
import com.acme.procurement.v1.QuotePurchaseOrderResponse;
import com.acme.procurement.v1.RecallRfqRequest;
import com.acme.procurement.v1.RecallRfqResponse;
import com.acme.procurement.v1.RejectRfqRequest;
import com.acme.procurement.v1.RejectRfqResponse;
import com.acme.procurement.v1.RetryPurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.RetryPurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqComment;
import com.acme.procurement.v1.ScorePurchaseRequisitionRequest;
import com.acme.procurement.v1.ScorePurchaseRequisitionResponse;
import com.acme.procurement.v1.SubmitRfqRequest;
import com.acme.procurement.v1.SubmitRfqResponse;
import com.acme.suppliers.v1.SubmitSupplierRiskRequest;
import com.acme.suppliers.v1.SubmitSupplierRiskResponse;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskApprovalServiceGrpc;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Routes rfqs through approval, with escalation and delegation. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class RfqApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(RfqApprovalApplication.class);

    private final ForecastWorkflowServiceGrpc.ForecastWorkflowServiceBlockingStub budgetingForecastWorkflowServiceBlocking;

    private final ChannelRegistry channels;

    private final AttestationWorkflowServiceGrpc.AttestationWorkflowServiceBlockingStub complianceAttestationWorkflowServiceBlocking;

    private final ControlApprovalServiceGrpc.ControlApprovalServiceBlockingStub complianceControlApprovalServiceBlocking;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking;

    private final ManagedDeviceCalculationServiceGrpc.ManagedDeviceCalculationServiceBlockingStub devicesManagedDeviceCalculationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final DemandForecastQueryServiceGrpc.DemandForecastQueryServiceBlockingStub forecastingDemandForecastQueryServiceBlocking;

    private final RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking;

    private final RfqApprovalRequestMapper mapper;

    private final MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking;

    private final NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking;

    private final GoodsReceiptApprovalServiceGrpc.GoodsReceiptApprovalServiceBlockingStub procurementGoodsReceiptApprovalServiceBlocking;

    private final PurchaseOrderApprovalServiceGrpc.PurchaseOrderApprovalServiceBlockingStub procurementPurchaseOrderApprovalServiceBlocking;

    private final PurchaseOrderCalculationServiceGrpc.PurchaseOrderCalculationServiceBlockingStub procurementPurchaseOrderCalculationServiceBlocking;

    private final PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking;

    private final PurchaseRequisitionWorkflowServiceGrpc.PurchaseRequisitionWorkflowServiceBlockingStub procurementPurchaseRequisitionWorkflowServiceBlocking;

    private final ProcurementRepository repository;

    private final Executor rpcExecutor;

    private final SupplierRiskApprovalServiceGrpc.SupplierRiskApprovalServiceBlockingStub suppliersSupplierRiskApprovalServiceBlocking;

    public RfqApprovalApplication(
            ForecastWorkflowServiceGrpc.ForecastWorkflowServiceBlockingStub budgetingForecastWorkflowServiceBlocking,
            ChannelRegistry channels,
            AttestationWorkflowServiceGrpc.AttestationWorkflowServiceBlockingStub complianceAttestationWorkflowServiceBlocking,
            ControlApprovalServiceGrpc.ControlApprovalServiceBlockingStub complianceControlApprovalServiceBlocking,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking,
            ManagedDeviceCalculationServiceGrpc.ManagedDeviceCalculationServiceBlockingStub devicesManagedDeviceCalculationServiceBlocking,
            ApplicationEventPublisher events,
            DemandForecastQueryServiceGrpc.DemandForecastQueryServiceBlockingStub forecastingDemandForecastQueryServiceBlocking,
            RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking,
            RfqApprovalRequestMapper mapper,
            MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking,
            NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking,
            GoodsReceiptApprovalServiceGrpc.GoodsReceiptApprovalServiceBlockingStub procurementGoodsReceiptApprovalServiceBlocking,
            PurchaseOrderApprovalServiceGrpc.PurchaseOrderApprovalServiceBlockingStub procurementPurchaseOrderApprovalServiceBlocking,
            PurchaseOrderCalculationServiceGrpc.PurchaseOrderCalculationServiceBlockingStub procurementPurchaseOrderCalculationServiceBlocking,
            PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking,
            PurchaseRequisitionWorkflowServiceGrpc.PurchaseRequisitionWorkflowServiceBlockingStub procurementPurchaseRequisitionWorkflowServiceBlocking,
            ProcurementRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierRiskApprovalServiceGrpc.SupplierRiskApprovalServiceBlockingStub suppliersSupplierRiskApprovalServiceBlocking) {
        this.budgetingForecastWorkflowServiceBlocking = budgetingForecastWorkflowServiceBlocking;
        this.channels = channels;
        this.complianceAttestationWorkflowServiceBlocking = complianceAttestationWorkflowServiceBlocking;
        this.complianceControlApprovalServiceBlocking = complianceControlApprovalServiceBlocking;
        this.customersCustomerCalculationServiceBlocking = customersCustomerCalculationServiceBlocking;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersCustomerSegmentValidationServiceBlocking = customersCustomerSegmentValidationServiceBlocking;
        this.devicesManagedDeviceCalculationServiceBlocking = devicesManagedDeviceCalculationServiceBlocking;
        this.events = events;
        this.forecastingDemandForecastQueryServiceBlocking = forecastingDemandForecastQueryServiceBlocking;
        this.lendingRepaymentReportingServiceBlocking = lendingRepaymentReportingServiceBlocking;
        this.mapper = mapper;
        this.metricsMetricSeriesQueryServiceBlocking = metricsMetricSeriesQueryServiceBlocking;
        this.notificationsNotificationServiceBlocking = notificationsNotificationServiceBlocking;
        this.procurementGoodsReceiptApprovalServiceBlocking = procurementGoodsReceiptApprovalServiceBlocking;
        this.procurementPurchaseOrderApprovalServiceBlocking = procurementPurchaseOrderApprovalServiceBlocking;
        this.procurementPurchaseOrderCalculationServiceBlocking = procurementPurchaseOrderCalculationServiceBlocking;
        this.procurementPurchaseRequisitionValidationServiceBlocking = procurementPurchaseRequisitionValidationServiceBlocking;
        this.procurementPurchaseRequisitionWorkflowServiceBlocking = procurementPurchaseRequisitionWorkflowServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierRiskApprovalServiceBlocking = suppliersSupplierRiskApprovalServiceBlocking;
    }

    public record ApproveRfqStage0Requested(CancelForecastWorkflowRequest request, CompletableFuture<CancelForecastWorkflowResponse> completion, io.grpc.Context context) {}

    public record RejectRfqStage2Requested(ListLedgerAccountSubscriptionsRequest request, CompletableFuture<ListLedgerAccountSubscriptionsResponse> completion, io.grpc.Context context) {}

    /**
     * Submits a rfq.
     */
    public SubmitRfqResponse submitRfq(SubmitRfqRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitRfq")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRfq(request.getContext().getTenantId(), request.getRfq());

        var outbound0 = ResolveDemandForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        RpcPort<ResolveDemandForecastsRequest, ResolveDemandForecastsResponse> port0 = RpcPort.adapt(forecastingDemandForecastQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::resolveDemandForecasts);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = mapper.submitRfqStage1(request);
        var reply1 = devicesManagedDeviceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateManagedDevice(outbound1);

        var futures2 = request.getRfq().getCommentsList().stream()
                .map(element -> RecalculateCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateCustomers(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? RecalculateCustomersResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getRfq().getRfqId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitRfq", request.getRfq().getRfqId());
        return SubmitRfqResponse.newBuilder()
                .setOutcome(SubmitRfqResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply1.getManagedDevice().getOwner().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getManagedDevice().getAudit().getCreatedAt())
                                .build())
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }

    /**
     * Approves a rfq.
     */
    public ApproveRfqResponse approveRfq(ApproveRfqRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveRfq")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRfq(request.getContext().getTenantId(), request.getRfq());

        var outbound0 = CancelForecastWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CancelForecastWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getRfq().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<CancelForecastWorkflowResponse>();
        events.publishEvent(new ApproveRfqStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getRfq().getRfqId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveRfq", request.getRfq().getRfqId());
        return ApproveRfqResponse.newBuilder()
                .setOutcome(ApproveRfqResponse.Outcome.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getForecast().getAudit().getUpdatedAt())
                                .build())
                        .setRfqId(request.getRfqId())
                        .setStatus(request.getRfq().getStatus())
                        .build())
                .build();
    }

    /**
     * Rejects a rfq.
     */
    public RejectRfqResponse rejectRfq(RejectRfqRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectRfq")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRfq(request.getContext().getTenantId(), request.getRfq());

        var outbound0 = JsonCodec.roundTrip(ListNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListNotificationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getRfq().getSla().getDueAt())
                        .build())
                .build(), ListNotificationsRequest.class);
        var reply0 = notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listNotifications(outbound0);

        var results1 = new ArrayList<CancelAttestationWorkflowResponse>();
        for (var element : request.getRfq().getCommentsList()) {
            CancelAttestationWorkflowRequest itemRequest = CancelAttestationWorkflowRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CancelAttestationWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .setPreferredChannel(request.getRfq().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
            results1.add(complianceAttestationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelAttestationWorkflow(itemRequest));
        }
        var reply1 = results1.isEmpty() ? CancelAttestationWorkflowResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = ListLedgerAccountSubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListLedgerAccountSubscriptionsRequest.Filter.newBuilder()
                        .setRegion(reply1.getAttestation().getBillingAddress().getRegion())
                        .build())
                .build();
        var completion2 = new CompletableFuture<ListLedgerAccountSubscriptionsResponse>();
        events.publishEvent(new RejectRfqStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound3 = CancelForecastWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CancelForecastWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setName(request.getRfq().getRequester().getName())
                                .build())
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getRfq().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply1.getAttestation().getBillingAddress().getRecipient())
                                .setRegion(reply1.getAttestation().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        final class Stage3Adapter {
            CancelForecastWorkflowResponse fetch(CancelForecastWorkflowRequest input) {
                return budgetingForecastWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelForecastWorkflow(input);
            }
        }
        var reply3 = new Stage3Adapter().fetch(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getRfq().getRfqId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectRfq", request.getRfq().getRfqId());
        return RejectRfqResponse.newBuilder()
                .setOutcome(RejectRfqResponse.Outcome.newBuilder()
                        .setMessage(reply3.getOutcome().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply3.getOutcome().getStatus())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .putAllCustomFields(request.getRfq().getCustomFieldsMap())
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }

    /**
     * Escalates a rfq.
     */
    public EscalateRfqResponse escalateRfq(EscalateRfqRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateRfq")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRfq(request.getContext().getTenantId(), request.getRfq());

        var outbound0 = ExplainCustomerSegmentCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getRfq().getAudit().getEtag())
                                .setUpdatedAt(request.getRfq().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .setDetails(ExplainCustomerSegmentCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getRfq().getRequester().getPhone())
                                .build())
                        .build())
                .build();
        var reply0 = customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCustomerSegmentCalculation(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getRfq().getRfqId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateRfq", request.getRfq().getRfqId());
        return EscalateRfqResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setRfqId(request.getRfqId())
                        .setSubject(request.getRfq().getSubject())
                        .build())
                .build();
    }

    /**
     * Lists rfqs a page at a time.
     */
    public ListPendingRfqsResponse listPendingRfqs(ListPendingRfqsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingRfqs")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Rfq", request.getContext().getTraceId(), "ListPendingRfqs");

        var outbound0 = EscalateGoodsReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EscalateGoodsReceiptRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.procurement.v1.GoodsReceiptApprovalService/EscalateGoodsReceipt", outbound0);
        var reply0 = procurementGoodsReceiptApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateGoodsReceipt(command0.payload());
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingRfqs", request.getPage().getPageToken());
        return ListPendingRfqsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Delegates a rfq.
     */
    public DelegateRfqApprovalResponse delegateRfqApproval(DelegateRfqApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateRfqApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRfq(request.getContext().getTenantId(), request.getRfq());

        var outbound0 = (ScorePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getRfq().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getRfq().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getRfq().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(PurchaseOrderRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = procurementPurchaseRequisitionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scorePurchaseRequisition(outbound0);

        var builder1 = CheckCustomerSegmentEligibilityRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setCustomerSegment(CustomerSegment.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedAt(reply0.getPurchaseRequisition().getAudit().getUpdatedAt())
                        .build())
                .setOwner(UserRef.newBuilder()
                        .setDisplayName(reply0.getPurchaseRequisition().getCounterparty().getDisplayName())
                        .setUserId(request.getDetails().getApprover().getUserId())
                        .build())
                .build());
        builder1.setDetails(CheckCustomerSegmentEligibilityRequest.Details.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setEmail(request.getRfq().getAssignee().getEmail())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = customersCustomerSegmentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkCustomerSegmentEligibility(outbound1);

        var outbound2 = RecalculateConsentRecordsRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(HouseholdRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RecalculateConsentRecordsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(reply0.getPurchaseRequisition().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.customers.v1.ConsentRecordCalculationService");
        var serviceName2 = "acme.customers.v1" + "." + "ConsentRecordCalculationService";
        var methodName2 = "Rec" + "alculateConsentRecords";
        var descriptor2 = MethodDescriptor.<RecalculateConsentRecordsRequest, RecalculateConsentRecordsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(RecalculateConsentRecordsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(RecalculateConsentRecordsResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        List<QuotePurchaseOrderResponse> results3 = request.getRfq().getCommentsList().stream()
                .map(element -> QuotePurchaseOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .setUpdatedBy(reply0.getPurchaseRequisition().getAudit().getUpdatedBy())
                                .build())
                        .addLines(PurchaseOrderLine.newBuilder()
                                .setTax(PurchaseOrderLineTax.newBuilder()
                                        .setAmount(reply1.getCustomerSegment().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(procurementPurchaseOrderCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePurchaseOrder(value)))
                .collect(Collectors.toList());
        var reply3 = results3.isEmpty() ? QuotePurchaseOrderResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getRfq().getRfqId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateRfqApproval", request.getRfq().getRfqId());
        return DelegateRfqApprovalResponse.newBuilder()
                .setOutcome(DelegateRfqApprovalResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setPriority(request.getRfq().getPriority())
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }

    /**
     * Recalls a rfq.
     */
    public RecallRfqResponse recallRfq(RecallRfqRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallRfq")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRfq(request.getContext().getTenantId(), request.getRfq());

        var outbound0 = CommentPurchaseOrderApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getRfq().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setDisplayName(switch (request.getRfq().getOriginCase()) {
                                    case CUSTOMER -> request.getRfq().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = procurementPurchaseOrderApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentPurchaseOrderApproval(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getRfq().getRfqId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallRfq", request.getRfq().getRfqId());
        return RecallRfqResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setRfqId(request.getRfqId())
                        .setSubject(request.getRfq().getSubject())
                        .build())
                .build();
    }

    /**
     * Returns a rfq.
     */
    public GetRfqApprovalStatusResponse getRfqApprovalStatus(GetRfqApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetRfqApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Rfq", request.getRfqId(), "GetRfqApprovalStatus");

        var outbound0 = ListDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListDocumentsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.documents.v1.DocumentService");
        var serviceName0 = "acme.documents.v1" + "." + "DocumentService";
        var methodName0 = "Lis" + "tDocuments";
        var descriptor0 = MethodDescriptor.<ListDocumentsRequest, ListDocumentsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ListDocumentsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ListDocumentsResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = RetryPurchaseRequisitionWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RetryPurchaseRequisitionWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> procurementPurchaseRequisitionWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryPurchaseRequisitionWorkflow(value)).orElseThrow();
        return GetRfqApprovalStatusResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .addComments(RfqComment.newBuilder()
                                .setCreatedAt(reply1.getPurchaseRequisition().getAudit().getCreatedAt())
                                .build())
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }

    /**
     * Comments a rfq.
     */
    public CommentRfqApprovalResponse commentRfqApproval(CommentRfqApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentRfqApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRfq(request.getContext().getTenantId(), request.getRfq());

        var outbound0 = SubmitControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getRfq().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(switch (request.getRfq().getOriginCase()) {
                                    case CUSTOMER -> request.getRfq().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .setEmail(request.getRfq().getRequester().getEmail())
                                .build())
                        .build())
                .setDetails(SubmitControlRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getRfq().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = complianceControlApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitControl(outbound0);

        var outbound1 = SubmitSupplierRiskRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitSupplierRiskRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .setUserId(reply0.getControl().getOwner().getUserId())
                                .build())
                        .build())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(switch (request.getRfq().getOriginCase()) {
                                    case CUSTOMER -> request.getRfq().getCustomer().getEmail();
                                    default -> "";
                                })
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getControl().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var reply1 = suppliersSupplierRiskApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitSupplierRisk(outbound1);

        var outbound2 = BrowseMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseMetricSeriesesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getControl().getAudit().getUpdatedAt())
                        .build())
                .build();
        var command2 = new DispatchCommand<>("acme.metrics.v1.MetricSeriesQueryService/BrowseMetricSerieses", outbound2);
        var reply2 = metricsMetricSeriesQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseMetricSerieses(command2.payload());

        var outbound3 = GenerateRepaymentReportRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        Function<GenerateRepaymentReportRequest, GenerateRepaymentReportResponse> operation3 = lendingRepaymentReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::generateRepaymentReport;
        var reply3 = operation3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getRfq().getRfqId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentRfqApproval", request.getRfq().getRfqId());
        return CommentRfqApprovalResponse.newBuilder()
                .setOutcome(CommentRfqApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply3.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply1.getSupplierRisk().getAssignee().getUserId())
                                .build())
                        .setPriority(request.getRfq().getPriority())
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }
}
