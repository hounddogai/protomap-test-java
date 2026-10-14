package com.acme.finance.domains.procurement;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.CompareAuditTrailVersionsRequest;
import com.acme.audit.v1.CompareAuditTrailVersionsResponse;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.CancelControlWorkflowRequest;
import com.acme.compliance.v1.CancelControlWorkflowResponse;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.RejectSanctionRequest;
import com.acme.compliance.v1.RejectSanctionResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.customers.v1.CalculateCustomerRequest;
import com.acme.customers.v1.CalculateCustomerResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.PreviewPreferenceRequest;
import com.acme.customers.v1.PreviewPreferenceResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ListPendingPoliciesRequest;
import com.acme.insurance.v1.ListPendingPoliciesResponse;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.metrics.v1.ScoreMetricSeriesRequest;
import com.acme.metrics.v1.ScoreMetricSeriesResponse;
import com.acme.procurement.v1.ActivateGoodsReceiptRequest;
import com.acme.procurement.v1.ActivateGoodsReceiptResponse;
import com.acme.procurement.v1.CheckGoodsReceiptEligibilityRequest;
import com.acme.procurement.v1.CheckGoodsReceiptEligibilityResponse;
import com.acme.procurement.v1.CompareRfqOptionsRequest;
import com.acme.procurement.v1.CompareRfqOptionsResponse;
import com.acme.procurement.v1.ExpireGoodsReceiptRequest;
import com.acme.procurement.v1.ExpireGoodsReceiptResponse;
import com.acme.procurement.v1.GetGoodsReceiptLifecycleRequest;
import com.acme.procurement.v1.GetGoodsReceiptLifecycleResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptLine;
import com.acme.procurement.v1.GoodsReceiptLineTax;
import com.acme.procurement.v1.GoodsReceiptValidationServiceGrpc;
import com.acme.procurement.v1.ListPurchaseRequisitionsRequest;
import com.acme.procurement.v1.ListPurchaseRequisitionsResponse;
import com.acme.procurement.v1.ListRfqReportsRequest;
import com.acme.procurement.v1.ListRfqReportsResponse;
import com.acme.procurement.v1.LookupPurchaseRequisitionRequest;
import com.acme.procurement.v1.LookupPurchaseRequisitionResponse;
import com.acme.procurement.v1.PreviewPurchaseOrderRequest;
import com.acme.procurement.v1.PreviewPurchaseOrderResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderValidationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionQueryServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.acme.procurement.v1.ReinstateGoodsReceiptRequest;
import com.acme.procurement.v1.ReinstateGoodsReceiptResponse;
import com.acme.procurement.v1.RenewGoodsReceiptRequest;
import com.acme.procurement.v1.RenewGoodsReceiptResponse;
import com.acme.procurement.v1.ResolvePurchaseRequisitionsRequest;
import com.acme.procurement.v1.ResolvePurchaseRequisitionsResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.procurement.v1.RfqReportingServiceGrpc;
import com.acme.procurement.v1.ScheduleGoodsReceiptExpiryRequest;
import com.acme.procurement.v1.ScheduleGoodsReceiptExpiryResponse;
import com.acme.procurement.v1.SearchPurchaseRequisitionsRequest;
import com.acme.procurement.v1.SearchPurchaseRequisitionsResponse;
import com.acme.procurement.v1.SuspendGoodsReceiptRequest;
import com.acme.procurement.v1.SuspendGoodsReceiptResponse;
import com.acme.procurement.v1.UpdatePurchaseRequisitionRequest;
import com.acme.procurement.v1.UpdatePurchaseRequisitionResponse;
import com.acme.shows.v1.QuoteShowRequest;
import com.acme.shows.v1.QuoteShowResponse;
import com.acme.shows.v1.Show;
import com.acme.shows.v1.ShowCalculationServiceGrpc;
import com.acme.subscriptions.v1.Subscription;
import com.acme.subscriptions.v1.SubscriptionServiceGrpc;
import com.acme.subscriptions.v1.UpdateSubscriptionRequest;
import com.acme.subscriptions.v1.UpdateSubscriptionResponse;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Moves a goods receipt between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class GoodsReceiptLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(GoodsReceiptLifecycleApplication.class);

    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking;

    private final ChannelRegistry channels;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking;

    private final PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final GoodsReceiptLifecycleRequestMapper mapper;

    private final MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking;

    private final GoodsReceiptValidationServiceGrpc.GoodsReceiptValidationServiceBlockingStub procurementGoodsReceiptValidationServiceBlocking;

    private final PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceBlockingStub procurementPurchaseOrderValidationServiceBlocking;

    private final PurchaseRequisitionQueryServiceGrpc.PurchaseRequisitionQueryServiceBlockingStub procurementPurchaseRequisitionQueryServiceBlocking;

    private final PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceBlockingStub procurementPurchaseRequisitionServiceBlocking;

    private final RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking;

    private final RfqReportingServiceGrpc.RfqReportingServiceBlockingStub procurementRfqReportingServiceBlocking;

    private final ProcurementRepository repository;

    private final Executor rpcExecutor;

    private final ShowCalculationServiceGrpc.ShowCalculationServiceBlockingStub showsShowCalculationServiceBlocking;

    private final SubscriptionServiceGrpc.SubscriptionServiceBlockingStub subscriptionsSubscriptionServiceBlocking;

    public GoodsReceiptLifecycleApplication(
            AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking,
            ChannelRegistry channels,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking,
            PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking,
            ApplicationEventPublisher events,
            GoodsReceiptLifecycleRequestMapper mapper,
            MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking,
            GoodsReceiptValidationServiceGrpc.GoodsReceiptValidationServiceBlockingStub procurementGoodsReceiptValidationServiceBlocking,
            PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceBlockingStub procurementPurchaseOrderValidationServiceBlocking,
            PurchaseRequisitionQueryServiceGrpc.PurchaseRequisitionQueryServiceBlockingStub procurementPurchaseRequisitionQueryServiceBlocking,
            PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceBlockingStub procurementPurchaseRequisitionServiceBlocking,
            RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking,
            RfqReportingServiceGrpc.RfqReportingServiceBlockingStub procurementRfqReportingServiceBlocking,
            ProcurementRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            ShowCalculationServiceGrpc.ShowCalculationServiceBlockingStub showsShowCalculationServiceBlocking,
            SubscriptionServiceGrpc.SubscriptionServiceBlockingStub subscriptionsSubscriptionServiceBlocking) {
        this.auditAuditTrailHistoryServiceBlocking = auditAuditTrailHistoryServiceBlocking;
        this.channels = channels;
        this.customersCustomerCalculationServiceBlocking = customersCustomerCalculationServiceBlocking;
        this.customersPreferenceValidationServiceBlocking = customersPreferenceValidationServiceBlocking;
        this.events = events;
        this.mapper = mapper;
        this.metricsMetricSeriesValidationServiceBlocking = metricsMetricSeriesValidationServiceBlocking;
        this.procurementGoodsReceiptValidationServiceBlocking = procurementGoodsReceiptValidationServiceBlocking;
        this.procurementPurchaseOrderValidationServiceBlocking = procurementPurchaseOrderValidationServiceBlocking;
        this.procurementPurchaseRequisitionQueryServiceBlocking = procurementPurchaseRequisitionQueryServiceBlocking;
        this.procurementPurchaseRequisitionServiceBlocking = procurementPurchaseRequisitionServiceBlocking;
        this.procurementRfqCalculationServiceBlocking = procurementRfqCalculationServiceBlocking;
        this.procurementRfqReportingServiceBlocking = procurementRfqReportingServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.showsShowCalculationServiceBlocking = showsShowCalculationServiceBlocking;
        this.subscriptionsSubscriptionServiceBlocking = subscriptionsSubscriptionServiceBlocking;
    }

    public record ReinstateGoodsReceiptStage1Requested(RejectSanctionRequest request, CompletableFuture<RejectSanctionResponse> completion, io.grpc.Context context) {}

    public record ExpireGoodsReceiptStage1Requested(UpdatePurchaseRequisitionRequest request, CompletableFuture<UpdatePurchaseRequisitionResponse> completion, io.grpc.Context context) {}

    /**
     * Activates a goods receipt.
     */
    public ActivateGoodsReceiptResponse activateGoodsReceipt(ActivateGoodsReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateGoodsReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "GoodsReceipt", request.getGoodsReceiptId(), "ActivateGoodsReceipt");

        var outbound0 = CancelControlWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .setDetails(CancelControlWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.compliance.v1.ControlWorkflowService");
        var serviceName0 = "acme.compliance.v1" + "." + "ControlWorkflowService";
        var methodName0 = "Can" + "celControlWorkflow";
        var descriptor0 = MethodDescriptor.<CancelControlWorkflowRequest, CancelControlWorkflowResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(CancelControlWorkflowRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CancelControlWorkflowResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = mapper.activateGoodsReceiptStage1(request, reply0);
        var reply1 = procurementRfqReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listRfqReports(outbound1);

        var outbound2 = BeanCopies.copy(CheckGoodsReceiptEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckGoodsReceiptEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getControl().getAudit().getCreatedBy())
                                .build())
                        .addLines(GoodsReceiptLine.newBuilder()
                                .setTax(GoodsReceiptLineTax.newBuilder()
                                        .setAmount(reply0.getControl().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build(), CheckGoodsReceiptEligibilityRequest.class);
        var reply2 = procurementGoodsReceiptValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkGoodsReceiptEligibility(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateGoodsReceipt", request.getDetails().getReasonCode());
        return ActivateGoodsReceiptResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setNumber(reply2.getGoodsReceipt().getCounterparty().getNumber())
                                .build())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .build())
                .setOutcome(ActivateGoodsReceiptResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Suspends a goods receipt.
     */
    public SuspendGoodsReceiptResponse suspendGoodsReceipt(SuspendGoodsReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendGoodsReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "GoodsReceipt", request.getGoodsReceiptId(), "SuspendGoodsReceipt");

        var outbound0 = PreviewPreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewPreferenceRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .setPreference(Preference.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> customersPreferenceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewPreference(outbound0), rpcExecutor);

        var outbound1 = ScoreMetricSeriesRequest.newBuilder()
                .setContext(request.getContext())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> metricsMetricSeriesValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreMetricSeries(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = PreviewPurchaseOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(switch (reply1.getMetricSeries().getLocatorCase()) {
                                    case ADDRESS -> reply1.getMetricSeries().getAddress().getCountryCode();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = procurementPurchaseOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewPurchaseOrder(outbound2);

        var outbound3 = UpdateSubscriptionRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscription(Subscription.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getMetricSeries().getAudit().getUpdatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .setHolder(CustomerRef.newBuilder()
                                .setEmail(reply1.getMetricSeries().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<UpdateSubscriptionRequest, UpdateSubscriptionResponse> port3 = RpcPort.adapt(subscriptionsSubscriptionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::updateSubscription);
        var reply3 = port3.exchange(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendGoodsReceipt", request.getDetails().getReasonCode());
        return SuspendGoodsReceiptResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getMetricSeries().getAudit().getCreatedBy())
                                .setUpdatedBy(reply1.getMetricSeries().getAudit().getUpdatedBy())
                                .build())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .build())
                .setOutcome(SuspendGoodsReceiptResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Reinstates a goods receipt.
     */
    public ReinstateGoodsReceiptResponse reinstateGoodsReceipt(ReinstateGoodsReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateGoodsReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "GoodsReceipt", request.getGoodsReceiptId(), "ReinstateGoodsReceipt");

        var outbound0 = CalculateCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .build();
        var reply0 = customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateCustomer(outbound0);

        var outbound1 = RejectSanctionRequest.newBuilder()
                .setContext(request.getContext())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(reply0.getCustomer().getAudit().getCreatedAt())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .addAllLines(reply0.getCustomer().getPrimaryAddress().getLinesList())
                                                .setPostalCode(reply0.getCustomer().getPrimaryAddress().getPostalCode())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(reply0.getCustomer().getRelationshipManager().getDisplayName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<RejectSanctionResponse>();
        events.publishEvent(new ReinstateGoodsReceiptStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateGoodsReceipt", request.getDetails().getReasonCode());
        return ReinstateGoodsReceiptResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getSanction().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getCustomer().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setDisplayName(reply1.getSanction().getManager().getDisplayName())
                                .build())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .build())
                .setOutcome(ReinstateGoodsReceiptResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Renews a goods receipt.
     */
    public RenewGoodsReceiptResponse renewGoodsReceipt(RenewGoodsReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewGoodsReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "GoodsReceipt", request.getGoodsReceiptId(), "RenewGoodsReceipt");

        var outbound0 = QuoteShowRequest.newBuilder()
                .setContext(request.getContext())
                .setShow(Show.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<QuoteShowRequest, QuoteShowResponse>() {
            @Override
            public QuoteShowResponse apply(QuoteShowRequest input) {
                return showsShowCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteShow(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = SearchPurchaseRequisitionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchPurchaseRequisitionsRequest.Filter.newBuilder()
                        .setRegion(reply0.getShow().getLocation().getRegion())
                        .build())
                .build();
        var reply1 = RpcSupport.retryRead(() -> procurementPurchaseRequisitionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchPurchaseRequisitions(outbound1));

        var outbound2 = JsonCodec.roundTrip(ResolvePurchaseRequisitionsRequest.newBuilder()
                .setContext(request.getContext())
                .build(), ResolvePurchaseRequisitionsRequest.class);
        var reply2 = procurementPurchaseRequisitionQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolvePurchaseRequisitions(outbound2);

        var outbound3 = CompareRfqOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareRfqOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getShow().getLocation().getPostalCode())
                                .build())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        var reply3 = Optional.of(outbound3).map(value -> value.toBuilder().build())
                .map(value -> procurementRfqCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareRfqOptions(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewGoodsReceipt", request.getDetails().getReasonCode());
        return RenewGoodsReceiptResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setDueAt(reply3.getRfq().getSla().getDueAt())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .putAllMetadata(reply0.getShow().getMetadataMap())
                        .build())
                .setOutcome(RenewGoodsReceiptResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Expires a goods receipt.
     */
    public ExpireGoodsReceiptResponse expireGoodsReceipt(ExpireGoodsReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireGoodsReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "GoodsReceipt", request.getGoodsReceiptId(), "ExpireGoodsReceipt");

        var builder0 = CompareAuditTrailVersionsRequest.newBuilder();
        builder0.putAllAnnotations(request.getAnnotationsMap());
        builder0.setAuditTrail(AuditTrail.newBuilder()
                .setCounterparty(RetentionPolicyRef.newBuilder()
                        .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                        .build())
                .build());
        builder0.setContext(request.getContext());
        builder0.setDetails(CompareAuditTrailVersionsRequest.Details.newBuilder()
                .setSnapshot(JsonCodec.encode(request.getContext()))
                .build());
        builder0.setReason(request.getReason());
        var outbound0 = builder0.build();
        var reply0 = auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareAuditTrailVersions(outbound0);

        var outbound1 = UpdatePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getAuditTrail().getAudit().getCreatedAt())
                                .setUpdatedBy(reply0.getAuditTrail().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getAuditTrail().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<UpdatePurchaseRequisitionResponse>();
        events.publishEvent(new ExpireGoodsReceiptStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = CompareRfqOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareRfqOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getAuditTrail().getBillingAddress().getPostalCode())
                                .setRecipient(reply1.getPurchaseRequisition().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build();
        var reply2 = procurementRfqCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareRfqOptions(outbound2);

        var outbound3 = ListPendingPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .setRegion(reply1.getPurchaseRequisition().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.insurance.v1.PolicyApprovalService");
        var selected3 = PolicyApprovalServiceGrpc.newBlockingStub(channel3)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply3 = selected3.listPendingPolicies(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireGoodsReceipt", request.getDetails().getReasonCode());
        return ExpireGoodsReceiptResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getRfq().getAudit().getCreatedAt())
                                .build())
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setDisplayName(reply0.getAuditTrail().getCounterparty().getDisplayName())
                                .build())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .putAllMetadata(reply0.getAuditTrail().getMetadataMap())
                        .build())
                .setOutcome(ExpireGoodsReceiptResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns a goods receipt.
     */
    public GetGoodsReceiptLifecycleResponse getGoodsReceiptLifecycle(GetGoodsReceiptLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetGoodsReceiptLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "GoodsReceipt", request.getGoodsReceiptId(), "GetGoodsReceiptLifecycle");
        return GetGoodsReceiptLifecycleResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .build())
                .build();
    }

    /**
     * Schedules a goods receipt.
     */
    public ScheduleGoodsReceiptExpiryResponse scheduleGoodsReceiptExpiry(ScheduleGoodsReceiptExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleGoodsReceiptExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var outbound0 = LookupPurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisitionId(request.getGoodsReceipt().getCounterparty().getPurchaseRequisitionId())
                .build();
        var reply0 = RpcSupport.retryRead(() -> procurementPurchaseRequisitionQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lookupPurchaseRequisition(outbound0));

        var outbound1 = ListPurchaseRequisitionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPurchaseRequisitionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getGoodsReceipt().getAudit().getCreatedAt())
                        .setRegion(request.getGoodsReceipt().getBillingAddress().getRegion())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.procurement.v1.PurchaseRequisitionService/ListPurchaseRequisitions", outbound1);
        var reply1 = procurementPurchaseRequisitionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPurchaseRequisitions(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleGoodsReceiptExpiry", request.getGoodsReceipt().getGoodsReceiptId());
        return ScheduleGoodsReceiptExpiryResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .setNotes(request.getGoodsReceipt().getNotes())
                        .build())
                .setOutcome(ScheduleGoodsReceiptExpiryResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
