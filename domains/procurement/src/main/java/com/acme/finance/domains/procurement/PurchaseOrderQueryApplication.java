package com.acme.finance.domains.procurement;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseSla;
import com.acme.compliance.v1.EstimateComplianceCaseRequest;
import com.acme.compliance.v1.EstimateComplianceCaseResponse;
import com.acme.compliance.v1.QuoteComplianceCaseRequest;
import com.acme.compliance.v1.QuoteComplianceCaseResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.ledger.v1.ResolveLedgerAccountsRequest;
import com.acme.ledger.v1.ResolveLedgerAccountsResponse;
import com.acme.ledger.v1.UnsubscribeLedgerAccountUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeLedgerAccountUpdatesResponse;
import com.acme.procurement.v1.BrowsePurchaseOrdersRequest;
import com.acme.procurement.v1.BrowsePurchaseOrdersResponse;
import com.acme.procurement.v1.CheckPurchaseRequisitionEligibilityRequest;
import com.acme.procurement.v1.CheckPurchaseRequisitionEligibilityResponse;
import com.acme.procurement.v1.ComparePurchaseRequisitionOptionsRequest;
import com.acme.procurement.v1.ComparePurchaseRequisitionOptionsResponse;
import com.acme.procurement.v1.CountPurchaseOrdersRequest;
import com.acme.procurement.v1.CountPurchaseOrdersResponse;
import com.acme.procurement.v1.ExplainPurchaseRequisitionDecisionRequest;
import com.acme.procurement.v1.ExplainPurchaseRequisitionDecisionResponse;
import com.acme.procurement.v1.ExportPurchaseOrdersRequest;
import com.acme.procurement.v1.ExportPurchaseOrdersResponse;
import com.acme.procurement.v1.FindPurchaseOrdersRequest;
import com.acme.procurement.v1.FindPurchaseOrdersResponse;
import com.acme.procurement.v1.LookupPurchaseOrderRequest;
import com.acme.procurement.v1.LookupPurchaseOrderResponse;
import com.acme.procurement.v1.PeekPurchaseOrderRequest;
import com.acme.procurement.v1.PeekPurchaseOrderResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderRef;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionBankSettlement;
import com.acme.procurement.v1.PurchaseRequisitionCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.procurement.v1.ResolvePurchaseOrdersRequest;
import com.acme.procurement.v1.ResolvePurchaseOrdersResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.procurement.v1.SimulateRfqRequest;
import com.acme.procurement.v1.SimulateRfqResponse;
import com.acme.procurement.v1.SuggestPurchaseOrdersRequest;
import com.acme.procurement.v1.SuggestPurchaseOrdersResponse;
import com.acme.procurement.v1.SummarizePurchaseOrdersRequest;
import com.acme.procurement.v1.SummarizePurchaseOrdersResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
 * Answers read-only questions about purchase orders, for screens and reports. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class PurchaseOrderQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(PurchaseOrderQueryApplication.class);

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking;

    private final LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking;

    private final PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceBlockingStub procurementPurchaseRequisitionCalculationServiceBlocking;

    private final PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking;

    private final RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking;

    private final ProcurementRepository repository;

    private final Executor rpcExecutor;

    public PurchaseOrderQueryApplication(
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking,
            LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking,
            PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceBlockingStub procurementPurchaseRequisitionCalculationServiceBlocking,
            PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking,
            RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking,
            ProcurementRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.channels = channels;
        this.events = events;
        this.ledgerLedgerAccountNotificationServiceBlocking = ledgerLedgerAccountNotificationServiceBlocking;
        this.ledgerLedgerAccountQueryServiceBlocking = ledgerLedgerAccountQueryServiceBlocking;
        this.procurementPurchaseRequisitionCalculationServiceBlocking = procurementPurchaseRequisitionCalculationServiceBlocking;
        this.procurementPurchaseRequisitionValidationServiceBlocking = procurementPurchaseRequisitionValidationServiceBlocking;
        this.procurementRfqCalculationServiceBlocking = procurementRfqCalculationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    public record CountPurchaseOrdersStage0Requested(QuoteComplianceCaseRequest request, CompletableFuture<QuoteComplianceCaseResponse> completion, io.grpc.Context context) {}

    /**
     * Returns a purchase order.
     */
    public LookupPurchaseOrderResponse lookupPurchaseOrder(LookupPurchaseOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupPurchaseOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseOrder", request.getPurchaseOrderId(), "LookupPurchaseOrder");
        return LookupPurchaseOrderResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .build();
    }

    /**
     * Lists purchase orders a page at a time.
     */
    public BrowsePurchaseOrdersResponse browsePurchaseOrders(BrowsePurchaseOrdersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowsePurchaseOrders")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseOrder", request.getContext().getTraceId(), "BrowsePurchaseOrders");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowsePurchaseOrders", request.getPage().getPageToken());
        return BrowsePurchaseOrdersResponse.getDefaultInstance();
    }

    /**
     * Lists purchase orders a page at a time.
     */
    public FindPurchaseOrdersResponse findPurchaseOrders(FindPurchaseOrdersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindPurchaseOrders")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseOrder", request.getContext().getTraceId(), "FindPurchaseOrders");

        var outbound0 = UnsubscribeLedgerAccountUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(UnsubscribeLedgerAccountUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllVariables(request.getFilter().getLabelsMap())
                        .build())
                .build();
        var adapter0 = new Function<UnsubscribeLedgerAccountUpdatesRequest, UnsubscribeLedgerAccountUpdatesResponse>() {
            @Override
            public UnsubscribeLedgerAccountUpdatesResponse apply(UnsubscribeLedgerAccountUpdatesRequest input) {
                return ledgerLedgerAccountNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).unsubscribeLedgerAccountUpdates(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindPurchaseOrders", request.getPage().getPageToken());
        return FindPurchaseOrdersResponse.newBuilder()
                .addPurchaseOrders(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getLedgerAccount().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Counts purchase orders by status.
     */
    public CountPurchaseOrdersResponse countPurchaseOrders(CountPurchaseOrdersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountPurchaseOrders")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseOrder", request.getContext().getTraceId(), "CountPurchaseOrders");

        var outbound0 = QuoteComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(QuoteComplianceCaseRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<QuoteComplianceCaseResponse>();
        events.publishEvent(new CountPurchaseOrdersStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountPurchaseOrders", request.getPage().getPageToken());
        return CountPurchaseOrdersResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns several purchase orders at once.
     */
    public ResolvePurchaseOrdersResponse resolvePurchaseOrders(ResolvePurchaseOrdersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolvePurchaseOrders")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseOrder", request.getContext().getTraceId(), "ResolvePurchaseOrders");

        var outbound0 = EstimateComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setExternalReference(request.getContext().getTraceId())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setSla(ComplianceCaseSla.newBuilder()
                                .setDueAt(request.getContext().getDeadline())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.compliance.v1.ComplianceCaseCalculationService");
        var serviceName0 = "acme.compliance.v1" + "." + "ComplianceCaseCalculationService";
        var methodName0 = "Est" + "imateComplianceCase";
        var descriptor0 = MethodDescriptor.<EstimateComplianceCaseRequest, EstimateComplianceCaseResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(EstimateComplianceCaseRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(EstimateComplianceCaseResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        return ResolvePurchaseOrdersResponse.newBuilder()
                .addPurchaseOrders(PurchaseOrder.newBuilder()
                        .setTotal(reply0.getBreakdown().getTotal())
                        .build())
                .build();
    }

    /**
     * Streams every matching purchase order.
     */
    public ExportPurchaseOrdersResponse exportPurchaseOrders(ExportPurchaseOrdersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportPurchaseOrders")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseOrder", request.getContext().getTraceId(), "ExportPurchaseOrders");

        var outbound0 = ExplainPurchaseRequisitionDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPurchaseRequisitionDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.procurement.v1.PurchaseRequisitionValidationService");
        var selected0 = PurchaseRequisitionValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.explainPurchaseRequisitionDecision(outbound0);

        var outbound1 = ComparePurchaseRequisitionOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ComparePurchaseRequisitionOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(reply0.getPurchaseRequisition().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPurchaseRequisition().getAudit().getCreatedAt())
                                .build())
                        .setCounterparty(PurchaseOrderRef.newBuilder()
                                .setNumber(reply0.getPurchaseRequisition().getCounterparty().getNumber())
                                .build())
                        .build())
                .build();
        var reply1 = procurementPurchaseRequisitionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePurchaseRequisitionOptions(outbound1);

        var outbound2 = CheckPurchaseRequisitionEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getPurchaseRequisition().getAudit().getUpdatedBy())
                                .build())
                        .setBank(PurchaseRequisitionBankSettlement.newBuilder()
                                .setAccountHolder(reply1.getPurchaseRequisition().getBank().getAccountHolder())
                                .setBic(reply0.getPurchaseRequisition().getBank().getBic())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply1.getPurchaseRequisition().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var reply2 = procurementPurchaseRequisitionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkPurchaseRequisitionEligibility(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportPurchaseOrders", request.getFilter().getQuery());
        return ExportPurchaseOrdersResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply1.getPurchaseRequisition().getBillingAddress().getLocality())
                                .setPostalCode(reply0.getPurchaseRequisition().getBillingAddress().getPostalCode())
                                .setRegion(reply0.getPurchaseRequisition().getBillingAddress().getRegion())
                                .build())
                        .setTotal(reply2.getPurchaseRequisition().getTotal())
                        .build())
                .build();
    }

    /**
     * Summarizes a purchase order.
     */
    public SummarizePurchaseOrdersResponse summarizePurchaseOrders(SummarizePurchaseOrdersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizePurchaseOrders")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseOrder", request.getPurchaseOrderId(), "SummarizePurchaseOrders");

        var outbound0 = SimulateRfqRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .setRfq(Rfq.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> procurementRfqCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateRfq(outbound0), rpcExecutor);

        var outbound1 = ResolveLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> ledgerLedgerAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveLedgerAccounts(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizePurchaseOrders", request.getPurchaseOrderId());
        return SummarizePurchaseOrdersResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists purchase orders a page at a time.
     */
    public SuggestPurchaseOrdersResponse suggestPurchaseOrders(SuggestPurchaseOrdersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestPurchaseOrders")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseOrder", request.getContext().getTraceId(), "SuggestPurchaseOrders");

        var outbound0 = ComparePurchaseRequisitionOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ComparePurchaseRequisitionOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(procurementPurchaseRequisitionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePurchaseRequisitionOptions(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestPurchaseOrders", request.getPage().getPageToken());
        return SuggestPurchaseOrdersResponse.newBuilder()
                .addPurchaseOrders(PurchaseOrder.newBuilder()
                        .setBillingAddress(reply0.getPurchaseRequisition().getBillingAddress())
                        .setNumber(reply0.getPurchaseRequisition().getNumber())
                        .build())
                .build();
    }

    /**
     * Returns a purchase order.
     */
    public PeekPurchaseOrderResponse peekPurchaseOrder(PeekPurchaseOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekPurchaseOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseOrder", request.getPurchaseOrderId(), "PeekPurchaseOrder");
        return PeekPurchaseOrderResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .build();
    }
}
