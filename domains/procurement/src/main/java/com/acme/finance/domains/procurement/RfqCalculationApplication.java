package com.acme.finance.domains.procurement;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.StreamRetentionPolicyExportRequest;
import com.acme.audit.v1.StreamRetentionPolicyExportResponse;
import com.acme.banking.v1.EstimateMandateRequest;
import com.acme.banking.v1.EstimateMandateResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.ExplainBudgetLineDecisionRequest;
import com.acme.budgeting.v1.ExplainBudgetLineDecisionResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.AnnotateJournalEntryVersionRequest;
import com.acme.ledger.v1.AnnotateJournalEntryVersionResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.lending.v1.ExplainLoanApplicationCalculationRequest;
import com.acme.lending.v1.ExplainLoanApplicationCalculationResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.procurement.v1.CalculateRfqRequest;
import com.acme.procurement.v1.CalculateRfqResponse;
import com.acme.procurement.v1.CompareGoodsReceiptOptionsRequest;
import com.acme.procurement.v1.CompareGoodsReceiptOptionsResponse;
import com.acme.procurement.v1.CompareRfqOptionsRequest;
import com.acme.procurement.v1.CompareRfqOptionsResponse;
import com.acme.procurement.v1.EstimateGoodsReceiptRequest;
import com.acme.procurement.v1.EstimateGoodsReceiptResponse;
import com.acme.procurement.v1.EstimateRfqRequest;
import com.acme.procurement.v1.EstimateRfqResponse;
import com.acme.procurement.v1.ExplainRfqCalculationRequest;
import com.acme.procurement.v1.ExplainRfqCalculationResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptCalculationServiceGrpc;
import com.acme.procurement.v1.LintPurchaseOrdersRequest;
import com.acme.procurement.v1.LintPurchaseOrdersResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderCardSettlement;
import com.acme.procurement.v1.PurchaseOrderValidationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.QuoteRfqRequest;
import com.acme.procurement.v1.QuoteRfqResponse;
import com.acme.procurement.v1.RecalculateGoodsReceiptsRequest;
import com.acme.procurement.v1.RecalculateGoodsReceiptsResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqComment;
import com.acme.procurement.v1.RfqSla;
import com.acme.procurement.v1.SimulateGoodsReceiptRequest;
import com.acme.procurement.v1.SimulateGoodsReceiptResponse;
import com.acme.procurement.v1.SimulatePurchaseOrderRequest;
import com.acme.procurement.v1.SimulatePurchaseOrderResponse;
import com.acme.procurement.v1.SimulateRfqRequest;
import com.acme.procurement.v1.SimulateRfqResponse;
import com.acme.risk.v1.BrowseRiskScenariosRequest;
import com.acme.risk.v1.BrowseRiskScenariosResponse;
import com.acme.risk.v1.CountRiskScenariosRequest;
import com.acme.risk.v1.CountRiskScenariosResponse;
import com.acme.risk.v1.RiskScenarioQueryServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a rfq, such as totals, taxes, and estimates. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class RfqCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(RfqCalculationApplication.class);

    private final FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync;

    private final RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking;

    private final BudgetLineValidationServiceGrpc.BudgetLineValidationServiceBlockingStub budgetingBudgetLineValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking;

    private final RfqCalculationRequestMapper mapper;

    private final GoodsReceiptCalculationServiceGrpc.GoodsReceiptCalculationServiceBlockingStub procurementGoodsReceiptCalculationServiceBlocking;

    private final PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceBlockingStub procurementPurchaseOrderValidationServiceBlocking;

    private final ProcurementRepository repository;

    private final RiskScenarioQueryServiceGrpc.RiskScenarioQueryServiceBlockingStub riskRiskScenarioQueryServiceBlocking;

    public RfqCalculationApplication(
            FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync,
            RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking,
            BudgetLineValidationServiceGrpc.BudgetLineValidationServiceBlockingStub budgetingBudgetLineValidationServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking,
            RfqCalculationRequestMapper mapper,
            GoodsReceiptCalculationServiceGrpc.GoodsReceiptCalculationServiceBlockingStub procurementGoodsReceiptCalculationServiceBlocking,
            PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceBlockingStub procurementPurchaseOrderValidationServiceBlocking,
            ProcurementRepository repository,
            RiskScenarioQueryServiceGrpc.RiskScenarioQueryServiceBlockingStub riskRiskScenarioQueryServiceBlocking) {
        this.assetsFixedAssetServiceAsync = assetsFixedAssetServiceAsync;
        this.auditRetentionPolicyExportServiceBlocking = auditRetentionPolicyExportServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.bankingMandateCalculationServiceBlocking = bankingMandateCalculationServiceBlocking;
        this.budgetingBudgetLineValidationServiceBlocking = budgetingBudgetLineValidationServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.lendingLoanApplicationCalculationServiceBlocking = lendingLoanApplicationCalculationServiceBlocking;
        this.mapper = mapper;
        this.procurementGoodsReceiptCalculationServiceBlocking = procurementGoodsReceiptCalculationServiceBlocking;
        this.procurementPurchaseOrderValidationServiceBlocking = procurementPurchaseOrderValidationServiceBlocking;
        this.repository = repository;
        this.riskRiskScenarioQueryServiceBlocking = riskRiskScenarioQueryServiceBlocking;
    }

    public record CalculateRfqStage2Requested(CountRiskScenariosRequest request, CompletableFuture<CountRiskScenariosResponse> completion, io.grpc.Context context) {}

    public record SimulateRfqStage0Requested(AnnotateJournalEntryVersionRequest request, CompletableFuture<AnnotateJournalEntryVersionResponse> completion, io.grpc.Context context) {}

    /**
     * Calculates a rfq.
     */
    public CalculateRfqResponse calculateRfq(CalculateRfqRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateRfq")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRfq(request.getContext().getTenantId(), request.getRfq());

        var outbound0 = EstimateGoodsReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateGoodsReceiptRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .build())
                        .build())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getRfq().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.procurement.v1.GoodsReceiptCalculationService");
        var serviceName0 = "acme.procurement.v1" + "." + "GoodsReceiptCalculationService";
        var methodName0 = "Est" + "imateGoodsReceipt";
        var descriptor0 = MethodDescriptor.<EstimateGoodsReceiptRequest, EstimateGoodsReceiptResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(EstimateGoodsReceiptRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(EstimateGoodsReceiptResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        List<SimulateGoodsReceiptResponse> results1 = request.getRfq().getCommentsList().stream()
                .map(element -> SimulateGoodsReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(procurementGoodsReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateGoodsReceipt(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? SimulateGoodsReceiptResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = CountRiskScenariosRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountRiskScenariosRequest.Filter.newBuilder()
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var completion2 = new CompletableFuture<CountRiskScenariosResponse>();
        events.publishEvent(new CalculateRfqStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound3 = BatchCreateFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateFixedAssetsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getRfq().getRequester())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(BatchCreateFixedAssetsResponse.getDefaultInstance());
        var input3 = assetsFixedAssetServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateFixedAssets(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getRfq().getAudit().getCreatedAt())
                                .setCreatedBy(request.getRfq().getAudit().getCreatedBy())
                                .setEtag(request.getRfq().getAudit().getEtag())
                                .setUpdatedAt(request.getRfq().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input4 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getRfq().getRfqId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateRfq", request.getRfq().getRfqId());
        return CalculateRfqResponse.newBuilder()
                .setBreakdown(CalculateRfqResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply0.getBreakdown().getCurrencyCode())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setRfqId(request.getRfqId())
                        .setStatus(request.getRfq().getStatus())
                        .build())
                .build();
    }

    /**
     * Estimates a rfq.
     */
    public EstimateRfqResponse estimateRfq(EstimateRfqRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateRfq")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRfq(request.getContext().getTenantId(), request.getRfq());

        var outbound0 = CompareGoodsReceiptOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getRfq().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .build();
        Function<CompareGoodsReceiptOptionsRequest, CompareGoodsReceiptOptionsResponse> operation0 = procurementGoodsReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::compareGoodsReceiptOptions;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = StreamRetentionPolicyExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamRetentionPolicyExportRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getGoodsReceipt().getIssuedAt())
                        .putAllLabels(reply0.getGoodsReceipt().getMetadataMap())
                        .setOwnerId(request.getRfq().getSla().getPolicyId())
                        .setRegion(reply0.getGoodsReceipt().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = auditRetentionPolicyExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyExport(outbound1);
        var reply1 = StreamRetentionPolicyExportResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getRfq().getRfqId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateRfq", request.getRfq().getRfqId());
        return EstimateRfqResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .addComments(RfqComment.newBuilder()
                                .setCreatedAt(reply0.getGoodsReceipt().getAudit().getCreatedAt())
                                .build())
                        .setRequester(request.getRfq().getRequester())
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }

    /**
     * Quotes a rfq.
     */
    public QuoteRfqResponse quoteRfq(QuoteRfqRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteRfq")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRfq(request.getContext().getTenantId(), request.getRfq());

        var outbound0 = ExplainLoanApplicationCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getRfq().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setCustomerId(switch (request.getRfq().getOriginCase()) {
                                    case CUSTOMER -> request.getRfq().getCustomer().getCustomerId();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = lendingLoanApplicationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainLoanApplicationCalculation(outbound0);

        var outbound1 = LintPurchaseOrdersRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setCard(PurchaseOrderCardSettlement.newBuilder()
                                .setHolderName(switch (reply0.getLoanApplication().getSettlementCase()) {
                                    case CARD -> reply0.getLoanApplication().getCard().getHolderName();
                                    default -> "";
                                })
                                .setToken(switch (reply0.getLoanApplication().getSettlementCase()) {
                                    case CARD -> reply0.getLoanApplication().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setDisplayName(request.getRfq().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = procurementPurchaseOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintPurchaseOrders(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRfq().getRfqId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteRfq", request.getRfq().getRfqId());
        return QuoteRfqResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getPurchaseOrder().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getLoanApplication().getAudit().getCreatedBy())
                                .build())
                        .setRfqId(request.getRfqId())
                        .setSubject(request.getRfq().getSubject())
                        .build())
                .build();
    }

    /**
     * Compares a rfq.
     */
    public CompareRfqOptionsResponse compareRfqOptions(CompareRfqOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareRfqOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRfq(request.getContext().getTenantId(), request.getRfq());

        var outbound0 = ExplainBudgetLineDecisionRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getRfq().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var adapter0 = new Function<ExplainBudgetLineDecisionRequest, ExplainBudgetLineDecisionResponse>() {
            @Override
            public ExplainBudgetLineDecisionResponse apply(ExplainBudgetLineDecisionRequest input) {
                return budgetingBudgetLineValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainBudgetLineDecision(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = SimulatePurchaseOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulatePurchaseOrderRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getBudgetLine().getBillingAddress().getCountryCode())
                                .setRegion(reply0.getBudgetLine().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getBudgetLine().getBillingAddress().getLinesList())
                                .build())
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setDisplayName(reply0.getBudgetLine().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.procurement.v1.PurchaseOrderCalculationService");
        var selected1 = PurchaseOrderCalculationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.simulatePurchaseOrder(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRfq().getRfqId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareRfqOptions", request.getRfq().getRfqId());
        return CompareRfqOptionsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getBudgetLine().getCounterparty().getDisplayName())
                                .setEmail(reply0.getBudgetLine().getCounterparty().getEmail())
                                .build())
                        .setRfqId(request.getRfqId())
                        .setSla(RfqSla.newBuilder()
                                .setDueAt(reply1.getPurchaseOrder().getDueAt())
                                .build())
                        .setSubject(request.getRfq().getSubject())
                        .build())
                .build();
    }

    /**
     * Simulates a rfq.
     */
    public SimulateRfqResponse simulateRfq(SimulateRfqRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateRfq")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRfq(request.getContext().getTenantId(), request.getRfq());

        var outbound0 = AnnotateJournalEntryVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AnnotateJournalEntryVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<AnnotateJournalEntryVersionResponse>();
        events.publishEvent(new SimulateRfqStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = (RecalculateGoodsReceiptsRequest.newBuilder()
                .setContext(request.getContext())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = procurementGoodsReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateGoodsReceipts(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRfq().getRfqId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateRfq", request.getRfq().getRfqId());
        return SimulateRfqResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getJournalEntry().getAudit().getEtag())
                                .build())
                        .setPriority(request.getRfq().getPriority())
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }

    /**
     * Explains a rfq.
     */
    public ExplainRfqCalculationResponse explainRfqCalculation(ExplainRfqCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainRfqCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRfq(request.getContext().getTenantId(), request.getRfq());

        var outbound0 = EstimateMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getRfq().getAudit().getCreatedAt())
                                .setUpdatedBy(request.getRfq().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setCustomerId(switch (request.getRfq().getOriginCase()) {
                                    case CUSTOMER -> request.getRfq().getCustomer().getCustomerId();
                                    default -> "";
                                })
                                .setEmail(request.getRfq().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateMandate(outbound0);

        var outbound1 = mapper.explainRfqCalculationStage1(request, reply0);
        var reply1 = riskRiskScenarioQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseRiskScenarios(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRfq().getRfqId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainRfqCalculation", request.getRfq().getRfqId());
        return ExplainRfqCalculationResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getMandate().getAudit().getUpdatedAt())
                                .build())
                        .setRequester(request.getRfq().getRequester())
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }
}
