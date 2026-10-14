package com.acme.finance.domains.payroll;

import com.acme.assets.v1.DepreciationQueryServiceGrpc;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.LintFixedAssetsRequest;
import com.acme.assets.v1.LintFixedAssetsResponse;
import com.acme.assets.v1.SummarizeDepreciationsRequest;
import com.acme.assets.v1.SummarizeDepreciationsResponse;
import com.acme.assets.v1.ValidateFixedAssetRequest;
import com.acme.assets.v1.ValidateFixedAssetResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.CompareAuditTrailVersionsRequest;
import com.acme.audit.v1.CompareAuditTrailVersionsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.CloneLoanApplicationRequest;
import com.acme.lending.v1.CloneLoanApplicationResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationLine;
import com.acme.lending.v1.LoanApplicationLineTax;
import com.acme.lending.v1.LoanApplicationServiceGrpc;
import com.acme.payroll.v1.CalculateDeductionRequest;
import com.acme.payroll.v1.CalculateDeductionResponse;
import com.acme.payroll.v1.CompareDeductionOptionsRequest;
import com.acme.payroll.v1.CompareDeductionOptionsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.EstimateDeductionRequest;
import com.acme.payroll.v1.EstimateDeductionResponse;
import com.acme.payroll.v1.ExplainDeductionCalculationRequest;
import com.acme.payroll.v1.ExplainDeductionCalculationResponse;
import com.acme.payroll.v1.ExplainPayRunCalculationRequest;
import com.acme.payroll.v1.ExplainPayRunCalculationResponse;
import com.acme.payroll.v1.GetDeductionRatesRequest;
import com.acme.payroll.v1.GetDeductionRatesResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunCardSettlement;
import com.acme.payroll.v1.PayRunValidationServiceGrpc;
import com.acme.payroll.v1.QuoteDeductionRequest;
import com.acme.payroll.v1.QuoteDeductionResponse;
import com.acme.payroll.v1.QuotePayRunRequest;
import com.acme.payroll.v1.QuotePayRunResponse;
import com.acme.payroll.v1.RecalculateDeductionsRequest;
import com.acme.payroll.v1.RecalculateDeductionsResponse;
import com.acme.payroll.v1.SimulateDeductionRequest;
import com.acme.payroll.v1.SimulateDeductionResponse;
import com.acme.payroll.v1.SimulatePayRunRequest;
import com.acme.payroll.v1.SimulatePayRunResponse;
import com.acme.payroll.v1.SimulatePayStubRequest;
import com.acme.payroll.v1.SimulatePayStubResponse;
import com.acme.payroll.v1.ValidatePayRunRequest;
import com.acme.payroll.v1.ValidatePayRunResponse;
import com.acme.procurement.v1.CalculateGoodsReceiptRequest;
import com.acme.procurement.v1.CalculateGoodsReceiptResponse;
import com.acme.procurement.v1.CountPurchaseOrdersRequest;
import com.acme.procurement.v1.CountPurchaseOrdersResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptCalculationServiceGrpc;
import com.acme.procurement.v1.GoodsReceiptLine;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a deduction, such as totals, taxes, and estimates. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class DeductionCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(DeductionCalculationApplication.class);

    private final DepreciationQueryServiceGrpc.DepreciationQueryServiceBlockingStub assetsDepreciationQueryServiceBlocking;

    private final FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking;

    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking;

    private final ChannelRegistry channels;

    private final LoanApplicationServiceGrpc.LoanApplicationServiceBlockingStub lendingLoanApplicationServiceBlocking;

    private final DeductionCalculationRequestMapper mapper;

    private final PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking;

    private final PayRunValidationServiceGrpc.PayRunValidationServiceBlockingStub payrollPayRunValidationServiceBlocking;

    private final GoodsReceiptCalculationServiceGrpc.GoodsReceiptCalculationServiceBlockingStub procurementGoodsReceiptCalculationServiceBlocking;

    private final PayrollRepository repository;

    public DeductionCalculationApplication(
            DepreciationQueryServiceGrpc.DepreciationQueryServiceBlockingStub assetsDepreciationQueryServiceBlocking,
            FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking,
            AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking,
            ChannelRegistry channels,
            LoanApplicationServiceGrpc.LoanApplicationServiceBlockingStub lendingLoanApplicationServiceBlocking,
            DeductionCalculationRequestMapper mapper,
            PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking,
            PayRunValidationServiceGrpc.PayRunValidationServiceBlockingStub payrollPayRunValidationServiceBlocking,
            GoodsReceiptCalculationServiceGrpc.GoodsReceiptCalculationServiceBlockingStub procurementGoodsReceiptCalculationServiceBlocking,
            PayrollRepository repository) {
        this.assetsDepreciationQueryServiceBlocking = assetsDepreciationQueryServiceBlocking;
        this.assetsFixedAssetValidationServiceBlocking = assetsFixedAssetValidationServiceBlocking;
        this.auditAuditTrailHistoryServiceBlocking = auditAuditTrailHistoryServiceBlocking;
        this.channels = channels;
        this.lendingLoanApplicationServiceBlocking = lendingLoanApplicationServiceBlocking;
        this.mapper = mapper;
        this.payrollPayRunCalculationServiceBlocking = payrollPayRunCalculationServiceBlocking;
        this.payrollPayRunValidationServiceBlocking = payrollPayRunValidationServiceBlocking;
        this.procurementGoodsReceiptCalculationServiceBlocking = procurementGoodsReceiptCalculationServiceBlocking;
        this.repository = repository;
    }

    /**
     * Calculates a deduction.
     */
    public CalculateDeductionResponse calculateDeduction(CalculateDeductionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateDeduction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = LintFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDeduction().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.assets.v1.FixedAssetValidationService");
        var selected0 = FixedAssetValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.lintFixedAssets(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateDeduction", request.getDeduction().getDeductionId());
        return CalculateDeductionResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .setName(request.getDeduction().getName())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Estimates a deduction.
     */
    public EstimateDeductionResponse estimateDeduction(EstimateDeductionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateDeduction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = mapper.estimateDeductionStage0(request);
        var reply0 = payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulatePayRun(outbound0);

        var outbound1 = ExplainPayRunCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPayRunCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDeduction().getOwner().getEmail())
                                .build())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .setCard(PayRunCardSettlement.newBuilder()
                                .setNetwork(switch (reply0.getPayRun().getSettlementCase()) {
                                    case CARD -> reply0.getPayRun().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPayRunCalculation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateDeduction", request.getDeductionId());
        return EstimateDeductionResponse.newBuilder()
                .setBreakdown(EstimateDeductionResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(reply1.getPayRun().getCounterparty().getDeductionId())
                        .setStatus(request.getDeduction().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Quotes a deduction.
     */
    public QuoteDeductionResponse quoteDeduction(QuoteDeductionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteDeduction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = JsonCodec.roundTrip(CloneLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .addLines(LoanApplicationLine.newBuilder()
                                .setTax(LoanApplicationLineTax.newBuilder()
                                        .setAmount(request.getDeduction().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build(), CloneLoanApplicationRequest.class);
        var reply0 = lendingLoanApplicationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneLoanApplication(outbound0);

        var outbound1 = JsonCodec.roundTrip(QuotePayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getLoanApplication().getBillingAddress().getCountryCode())
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .build())
                .build(), QuotePayRunRequest.class);
        var reply1 = payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePayRun(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteDeduction", request.getDeduction().getDeductionId());
        return QuoteDeductionResponse.newBuilder()
                .setBreakdown(QuoteDeductionResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .setTotal(reply0.getLoanApplication().getTotal())
                        .build())
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Compares a deduction.
     */
    public CompareDeductionOptionsResponse compareDeductionOptions(CompareDeductionOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareDeductionOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = SummarizeDepreciationsRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(switch (request.getDeduction().getActionCase()) {
                    case BLOCK -> request.getDeduction().getBlock().getReason();
                    default -> "";
                })
                .build();
        var reply0 = assetsDepreciationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeDepreciations(outbound0);

        var outbound1 = CountPurchaseOrdersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountPurchaseOrdersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDeduction().getAudit().getUpdatedAt())
                        .putAllLabels(request.getDeduction().getParametersMap())
                        .setOwnerId(reply0.getReportId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.procurement.v1.PurchaseOrderQueryService");
        var serviceName1 = "acme.procurement.v1" + "." + "PurchaseOrderQueryService";
        var methodName1 = "Cou" + "ntPurchaseOrders";
        var descriptor1 = MethodDescriptor.<CountPurchaseOrdersRequest, CountPurchaseOrdersResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(CountPurchaseOrdersRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CountPurchaseOrdersResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareDeductionOptions", request.getDeduction().getDeductionId());
        return CompareDeductionOptionsResponse.newBuilder()
                .setBreakdown(CompareDeductionOptionsResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Recalculates a deduction.
     */
    public RecalculateDeductionsResponse recalculateDeductions(RecalculateDeductionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateDeductions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = CalculateGoodsReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .addLines(GoodsReceiptLine.newBuilder()
                                .setAmount(request.getDeduction().getDiscount().getAmount())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(procurementGoodsReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateGoodsReceipt(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = RpcSupport.unpack(Any.pack(ValidatePayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidatePayRunRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDeduction().getOwner().getEmail())
                                .build())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getGoodsReceipt().getBillingAddress().getLinesList())
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .build()), ValidatePayRunRequest.class);
        var reply1 = payrollPayRunValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePayRun(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateDeductions", request.getDeduction().getDeductionId());
        return RecalculateDeductionsResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getGoodsReceipt().getAudit().getCreatedBy())
                                .setUpdatedBy(reply1.getPayRun().getAudit().getUpdatedBy())
                                .build())
                        .setDeductionId(request.getDeductionId())
                        .setEffective(request.getDeduction().getEffective())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Simulates a deduction.
     */
    public SimulateDeductionResponse simulateDeduction(SimulateDeductionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateDeduction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = SimulatePayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulatePayStubRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.payroll.v1.PayStubCalculationService");
        var serviceName0 = "acme.payroll.v1" + "." + "PayStubCalculationService";
        var methodName0 = "Sim" + "ulatePayStub";
        var descriptor0 = MethodDescriptor.<SimulatePayStubRequest, SimulatePayStubResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(SimulatePayStubRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SimulatePayStubResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateDeduction", request.getDeductionId());
        return SimulateDeductionResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(reply0.getPayStub().getCounterparty().getDeductionId())
                        .setName(request.getDeduction().getName())
                        .setStatus(request.getDeduction().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Explains a deduction.
     */
    public ExplainDeductionCalculationResponse explainDeductionCalculation(ExplainDeductionCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainDeductionCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = (ValidateFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateFixedAssetRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDeduction().getOwner().getEmail())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = assetsFixedAssetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateFixedAsset(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainDeductionCalculation", request.getDeduction().getDeductionId());
        return ExplainDeductionCalculationResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .setOwner(request.getDeduction().getOwner())
                        .build())
                .setOutcome(ExplainDeductionCalculationResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .build();
    }

    /**
     * Returns a deduction.
     */
    public GetDeductionRatesResponse getDeductionRates(GetDeductionRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetDeductionRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Deduction", request.getDeductionId(), "GetDeductionRates");

        var outbound0 = CompareAuditTrailVersionsRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setIssuedAt(request.getContext().getDeadline())
                        .putAllMetadata(request.getContext().getBaggageMap())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareAuditTrailVersionsRequest.Details.newBuilder()
                        .setAuthorEmail(request.getContext().getActor().getEmail())
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareAuditTrailVersions(value));
        return GetDeductionRatesResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getAuditTrail().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getAuditTrail().getAudit().getUpdatedAt())
                                .build())
                        .setDeductionId(request.getDeductionId())
                        .build())
                .build();
    }
}
