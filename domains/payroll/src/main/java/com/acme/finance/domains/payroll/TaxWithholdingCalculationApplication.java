package com.acme.finance.domains.payroll;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailAdminServiceGrpc;
import com.acme.audit.v1.AuditTrailBankSettlement;
import com.acme.audit.v1.AuditTrailLine;
import com.acme.audit.v1.PushRetentionPolicyRequest;
import com.acme.audit.v1.PushRetentionPolicyResponse;
import com.acme.audit.v1.RepairAuditTrailRequest;
import com.acme.audit.v1.RepairAuditTrailResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyDiscountAction;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamAuditEventNoticesRequest;
import com.acme.audit.v1.StreamAuditEventNoticesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.insurance.v1.ValidatePolicyRequest;
import com.acme.insurance.v1.ValidatePolicyResponse;
import com.acme.insurance.v1.ValidateUnderwritingRequest;
import com.acme.insurance.v1.ValidateUnderwritingResponse;
import com.acme.media.v1.CountMediaAssetsRequest;
import com.acme.media.v1.CountMediaAssetsResponse;
import com.acme.media.v1.MediaAssetQueryServiceGrpc;
import com.acme.payroll.v1.CalculatePayStubRequest;
import com.acme.payroll.v1.CalculatePayStubResponse;
import com.acme.payroll.v1.CalculateTaxWithholdingRequest;
import com.acme.payroll.v1.CalculateTaxWithholdingResponse;
import com.acme.payroll.v1.ComparePayStubOptionsRequest;
import com.acme.payroll.v1.ComparePayStubOptionsResponse;
import com.acme.payroll.v1.CompareTaxWithholdingOptionsRequest;
import com.acme.payroll.v1.CompareTaxWithholdingOptionsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionCalculationServiceGrpc;
import com.acme.payroll.v1.DeductionCondition;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.EstimatePayStubRequest;
import com.acme.payroll.v1.EstimatePayStubResponse;
import com.acme.payroll.v1.EstimateTaxWithholdingRequest;
import com.acme.payroll.v1.EstimateTaxWithholdingResponse;
import com.acme.payroll.v1.ExplainDeductionCalculationRequest;
import com.acme.payroll.v1.ExplainDeductionCalculationResponse;
import com.acme.payroll.v1.ExplainPayRunCalculationRequest;
import com.acme.payroll.v1.ExplainPayRunCalculationResponse;
import com.acme.payroll.v1.ExplainTaxWithholdingCalculationRequest;
import com.acme.payroll.v1.ExplainTaxWithholdingCalculationResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunValidationServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.PayStubLine;
import com.acme.payroll.v1.PayStubLineTax;
import com.acme.payroll.v1.PreviewPayRunRequest;
import com.acme.payroll.v1.PreviewPayRunResponse;
import com.acme.payroll.v1.QuotePayStubRequest;
import com.acme.payroll.v1.QuotePayStubResponse;
import com.acme.payroll.v1.QuoteTaxWithholdingRequest;
import com.acme.payroll.v1.QuoteTaxWithholdingResponse;
import com.acme.payroll.v1.RecalculateTaxWithholdingsRequest;
import com.acme.payroll.v1.RecalculateTaxWithholdingsResponse;
import com.acme.payroll.v1.SimulatePayStubRequest;
import com.acme.payroll.v1.SimulatePayStubResponse;
import com.acme.payroll.v1.SimulateTaxWithholdingRequest;
import com.acme.payroll.v1.SimulateTaxWithholdingResponse;
import com.acme.payroll.v1.TaxWithholding;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
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
 * Computes the figures of a tax withholding, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class TaxWithholdingCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(TaxWithholdingCalculationApplication.class);

    private final AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking;

    private final AuditTrailAdminServiceGrpc.AuditTrailAdminServiceBlockingStub auditAuditTrailAdminServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking;

    private final ChannelRegistry channels;

    private final UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking;

    private final MediaAssetQueryServiceGrpc.MediaAssetQueryServiceFutureStub mediaMediaAssetQueryServiceFuture;

    private final DeductionCalculationServiceGrpc.DeductionCalculationServiceBlockingStub payrollDeductionCalculationServiceBlocking;

    private final PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking;

    private final PayRunValidationServiceGrpc.PayRunValidationServiceBlockingStub payrollPayRunValidationServiceBlocking;

    private final PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking;

    private final PayrollRepository repository;

    private final Executor rpcExecutor;

    public TaxWithholdingCalculationApplication(
            AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking,
            AuditTrailAdminServiceGrpc.AuditTrailAdminServiceBlockingStub auditAuditTrailAdminServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking,
            ChannelRegistry channels,
            UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking,
            MediaAssetQueryServiceGrpc.MediaAssetQueryServiceFutureStub mediaMediaAssetQueryServiceFuture,
            DeductionCalculationServiceGrpc.DeductionCalculationServiceBlockingStub payrollDeductionCalculationServiceBlocking,
            PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking,
            PayRunValidationServiceGrpc.PayRunValidationServiceBlockingStub payrollPayRunValidationServiceBlocking,
            PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking,
            PayrollRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditAuditEventNotificationServiceBlocking = auditAuditEventNotificationServiceBlocking;
        this.auditAuditTrailAdminServiceBlocking = auditAuditTrailAdminServiceBlocking;
        this.auditRetentionPolicySyncServiceBlocking = auditRetentionPolicySyncServiceBlocking;
        this.channels = channels;
        this.insuranceUnderwritingValidationServiceBlocking = insuranceUnderwritingValidationServiceBlocking;
        this.mediaMediaAssetQueryServiceFuture = mediaMediaAssetQueryServiceFuture;
        this.payrollDeductionCalculationServiceBlocking = payrollDeductionCalculationServiceBlocking;
        this.payrollPayRunCalculationServiceBlocking = payrollPayRunCalculationServiceBlocking;
        this.payrollPayRunValidationServiceBlocking = payrollPayRunValidationServiceBlocking;
        this.payrollPayStubCalculationServiceBlocking = payrollPayStubCalculationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Calculates a tax withholding.
     */
    public CalculateTaxWithholdingResponse calculateTaxWithholding(CalculateTaxWithholdingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateTaxWithholding")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveTaxWithholding(request.getContext().getTenantId(), request.getTaxWithholding());

        var outbound0 = PreviewPayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getTaxWithholding().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .build();
        RpcPort<PreviewPayRunRequest, PreviewPayRunResponse> port0 = RpcPort.adapt(payrollPayRunValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewPayRun);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholding().getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateTaxWithholding", request.getTaxWithholding().getTaxWithholdingId());
        return CalculateTaxWithholdingResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setPriority(request.getTaxWithholding().getPriority())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Estimates a tax withholding.
     */
    public EstimateTaxWithholdingResponse estimateTaxWithholding(EstimateTaxWithholdingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateTaxWithholding")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveTaxWithholding(request.getContext().getTenantId(), request.getTaxWithholding());

        var outbound0 = BeanCopies.copy(SimulatePayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getTaxWithholding().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getTaxWithholding().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .build())
                .build(), SimulatePayStubRequest.class);
        var reply0 = payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulatePayStub(outbound0);

        var outbound1 = BeanCopies.copy(EstimatePayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getTaxWithholding().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(DeductionRef.newBuilder()
                                .setVersion(reply0.getPayStub().getCounterparty().getVersion())
                                .build())
                        .build())
                .build(), EstimatePayStubRequest.class);
        var reply1 = payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimatePayStub(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholding().getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateTaxWithholding", request.getTaxWithholding().getTaxWithholdingId());
        return EstimateTaxWithholdingResponse.newBuilder()
                .setBreakdown(EstimateTaxWithholdingResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply1.getBreakdown().getCurrencyCode())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPayStub().getAudit().getCreatedAt())
                                .build())
                        .setEffective(request.getTaxWithholding().getEffective())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Quotes a tax withholding.
     */
    public QuoteTaxWithholdingResponse quoteTaxWithholding(QuoteTaxWithholdingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteTaxWithholding")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveTaxWithholding(request.getContext().getTenantId(), request.getTaxWithholding());

        var outbound0 = QuotePayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .addLines(PayStubLine.newBuilder()
                                .setTax(PayStubLineTax.newBuilder()
                                        .setAmount(request.getTaxWithholding().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.payroll.v1.PayStubCalculationService");
        var selected0 = PayStubCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.quotePayStub(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholding().getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteTaxWithholding", request.getTaxWithholding().getTaxWithholdingId());
        return QuoteTaxWithholdingResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getPayStub().getAudit().getEtag())
                                .build())
                        .setDescription(request.getTaxWithholding().getDescription())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Compares a tax withholding.
     */
    public CompareTaxWithholdingOptionsResponse compareTaxWithholdingOptions(CompareTaxWithholdingOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareTaxWithholdingOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveTaxWithholding(request.getContext().getTenantId(), request.getTaxWithholding());

        var builder0 = CalculatePayStubRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(CalculatePayStubRequest.Details.newBuilder()
                .setDestination(PostalAddress.newBuilder()
                        .setCountryCode(request.getDetails().getDestination().getCountryCode())
                        .setPostalCode(request.getDetails().getDestination().getPostalCode())
                        .build())
                .build());
        builder0.setPayStub(PayStub.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedBy(request.getTaxWithholding().getAudit().getUpdatedBy())
                        .build())
                .addLines(PayStubLine.newBuilder()
                        .setTax(PayStubLineTax.newBuilder()
                                .setAmount(request.getTaxWithholding().getDiscount().getAmount())
                                .build())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePayStub(outbound0);

        var outbound1 = CountMediaAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountMediaAssetsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getPayStub().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(request.getTaxWithholding().getOwner().getUserId())
                        .setRegion(reply0.getPayStub().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending1 = mediaMediaAssetQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).countMediaAssets(outbound1);
        var reply1 = RpcSupport.await(pending1);
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholding().getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareTaxWithholdingOptions", request.getTaxWithholding().getTaxWithholdingId());
        return CompareTaxWithholdingOptionsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setStatus(request.getTaxWithholding().getStatus())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .setVersion(reply0.getPayStub().getCounterparty().getVersion())
                        .build())
                .build();
    }

    /**
     * Recalculates a tax withholding.
     */
    public RecalculateTaxWithholdingsResponse recalculateTaxWithholdings(RecalculateTaxWithholdingsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateTaxWithholdings")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveTaxWithholding(request.getContext().getTenantId(), request.getTaxWithholding());

        var outbound0 = ExplainPayRunCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .addLines(PayRunLine.newBuilder()
                                .setAmount(request.getTaxWithholding().getDiscount().getAmount())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPayRunCalculation(value));

        var outbound1 = StreamAuditEventNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamAuditEventNoticesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getPayRun().getAudit().getUpdatedAt())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getTaxWithholding().getOwner().getEmail())
                        .build())
                .build();
        var stream1 = auditAuditEventNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditEventNotices(outbound1);
        var reply1 = StreamAuditEventNoticesResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = (ComparePayStubOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .addLines(PayStubLine.newBuilder()
                                .setTax(PayStubLineTax.newBuilder()
                                        .setAmount(request.getTaxWithholding().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePayStubOptions(outbound2);

        var outbound3 = ValidateUnderwritingRequest.newBuilder()
                .setContext(request.getContext())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getTaxWithholding().getOwner().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPayRun().getAudit().getCreatedAt())
                                .build())
                        .addComments(UnderwritingComment.newBuilder()
                                .setCreatedAt(reply2.getPayStub().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        RpcPort<ValidateUnderwritingRequest, ValidateUnderwritingResponse> port3 = RpcPort.adapt(insuranceUnderwritingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::validateUnderwriting);
        var reply3 = port3.exchange(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholding().getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateTaxWithholdings", request.getTaxWithholding().getTaxWithholdingId());
        return RecalculateTaxWithholdingsResponse.newBuilder()
                .setBreakdown(RecalculateTaxWithholdingsResponse.Breakdown.newBuilder()
                        .setTotal(reply2.getPayStub().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPayRun().getAudit().getCreatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply3.getUnderwriting().getAssignee().getEmail())
                                .build())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .setVersion(request.getTaxWithholding().getVersion())
                        .build())
                .build();
    }

    /**
     * Simulates a tax withholding.
     */
    public SimulateTaxWithholdingResponse simulateTaxWithholding(SimulateTaxWithholdingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateTaxWithholding")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveTaxWithholding(request.getContext().getTenantId(), request.getTaxWithholding());

        var outbound0 = PushRetentionPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PushRetentionPolicyRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setDiscount(RetentionPolicyDiscountAction.newBuilder()
                                .setPercent(request.getTaxWithholding().getDiscount().getPercent())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(auditRetentionPolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushRetentionPolicy(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = CalculatePayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculatePayStubRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getTaxWithholding().getAudit().getCreatedBy())
                                .build())
                        .addLines(PayStubLine.newBuilder()
                                .setTax(PayStubLineTax.newBuilder()
                                        .setAmount(reply0.getRetentionPolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.payroll.v1.PayStubCalculationService/CalculatePayStub", outbound1);
        var reply1 = payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePayStub(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholding().getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateTaxWithholding", request.getTaxWithholding().getTaxWithholdingId());
        return SimulateTaxWithholdingResponse.newBuilder()
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getPayStub().getAudit().getCreatedBy())
                                .setUpdatedBy(reply1.getPayStub().getAudit().getUpdatedBy())
                                .build())
                        .setDescription(reply0.getRetentionPolicy().getDescription())
                        .setStatus(request.getTaxWithholding().getStatus())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Explains a tax withholding.
     */
    public ExplainTaxWithholdingCalculationResponse explainTaxWithholdingCalculation(ExplainTaxWithholdingCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainTaxWithholdingCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveTaxWithholding(request.getContext().getTenantId(), request.getTaxWithholding());

        var outbound0 = ValidatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getTaxWithholding().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getTaxWithholding().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.insurance.v1.PolicyValidationService");
        var serviceName0 = "acme.insurance.v1" + "." + "PolicyValidationService";
        var methodName0 = "Val" + "idatePolicy";
        var descriptor0 = MethodDescriptor.<ValidatePolicyRequest, ValidatePolicyResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ValidatePolicyRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ValidatePolicyResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var futures1 = request.getTaxWithholding().getConditionsList().stream()
                .map(element -> ExplainDeductionCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDeduction(Deduction.newBuilder()
                        .addConditions(DeductionCondition.newBuilder()
                                .setNegate(element.getNegate())
                                .setOperator(element.getOperator())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> payrollDeductionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainDeductionCalculation(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? ExplainDeductionCalculationResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = RepairAuditTrailRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getPolicy().getAudit().getCreatedBy())
                                .build())
                        .setBank(AuditTrailBankSettlement.newBuilder()
                                .setIban(switch (reply0.getPolicy().getFundingCase()) {
                                    case BANK -> reply0.getPolicy().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(RetentionPolicyRef.newBuilder()
                                .setDisplayName(reply1.getDeduction().getOwner().getDisplayName())
                                .build())
                        .addLines(AuditTrailLine.newBuilder()
                                .setAmount(request.getTaxWithholding().getDiscount().getAmount())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RepairAuditTrailRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply2 = auditAuditTrailAdminServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).repairAuditTrail(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholding().getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainTaxWithholdingCalculation", request.getTaxWithholding().getTaxWithholdingId());
        return ExplainTaxWithholdingCalculationResponse.newBuilder()
                .setOutcome(ExplainTaxWithholdingCalculationResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply2.getOutcome().getStatus())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setOwner(request.getTaxWithholding().getOwner())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .setVersion(reply2.getAuditTrail().getCounterparty().getVersion())
                        .build())
                .build();
    }
}
