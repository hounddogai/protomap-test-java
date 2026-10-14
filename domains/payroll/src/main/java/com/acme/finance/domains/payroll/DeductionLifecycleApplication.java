package com.acme.finance.domains.payroll;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.ValidateAuditEventRequest;
import com.acme.audit.v1.ValidateAuditEventResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ListUnderwritingImportsRequest;
import com.acme.insurance.v1.ListUnderwritingImportsResponse;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.keymanagement.v1.CryptoKeyServiceGrpc;
import com.acme.keymanagement.v1.WatchCryptoKeysRequest;
import com.acme.keymanagement.v1.WatchCryptoKeysResponse;
import com.acme.lending.v1.CommentRepaymentApprovalRequest;
import com.acme.lending.v1.CommentRepaymentApprovalResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentApprovalServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.payroll.v1.ActivateDeductionRequest;
import com.acme.payroll.v1.ActivateDeductionResponse;
import com.acme.payroll.v1.CheckDeductionEligibilityRequest;
import com.acme.payroll.v1.CheckDeductionEligibilityResponse;
import com.acme.payroll.v1.ComparePayStubOptionsRequest;
import com.acme.payroll.v1.ComparePayStubOptionsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionCalculationServiceGrpc;
import com.acme.payroll.v1.DeductionValidationServiceGrpc;
import com.acme.payroll.v1.FindPayRunsRequest;
import com.acme.payroll.v1.FindPayRunsResponse;
import com.acme.payroll.v1.GetDeductionLifecycleRequest;
import com.acme.payroll.v1.GetDeductionLifecycleResponse;
import com.acme.payroll.v1.GetDeductionRatesRequest;
import com.acme.payroll.v1.GetDeductionRatesResponse;
import com.acme.payroll.v1.PayRunQueryServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.ReinstateDeductionRequest;
import com.acme.payroll.v1.ReinstateDeductionResponse;
import com.acme.payroll.v1.RenewDeductionRequest;
import com.acme.payroll.v1.RenewDeductionResponse;
import com.acme.payroll.v1.ScheduleDeductionExpiryRequest;
import com.acme.payroll.v1.ScheduleDeductionExpiryResponse;
import com.acme.payroll.v1.ScheduleTaxWithholdingReportRequest;
import com.acme.payroll.v1.ScheduleTaxWithholdingReportResponse;
import com.acme.payroll.v1.ShareTaxWithholdingReportRequest;
import com.acme.payroll.v1.ShareTaxWithholdingReportResponse;
import com.acme.payroll.v1.SuspendDeductionRequest;
import com.acme.payroll.v1.SuspendDeductionResponse;
import com.acme.payroll.v1.TaxWithholdingReportingServiceGrpc;
import com.acme.payroll.v1.TerminateDeductionRequest;
import com.acme.payroll.v1.TerminateDeductionResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Moves a deduction between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class DeductionLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(DeductionLifecycleApplication.class);

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final UnderwritingImportServiceGrpc.UnderwritingImportServiceBlockingStub insuranceUnderwritingImportServiceBlocking;

    private final CryptoKeyServiceGrpc.CryptoKeyServiceBlockingStub keymanagementCryptoKeyServiceBlocking;

    private final RepaymentApprovalServiceGrpc.RepaymentApprovalServiceBlockingStub lendingRepaymentApprovalServiceBlocking;

    private final DeductionCalculationServiceGrpc.DeductionCalculationServiceBlockingStub payrollDeductionCalculationServiceBlocking;

    private final DeductionValidationServiceGrpc.DeductionValidationServiceBlockingStub payrollDeductionValidationServiceBlocking;

    private final PayRunQueryServiceGrpc.PayRunQueryServiceBlockingStub payrollPayRunQueryServiceBlocking;

    private final TaxWithholdingReportingServiceGrpc.TaxWithholdingReportingServiceBlockingStub payrollTaxWithholdingReportingServiceBlocking;

    private final PayrollRepository repository;

    public DeductionLifecycleApplication(
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            ChannelRegistry channels,
            UnderwritingImportServiceGrpc.UnderwritingImportServiceBlockingStub insuranceUnderwritingImportServiceBlocking,
            CryptoKeyServiceGrpc.CryptoKeyServiceBlockingStub keymanagementCryptoKeyServiceBlocking,
            RepaymentApprovalServiceGrpc.RepaymentApprovalServiceBlockingStub lendingRepaymentApprovalServiceBlocking,
            DeductionCalculationServiceGrpc.DeductionCalculationServiceBlockingStub payrollDeductionCalculationServiceBlocking,
            DeductionValidationServiceGrpc.DeductionValidationServiceBlockingStub payrollDeductionValidationServiceBlocking,
            PayRunQueryServiceGrpc.PayRunQueryServiceBlockingStub payrollPayRunQueryServiceBlocking,
            TaxWithholdingReportingServiceGrpc.TaxWithholdingReportingServiceBlockingStub payrollTaxWithholdingReportingServiceBlocking,
            PayrollRepository repository) {
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.channels = channels;
        this.insuranceUnderwritingImportServiceBlocking = insuranceUnderwritingImportServiceBlocking;
        this.keymanagementCryptoKeyServiceBlocking = keymanagementCryptoKeyServiceBlocking;
        this.lendingRepaymentApprovalServiceBlocking = lendingRepaymentApprovalServiceBlocking;
        this.payrollDeductionCalculationServiceBlocking = payrollDeductionCalculationServiceBlocking;
        this.payrollDeductionValidationServiceBlocking = payrollDeductionValidationServiceBlocking;
        this.payrollPayRunQueryServiceBlocking = payrollPayRunQueryServiceBlocking;
        this.payrollTaxWithholdingReportingServiceBlocking = payrollTaxWithholdingReportingServiceBlocking;
        this.repository = repository;
    }

    /**
     * Activates a deduction.
     */
    public ActivateDeductionResponse activateDeduction(ActivateDeductionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateDeduction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Deduction", request.getDeductionId(), "ActivateDeduction");

        var builder0 = GetDeductionRatesRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDeductionId(request.getDeductionId());
        var outbound0 = builder0.build();
        var reply0 = payrollDeductionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getDeductionRates(outbound0);

        var outbound1 = FindPayRunsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindPayRunsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getDeduction().getAudit().getCreatedAt())
                        .setOwnerId(reply0.getDeduction().getOwner().getUserId())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(payrollPayRunQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findPayRuns(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateDeduction", request.getDetails().getReasonCode());
        return ActivateDeductionResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getDeduction().getOwner().getDisplayName())
                                .build())
                        .build())
                .setOutcome(ActivateDeductionResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Suspends a deduction.
     */
    public SuspendDeductionResponse suspendDeduction(SuspendDeductionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendDeduction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Deduction", request.getDeductionId(), "SuspendDeduction");

        var outbound0 = (ValidateAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(ValidateAuditEventRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateAuditEvent(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendDeduction", request.getDetails().getReasonCode());
        return SuspendDeductionResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .build())
                .setOutcome(SuspendDeductionResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Reinstates a deduction.
     */
    public ReinstateDeductionResponse reinstateDeduction(ReinstateDeductionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateDeduction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Deduction", request.getDeductionId(), "ReinstateDeduction");

        var outbound0 = CommentRepaymentApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getNotifyContact().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> lendingRepaymentApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentRepaymentApproval(value)).orElseThrow();

        var outbound1 = ComparePayStubOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ComparePayStubOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getRepayment().getLocation().getCountryCode())
                                .addAllLines(reply0.getRepayment().getLocation().getLinesList())
                                .build())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getRepayment().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.payroll.v1.PayStubCalculationService");
        var selected1 = PayStubCalculationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.comparePayStubOptions(outbound1);

        var outbound2 = CheckDeductionEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getPayStub().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getPayStub().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        RpcPort<CheckDeductionEligibilityRequest, CheckDeductionEligibilityResponse> port2 = RpcPort.adapt(payrollDeductionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::checkDeductionEligibility);
        var reply2 = port2.exchange(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateDeduction", request.getDetails().getReasonCode());
        return ReinstateDeductionResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getRepayment().getAudit().getEtag())
                                .build())
                        .setDeductionId(request.getDeductionId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getPayStub().getCounterparty().getDisplayName())
                                .build())
                        .setVersion(reply1.getPayStub().getCounterparty().getVersion())
                        .build())
                .setOutcome(ReinstateDeductionResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setReferenceId(reply2.getOutcome().getReferenceId())
                        .build())
                .build();
    }

    /**
     * Terminates a deduction.
     */
    public TerminateDeductionResponse terminateDeduction(TerminateDeductionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateDeduction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Deduction", request.getDeductionId(), "TerminateDeduction");

        var outbound0 = ListUnderwritingImportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListUnderwritingImportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var adapter0 = new Function<ListUnderwritingImportsRequest, ListUnderwritingImportsResponse>() {
            @Override
            public ListUnderwritingImportsResponse apply(ListUnderwritingImportsRequest input) {
                return insuranceUnderwritingImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listUnderwritingImports(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateDeduction", request.getDetails().getReasonCode());
        return TerminateDeductionResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .build())
                .setOutcome(TerminateDeductionResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Renews a deduction.
     */
    public RenewDeductionResponse renewDeduction(RenewDeductionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewDeduction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Deduction", request.getDeductionId(), "RenewDeduction");

        var outbound0 = ScheduleTaxWithholdingReportRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var channel0 = channels.lookup("acme.payroll.v1.TaxWithholdingReportingService");
        var serviceName0 = "acme.payroll.v1" + "." + "TaxWithholdingReportingService";
        var methodName0 = "Sch" + "eduleTaxWithholdingReport";
        var descriptor0 = MethodDescriptor.<ScheduleTaxWithholdingReportRequest, ScheduleTaxWithholdingReportResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ScheduleTaxWithholdingReportRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ScheduleTaxWithholdingReportResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = ShareTaxWithholdingReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareTaxWithholdingReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> payrollTaxWithholdingReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareTaxWithholdingReport(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewDeduction", request.getDetails().getReasonCode());
        return RenewDeductionResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .setOwner(reply1.getTaxWithholding().getOwner())
                        .build())
                .setOutcome(RenewDeductionResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns a deduction.
     */
    public GetDeductionLifecycleResponse getDeductionLifecycle(GetDeductionLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetDeductionLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Deduction", request.getDeductionId(), "GetDeductionLifecycle");
        return GetDeductionLifecycleResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .build())
                .build();
    }

    /**
     * Schedules a deduction.
     */
    public ScheduleDeductionExpiryResponse scheduleDeductionExpiry(ScheduleDeductionExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleDeductionExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = WatchCryptoKeysRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setPhone(request.getDetails().getNotify().getPhone())
                        .build())
                .build();
        var stream0 = keymanagementCryptoKeyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchCryptoKeys(outbound0);
        var reply0 = WatchCryptoKeysResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleDeductionExpiry", request.getDeduction().getDeductionId());
        return ScheduleDeductionExpiryResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .setEffective(request.getDeduction().getEffective())
                        .build())
                .build();
    }
}
