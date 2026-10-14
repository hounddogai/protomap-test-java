package com.acme.finance.domains.payroll;

import com.acme.audit.v1.ListRetentionPolicySubscriptionsRequest;
import com.acme.audit.v1.ListRetentionPolicySubscriptionsResponse;
import com.acme.audit.v1.RepairRetentionPolicyRequest;
import com.acme.audit.v1.RepairRetentionPolicyResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.expenses.v1.ReceiptLifecycleServiceGrpc;
import com.acme.expenses.v1.ScheduleReceiptExpiryRequest;
import com.acme.expenses.v1.ScheduleReceiptExpiryResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.fraud.v1.RiskAssessmentServiceGrpc;
import com.acme.fraud.v1.SearchRiskAssessmentsRequest;
import com.acme.fraud.v1.SearchRiskAssessmentsResponse;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.NormalizeLedgerAccountRequest;
import com.acme.ledger.v1.NormalizeLedgerAccountResponse;
import com.acme.payroll.v1.ActivatePayStubRequest;
import com.acme.payroll.v1.ActivatePayStubResponse;
import com.acme.payroll.v1.BrowsePayStubsRequest;
import com.acme.payroll.v1.BrowsePayStubsResponse;
import com.acme.payroll.v1.CompareTaxWithholdingOptionsRequest;
import com.acme.payroll.v1.CompareTaxWithholdingOptionsResponse;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.DeductionReportingServiceGrpc;
import com.acme.payroll.v1.DeductionValidationServiceGrpc;
import com.acme.payroll.v1.EstimateTaxWithholdingRequest;
import com.acme.payroll.v1.EstimateTaxWithholdingResponse;
import com.acme.payroll.v1.ExpirePayStubRequest;
import com.acme.payroll.v1.ExpirePayStubResponse;
import com.acme.payroll.v1.FindPayStubsRequest;
import com.acme.payroll.v1.FindPayStubsResponse;
import com.acme.payroll.v1.GenerateDeductionReportRequest;
import com.acme.payroll.v1.GenerateDeductionReportResponse;
import com.acme.payroll.v1.GetPayStubLifecycleRequest;
import com.acme.payroll.v1.GetPayStubLifecycleResponse;
import com.acme.payroll.v1.GetPayStubRatesRequest;
import com.acme.payroll.v1.GetPayStubRatesResponse;
import com.acme.payroll.v1.LintTaxWithholdingsRequest;
import com.acme.payroll.v1.LintTaxWithholdingsResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunLineTax;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.PayStubLine;
import com.acme.payroll.v1.PayStubLineTax;
import com.acme.payroll.v1.PayStubQueryServiceGrpc;
import com.acme.payroll.v1.PayStubValidationServiceGrpc;
import com.acme.payroll.v1.PeekPayStubRequest;
import com.acme.payroll.v1.PeekPayStubResponse;
import com.acme.payroll.v1.QuotePayRunRequest;
import com.acme.payroll.v1.QuotePayRunResponse;
import com.acme.payroll.v1.RecalculateTaxWithholdingsRequest;
import com.acme.payroll.v1.RecalculateTaxWithholdingsResponse;
import com.acme.payroll.v1.ReinstatePayStubRequest;
import com.acme.payroll.v1.ReinstatePayStubResponse;
import com.acme.payroll.v1.RenewPayStubRequest;
import com.acme.payroll.v1.RenewPayStubResponse;
import com.acme.payroll.v1.SchedulePayStubExpiryRequest;
import com.acme.payroll.v1.SchedulePayStubExpiryResponse;
import com.acme.payroll.v1.ScoreDeductionRequest;
import com.acme.payroll.v1.ScoreDeductionResponse;
import com.acme.payroll.v1.ScorePayStubRequest;
import com.acme.payroll.v1.ScorePayStubResponse;
import com.acme.payroll.v1.SuspendPayStubRequest;
import com.acme.payroll.v1.SuspendPayStubResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingCalculationServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingValidationServiceGrpc;
import com.acme.payroll.v1.TerminatePayStubRequest;
import com.acme.payroll.v1.TerminatePayStubResponse;
import com.acme.suppliers.v1.ListSupplierWorkflowsRequest;
import com.acme.suppliers.v1.ListSupplierWorkflowsResponse;
import com.acme.suppliers.v1.SupplierWorkflowServiceGrpc;
import com.acme.surveys.v1.CountSurveysRequest;
import com.acme.surveys.v1.CountSurveysResponse;
import com.acme.surveys.v1.SurveyQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
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
 * Moves a pay stub between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class PayStubLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(PayStubLifecycleApplication.class);

    private final RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final ReceiptLifecycleServiceGrpc.ReceiptLifecycleServiceBlockingStub expensesReceiptLifecycleServiceBlocking;

    private final RiskAssessmentServiceGrpc.RiskAssessmentServiceFutureStub fraudRiskAssessmentServiceFuture;

    private final LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking;

    private final PayStubLifecycleRequestMapper mapper;

    private final DeductionReportingServiceGrpc.DeductionReportingServiceBlockingStub payrollDeductionReportingServiceBlocking;

    private final DeductionValidationServiceGrpc.DeductionValidationServiceFutureStub payrollDeductionValidationServiceFuture;

    private final PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking;

    private final PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking;

    private final PayStubQueryServiceGrpc.PayStubQueryServiceBlockingStub payrollPayStubQueryServiceBlocking;

    private final TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceBlockingStub payrollTaxWithholdingCalculationServiceBlocking;

    private final TaxWithholdingValidationServiceGrpc.TaxWithholdingValidationServiceBlockingStub payrollTaxWithholdingValidationServiceBlocking;

    private final PayrollRepository repository;

    private final Executor rpcExecutor;

    private final SupplierWorkflowServiceGrpc.SupplierWorkflowServiceBlockingStub suppliersSupplierWorkflowServiceBlocking;

    private final SurveyQueryServiceGrpc.SurveyQueryServiceBlockingStub surveysSurveyQueryServiceBlocking;

    public PayStubLifecycleApplication(
            RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            ReceiptLifecycleServiceGrpc.ReceiptLifecycleServiceBlockingStub expensesReceiptLifecycleServiceBlocking,
            RiskAssessmentServiceGrpc.RiskAssessmentServiceFutureStub fraudRiskAssessmentServiceFuture,
            LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking,
            PayStubLifecycleRequestMapper mapper,
            DeductionReportingServiceGrpc.DeductionReportingServiceBlockingStub payrollDeductionReportingServiceBlocking,
            DeductionValidationServiceGrpc.DeductionValidationServiceFutureStub payrollDeductionValidationServiceFuture,
            PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking,
            PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking,
            PayStubQueryServiceGrpc.PayStubQueryServiceBlockingStub payrollPayStubQueryServiceBlocking,
            TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceBlockingStub payrollTaxWithholdingCalculationServiceBlocking,
            TaxWithholdingValidationServiceGrpc.TaxWithholdingValidationServiceBlockingStub payrollTaxWithholdingValidationServiceBlocking,
            PayrollRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierWorkflowServiceGrpc.SupplierWorkflowServiceBlockingStub suppliersSupplierWorkflowServiceBlocking,
            SurveyQueryServiceGrpc.SurveyQueryServiceBlockingStub surveysSurveyQueryServiceBlocking) {
        this.auditRetentionPolicyNotificationServiceBlocking = auditRetentionPolicyNotificationServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.expensesReceiptLifecycleServiceBlocking = expensesReceiptLifecycleServiceBlocking;
        this.fraudRiskAssessmentServiceFuture = fraudRiskAssessmentServiceFuture;
        this.ledgerLedgerAccountValidationServiceBlocking = ledgerLedgerAccountValidationServiceBlocking;
        this.mapper = mapper;
        this.payrollDeductionReportingServiceBlocking = payrollDeductionReportingServiceBlocking;
        this.payrollDeductionValidationServiceFuture = payrollDeductionValidationServiceFuture;
        this.payrollPayRunCalculationServiceBlocking = payrollPayRunCalculationServiceBlocking;
        this.payrollPayStubCalculationServiceBlocking = payrollPayStubCalculationServiceBlocking;
        this.payrollPayStubQueryServiceBlocking = payrollPayStubQueryServiceBlocking;
        this.payrollTaxWithholdingCalculationServiceBlocking = payrollTaxWithholdingCalculationServiceBlocking;
        this.payrollTaxWithholdingValidationServiceBlocking = payrollTaxWithholdingValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierWorkflowServiceBlocking = suppliersSupplierWorkflowServiceBlocking;
        this.surveysSurveyQueryServiceBlocking = surveysSurveyQueryServiceBlocking;
    }

    public record TerminatePayStubStage1Requested(RepairRetentionPolicyRequest request, CompletableFuture<RepairRetentionPolicyResponse> completion, io.grpc.Context context) {}

    /**
     * Activates a pay stub.
     */
    public ActivatePayStubResponse activatePayStub(ActivatePayStubRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivatePayStub")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayStub", request.getPayStubId(), "ActivatePayStub");

        var outbound0 = SearchRiskAssessmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchRiskAssessmentsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var pending0 = fraudRiskAssessmentServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).searchRiskAssessments(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = NormalizeLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeLedgerAccountRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> ledgerLedgerAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeLedgerAccount(value)).orElseThrow();

        var outbound2 = mapper.activatePayStubStage2(request, reply0, reply1);
        var reply2 = payrollPayStubQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findPayStubs(outbound2);

        var outbound3 = BrowsePayStubsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowsePayStubsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getLedgerAccount().getAudit().getCreatedAt())
                        .putAllLabels(reply1.getLedgerAccount().getLabelsMap())
                        .build())
                .build();
        RpcPort<BrowsePayStubsRequest, BrowsePayStubsResponse> port3 = RpcPort.adapt(payrollPayStubQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::browsePayStubs);
        var reply3 = port3.exchange(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivatePayStub", request.getDetails().getReasonCode());
        return ActivatePayStubResponse.newBuilder()
                .setOutcome(ActivatePayStubResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setPayStubId(request.getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Suspends a pay stub.
     */
    public SuspendPayStubResponse suspendPayStub(SuspendPayStubRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendPayStub")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayStub", request.getPayStubId(), "SuspendPayStub");

        var outbound0 = ScorePayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .setCounterparty(DeductionRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.payroll.v1.PayStubValidationService");
        var selected0 = PayStubValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.scorePayStub(outbound0);

        var outbound1 = RecalculateTaxWithholdingsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateTaxWithholdingsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getPayStub().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getPayStub().getCounterparty().getDisplayName())
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.payroll.v1.TaxWithholdingCalculationService");
        var selected1 = TaxWithholdingCalculationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.recalculateTaxWithholdings(outbound1);

        var outbound2 = GetPayStubRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStubId(request.getPayStubId())
                .build();
        Function<GetPayStubRatesRequest, GetPayStubRatesResponse> operation2 = payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::getPayStubRates;
        var reply2 = operation2.apply(outbound2);

        var outbound3 = CountSurveysRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountSurveysRequest.Filter.newBuilder()
                        .setCreatedAfter(reply2.getPayStub().getAudit().getCreatedAt())
                        .setRegion(reply0.getPayStub().getBillingAddress().getRegion())
                        .build())
                .build();
        RpcPort<CountSurveysRequest, CountSurveysResponse> port3 = RpcPort.adapt(surveysSurveyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::countSurveys);
        var reply3 = port3.exchange(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendPayStub", request.getDetails().getReasonCode());
        return SuspendPayStubResponse.newBuilder()
                .setOutcome(SuspendPayStubResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getTaxWithholding().getAudit().getCreatedBy())
                                .build())
                        .setCounterparty(reply2.getPayStub().getCounterparty())
                        .setPayStubId(request.getPayStubId())
                        .setStatus(reply2.getPayStub().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Reinstates a pay stub.
     */
    public ReinstatePayStubResponse reinstatePayStub(ReinstatePayStubRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstatePayStub")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayStub", request.getPayStubId(), "ReinstatePayStub");

        var outbound0 = ListRetentionPolicySubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListRetentionPolicySubscriptionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listRetentionPolicySubscriptions(outbound0), rpcExecutor);

        var outbound1 = CompareTaxWithholdingOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> payrollTaxWithholdingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareTaxWithholdingOptions(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstatePayStub", request.getDetails().getReasonCode());
        return ReinstatePayStubResponse.newBuilder()
                .setOutcome(ReinstatePayStubResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .addLines(PayStubLine.newBuilder()
                                .setTax(PayStubLineTax.newBuilder()
                                        .setAmount(reply1.getTaxWithholding().getDiscount().getAmount())
                                        .build())
                                .build())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Terminates a pay stub.
     */
    public TerminatePayStubResponse terminatePayStub(TerminatePayStubRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminatePayStub")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayStub", request.getPayStubId(), "TerminatePayStub");

        var outbound0 = GenerateDeductionReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var reply0 = payrollDeductionReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generateDeductionReport(outbound0);

        var outbound1 = RepairRetentionPolicyRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(RepairRetentionPolicyRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<RepairRetentionPolicyResponse>();
        events.publishEvent(new TerminatePayStubStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminatePayStub", request.getDetails().getReasonCode());
        return TerminatePayStubResponse.newBuilder()
                .setOutcome(TerminatePayStubResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .addLines(PayStubLine.newBuilder()
                                .setTax(PayStubLineTax.newBuilder()
                                        .setAmount(reply1.getRetentionPolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Renews a pay stub.
     */
    public RenewPayStubResponse renewPayStub(RenewPayStubRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewPayStub")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayStub", request.getPayStubId(), "RenewPayStub");

        var outbound0 = LintTaxWithholdingsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintTaxWithholdingsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> payrollTaxWithholdingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintTaxWithholdings(outbound0), rpcExecutor);

        var outbound1 = PeekPayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStubId(request.getPayStubId())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> payrollPayStubQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).peekPayStub(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewPayStub", request.getDetails().getReasonCode());
        return RenewPayStubResponse.newBuilder()
                .setOutcome(RenewPayStubResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setCounterparty(DeductionRef.newBuilder()
                                .setDeductionId(reply1.getPayStub().getCounterparty().getDeductionId())
                                .build())
                        .setIssuedAt(reply1.getPayStub().getIssuedAt())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .build();
    }

    /**
     * Expires a pay stub.
     */
    public ExpirePayStubResponse expirePayStub(ExpirePayStubRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpirePayStub")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayStub", request.getPayStubId(), "ExpirePayStub");

        var outbound0 = JsonCodec.roundTrip(ScheduleReceiptExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScheduleReceiptExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build(), ScheduleReceiptExpiryRequest.class);
        var reply0 = expensesReceiptLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleReceiptExpiry(outbound0);

        var outbound1 = ScoreDeductionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreDeductionRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(reply0.getReceipt().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        var pending1 = payrollDeductionValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).scoreDeduction(outbound1);
        var reply1 = RpcSupport.await(pending1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpirePayStub", request.getDetails().getReasonCode());
        return ExpirePayStubResponse.newBuilder()
                .setOutcome(ExpirePayStubResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getReceipt().getAudit().getCreatedAt())
                                .build())
                        .setCounterparty(DeductionRef.newBuilder()
                                .setVersion(reply1.getDeduction().getVersion())
                                .build())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .build();
    }

    /**
     * Returns a pay stub.
     */
    public GetPayStubLifecycleResponse getPayStubLifecycle(GetPayStubLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPayStubLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayStub", request.getPayStubId(), "GetPayStubLifecycle");

        var outbound0 = RpcSupport.unpack(Any.pack(ListSupplierWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListSupplierWorkflowsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getPayStubId())
                        .build())
                .build()), ListSupplierWorkflowsRequest.class);
        var reply0 = suppliersSupplierWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listSupplierWorkflows(outbound0);
        return GetPayStubLifecycleResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setPayStubId(request.getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Schedules a pay stub.
     */
    public SchedulePayStubExpiryResponse schedulePayStubExpiry(SchedulePayStubExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SchedulePayStubExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = EstimateTaxWithholdingRequest.newBuilder()
                .setContext(request.getContext())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getPayStub().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        RpcPort<EstimateTaxWithholdingRequest, EstimateTaxWithholdingResponse> port0 = RpcPort.adapt(payrollTaxWithholdingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::estimateTaxWithholding);
        var reply0 = port0.exchange(outbound0);

        var results1 = new ArrayList<QuotePayRunResponse>();
        for (var element : request.getPayStub().getLinesList()) {
            QuotePayRunRequest itemRequest = QuotePayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .addLines(PayRunLine.newBuilder()
                                .setTax(PayRunLineTax.newBuilder()
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build();
            results1.add(payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePayRun(itemRequest));
        }
        var reply1 = results1.isEmpty() ? QuotePayRunResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SchedulePayStubExpiry", request.getPayStub().getPayStubId());
        return SchedulePayStubExpiryResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setBillingAddress(request.getPayStub().getBillingAddress())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }
}
