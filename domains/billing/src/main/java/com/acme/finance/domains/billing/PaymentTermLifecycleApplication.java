package com.acme.finance.domains.billing;

import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.NormalizeAuditTrailRequest;
import com.acme.audit.v1.NormalizeAuditTrailResponse;
import com.acme.billing.v1.ActivatePaymentTermRequest;
import com.acme.billing.v1.ActivatePaymentTermResponse;
import com.acme.billing.v1.CalculateCreditNoteRequest;
import com.acme.billing.v1.CalculateCreditNoteResponse;
import com.acme.billing.v1.CalculatePaymentTermRequest;
import com.acme.billing.v1.CalculatePaymentTermResponse;
import com.acme.billing.v1.CompareCreditNoteOptionsRequest;
import com.acme.billing.v1.CompareCreditNoteOptionsResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteCalculationServiceGrpc;
import com.acme.billing.v1.CreditNoteValidationServiceGrpc;
import com.acme.billing.v1.DunningReportingServiceGrpc;
import com.acme.billing.v1.ExpirePaymentTermRequest;
import com.acme.billing.v1.ExpirePaymentTermResponse;
import com.acme.billing.v1.GenerateDunningReportRequest;
import com.acme.billing.v1.GenerateDunningReportResponse;
import com.acme.billing.v1.GetPaymentTermLifecycleRequest;
import com.acme.billing.v1.GetPaymentTermLifecycleResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceLine;
import com.acme.billing.v1.InvoiceLineTax;
import com.acme.billing.v1.InvoiceQueryServiceGrpc;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.InvoiceValidationServiceGrpc;
import com.acme.billing.v1.NormalizeCreditNoteRequest;
import com.acme.billing.v1.NormalizeCreditNoteResponse;
import com.acme.billing.v1.NormalizeInvoiceRequest;
import com.acme.billing.v1.NormalizeInvoiceResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermCalculationServiceGrpc;
import com.acme.billing.v1.ReinstatePaymentTermRequest;
import com.acme.billing.v1.ReinstatePaymentTermResponse;
import com.acme.billing.v1.RenewPaymentTermRequest;
import com.acme.billing.v1.RenewPaymentTermResponse;
import com.acme.billing.v1.ResolveInvoicesRequest;
import com.acme.billing.v1.ResolveInvoicesResponse;
import com.acme.billing.v1.SchedulePaymentTermExpiryRequest;
import com.acme.billing.v1.SchedulePaymentTermExpiryResponse;
import com.acme.billing.v1.ScoreInvoiceRequest;
import com.acme.billing.v1.ScoreInvoiceResponse;
import com.acme.billing.v1.ShareDunningReportRequest;
import com.acme.billing.v1.ShareDunningReportResponse;
import com.acme.billing.v1.SuspendPaymentTermRequest;
import com.acme.billing.v1.SuspendPaymentTermResponse;
import com.acme.billing.v1.TerminatePaymentTermRequest;
import com.acme.billing.v1.TerminatePaymentTermResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.ContractClauseApprovalServiceGrpc;
import com.acme.contracts.v1.DelegateContractClauseApprovalRequest;
import com.acme.contracts.v1.DelegateContractClauseApprovalResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.expenses.v1.MileageClaimRef;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptApprovalServiceGrpc;
import com.acme.expenses.v1.SubmitReceiptRequest;
import com.acme.expenses.v1.SubmitReceiptResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.metrics.v1.PreviewMetricSeriesRequest;
import com.acme.metrics.v1.PreviewMetricSeriesResponse;
import com.acme.onboarding.v1.ListOnboardingPlansRequest;
import com.acme.onboarding.v1.ListOnboardingPlansResponse;
import com.acme.onboarding.v1.OnboardingPlanServiceGrpc;
import com.google.protobuf.Any;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Moves a payment term between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class PaymentTermLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(PaymentTermLifecycleApplication.class);

    private final AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking;

    private final CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceBlockingStub billingCreditNoteCalculationServiceBlocking;

    private final CreditNoteValidationServiceGrpc.CreditNoteValidationServiceBlockingStub billingCreditNoteValidationServiceBlocking;

    private final DunningReportingServiceGrpc.DunningReportingServiceBlockingStub billingDunningReportingServiceBlocking;

    private final InvoiceQueryServiceGrpc.InvoiceQueryServiceBlockingStub billingInvoiceQueryServiceBlocking;

    private final InvoiceValidationServiceGrpc.InvoiceValidationServiceBlockingStub billingInvoiceValidationServiceBlocking;

    private final PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceBlockingStub billingPaymentTermCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final ContractClauseApprovalServiceGrpc.ContractClauseApprovalServiceBlockingStub contractsContractClauseApprovalServiceBlocking;

    private final ReceiptApprovalServiceGrpc.ReceiptApprovalServiceBlockingStub expensesReceiptApprovalServiceBlocking;

    private final MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking;

    private final OnboardingPlanServiceGrpc.OnboardingPlanServiceBlockingStub onboardingOnboardingPlanServiceBlocking;

    private final BillingRepository repository;

    private final Executor rpcExecutor;

    public PaymentTermLifecycleApplication(
            AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking,
            CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceBlockingStub billingCreditNoteCalculationServiceBlocking,
            CreditNoteValidationServiceGrpc.CreditNoteValidationServiceBlockingStub billingCreditNoteValidationServiceBlocking,
            DunningReportingServiceGrpc.DunningReportingServiceBlockingStub billingDunningReportingServiceBlocking,
            InvoiceQueryServiceGrpc.InvoiceQueryServiceBlockingStub billingInvoiceQueryServiceBlocking,
            InvoiceValidationServiceGrpc.InvoiceValidationServiceBlockingStub billingInvoiceValidationServiceBlocking,
            PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceBlockingStub billingPaymentTermCalculationServiceBlocking,
            ChannelRegistry channels,
            ContractClauseApprovalServiceGrpc.ContractClauseApprovalServiceBlockingStub contractsContractClauseApprovalServiceBlocking,
            ReceiptApprovalServiceGrpc.ReceiptApprovalServiceBlockingStub expensesReceiptApprovalServiceBlocking,
            MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking,
            OnboardingPlanServiceGrpc.OnboardingPlanServiceBlockingStub onboardingOnboardingPlanServiceBlocking,
            BillingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditAuditTrailValidationServiceBlocking = auditAuditTrailValidationServiceBlocking;
        this.billingCreditNoteCalculationServiceBlocking = billingCreditNoteCalculationServiceBlocking;
        this.billingCreditNoteValidationServiceBlocking = billingCreditNoteValidationServiceBlocking;
        this.billingDunningReportingServiceBlocking = billingDunningReportingServiceBlocking;
        this.billingInvoiceQueryServiceBlocking = billingInvoiceQueryServiceBlocking;
        this.billingInvoiceValidationServiceBlocking = billingInvoiceValidationServiceBlocking;
        this.billingPaymentTermCalculationServiceBlocking = billingPaymentTermCalculationServiceBlocking;
        this.channels = channels;
        this.contractsContractClauseApprovalServiceBlocking = contractsContractClauseApprovalServiceBlocking;
        this.expensesReceiptApprovalServiceBlocking = expensesReceiptApprovalServiceBlocking;
        this.metricsMetricSeriesValidationServiceBlocking = metricsMetricSeriesValidationServiceBlocking;
        this.onboardingOnboardingPlanServiceBlocking = onboardingOnboardingPlanServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Activates a payment term.
     */
    public ActivatePaymentTermResponse activatePaymentTerm(ActivatePaymentTermRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivatePaymentTerm")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PaymentTerm", request.getPaymentTermId(), "ActivatePaymentTerm");

        var outbound0 = SubmitReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitReceiptRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .setApproverEmail(request.getDetails().getNewOwner().getEmail())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setCounterparty(MileageClaimRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        RpcPort<SubmitReceiptRequest, SubmitReceiptResponse> port0 = RpcPort.adapt(expensesReceiptApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::submitReceipt);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = PreviewMetricSeriesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewMetricSeriesRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getReceipt().getBillingAddress().getLinesList())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getReceipt().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        RpcPort<PreviewMetricSeriesRequest, PreviewMetricSeriesResponse> port1 = RpcPort.adapt(metricsMetricSeriesValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewMetricSeries);
        var reply1 = port1.exchange(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivatePaymentTerm", request.getDetails().getReasonCode());
        return ActivatePaymentTermResponse.newBuilder()
                .setOutcome(ActivatePaymentTermResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setName(reply1.getMetricSeries().getName())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply1.getMetricSeries().getOwner().getUserId())
                                .build())
                        .setPaymentTermId(request.getPaymentTermId())
                        .build())
                .build();
    }

    /**
     * Suspends a payment term.
     */
    public SuspendPaymentTermResponse suspendPaymentTerm(SuspendPaymentTermRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendPaymentTerm")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PaymentTerm", request.getPaymentTermId(), "SuspendPaymentTerm");

        var outbound0 = RpcSupport.unpack(Any.pack(CalculateCreditNoteRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setCounterparty(InvoiceRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .setIssuedAt(request.getDetails().getEffectiveAt())
                        .build())
                .setReason(request.getReason())
                .build()), CalculateCreditNoteRequest.class);
        var reply0 = billingCreditNoteCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateCreditNote(outbound0);

        var outbound1 = DelegateContractClauseApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setContractClause(ContractClause.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getCreditNote().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setDetails(DelegateContractClauseApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> contractsContractClauseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateContractClauseApproval(value));
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendPaymentTerm", request.getDetails().getReasonCode());
        return SuspendPaymentTermResponse.newBuilder()
                .setOutcome(SuspendPaymentTermResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCreditNote().getAudit().getCreatedBy())
                                .build())
                        .setPaymentTermId(request.getPaymentTermId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Reinstates a payment term.
     */
    public ReinstatePaymentTermResponse reinstatePaymentTerm(ReinstatePaymentTermRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstatePaymentTerm")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PaymentTerm", request.getPaymentTermId(), "ReinstatePaymentTerm");

        var outbound0 = BeanCopies.copy(ShareDunningReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareDunningReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build(), ShareDunningReportRequest.class);
        var reply0 = billingDunningReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareDunningReport(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstatePaymentTerm", request.getDetails().getReasonCode());
        return ReinstatePaymentTermResponse.newBuilder()
                .setOutcome(ReinstatePaymentTermResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getDunning().getAudit().getUpdatedAt())
                                .build())
                        .setPaymentTermId(request.getPaymentTermId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Terminates a payment term.
     */
    public TerminatePaymentTermResponse terminatePaymentTerm(TerminatePaymentTermRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminatePaymentTerm")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PaymentTerm", request.getPaymentTermId(), "TerminatePaymentTerm");

        var outbound0 = (ListOnboardingPlansRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListOnboardingPlansRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getPaymentTermId())
                        .build())
                .build()).toBuilder().build();
        var reply0 = onboardingOnboardingPlanServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listOnboardingPlans(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminatePaymentTerm", request.getDetails().getReasonCode());
        return TerminatePaymentTermResponse.newBuilder()
                .setOutcome(TerminatePaymentTermResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setPaymentTermId(request.getPaymentTermId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Renews a payment term.
     */
    public RenewPaymentTermResponse renewPaymentTerm(RenewPaymentTermRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewPaymentTerm")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PaymentTerm", request.getPaymentTermId(), "RenewPaymentTerm");

        var outbound0 = NormalizeInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeInvoiceRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .setInvoice(Invoice.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.billing.v1.InvoiceValidationService");
        var selected0 = InvoiceValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.normalizeInvoice(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewPaymentTerm", request.getDetails().getReasonCode());
        return RenewPaymentTermResponse.newBuilder()
                .setOutcome(RenewPaymentTermResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setPaymentTermId(request.getPaymentTermId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Expires a payment term.
     */
    public ExpirePaymentTermResponse expirePaymentTerm(ExpirePaymentTermRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpirePaymentTerm")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PaymentTerm", request.getPaymentTermId(), "ExpirePaymentTerm");

        var outbound0 = CompareCreditNoteOptionsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setCounterparty(InvoiceRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .setDueAt(request.getDetails().getEffectiveAt())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> billingCreditNoteCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareCreditNoteOptions(outbound0), rpcExecutor);

        var outbound1 = ResolveInvoicesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> billingInvoiceQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveInvoices(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpirePaymentTerm", request.getDetails().getReasonCode());
        return ExpirePaymentTermResponse.newBuilder()
                .setOutcome(ExpirePaymentTermResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getCreditNote().getAudit().getEtag())
                                .build())
                        .setPaymentTermId(request.getPaymentTermId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a payment term.
     */
    public GetPaymentTermLifecycleResponse getPaymentTermLifecycle(GetPaymentTermLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPaymentTermLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PaymentTerm", request.getPaymentTermId(), "GetPaymentTermLifecycle");

        var outbound0 = JsonCodec.roundTrip(NormalizeAuditTrailRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeAuditTrailRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build(), NormalizeAuditTrailRequest.class);
        var reply0 = auditAuditTrailValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeAuditTrail(outbound0);
        return GetPaymentTermLifecycleResponse.newBuilder()
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getAuditTrail().getAudit().getUpdatedAt())
                                .build())
                        .setPaymentTermId(request.getPaymentTermId())
                        .build())
                .build();
    }

    /**
     * Schedules a payment term.
     */
    public SchedulePaymentTermExpiryResponse schedulePaymentTermExpiry(SchedulePaymentTermExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SchedulePaymentTermExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentTerm(request.getContext().getTenantId(), request.getPaymentTerm());

        var outbound0 = ScoreInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPaymentTerm().getAudit().getUpdatedAt())
                                .build())
                        .addLines(InvoiceLine.newBuilder()
                                .setTax(InvoiceLineTax.newBuilder()
                                        .setAmount(request.getPaymentTerm().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = billingInvoiceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreInvoice(outbound0);

        var builder1 = CalculatePaymentTermRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setDetails(CalculatePaymentTermRequest.Details.newBuilder()
                .setDestination(PostalAddress.newBuilder()
                        .setCountryCode(reply0.getInvoice().getBillingAddress().getCountryCode())
                        .setLocality(reply0.getInvoice().getBillingAddress().getLocality())
                        .build())
                .build());
        builder1.setPaymentTerm(PaymentTerm.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedAt(reply0.getInvoice().getAudit().getUpdatedAt())
                        .build())
                .setOwner(UserRef.newBuilder()
                        .setUserId(request.getPaymentTerm().getOwner().getUserId())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = billingPaymentTermCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePaymentTerm(outbound1);

        var outbound2 = JsonCodec.roundTrip(NormalizeCreditNoteRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getInvoice().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getInvoice().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build(), NormalizeCreditNoteRequest.class);
        var reply2 = billingCreditNoteValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeCreditNote(outbound2);

        var outbound3 = GenerateDunningReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(switch (reply1.getPaymentTerm().getActionCase()) {
                    case BLOCK -> reply1.getPaymentTerm().getBlock().getReason();
                    default -> "";
                })
                .build();
        var reply3 = billingDunningReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generateDunningReport(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPaymentTerm().getPaymentTermId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SchedulePaymentTermExpiry", request.getPaymentTerm().getPaymentTermId());
        return SchedulePaymentTermExpiryResponse.newBuilder()
                .setOutcome(SchedulePaymentTermExpiryResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setEffective(reply1.getPaymentTerm().getEffective())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getInvoice().getCounterparty().getDisplayName())
                                .build())
                        .setPaymentTermId(request.getPaymentTermId())
                        .setVersion(request.getPaymentTerm().getVersion())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }
}
