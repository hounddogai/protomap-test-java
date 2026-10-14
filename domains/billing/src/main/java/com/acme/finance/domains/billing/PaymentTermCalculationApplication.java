package com.acme.finance.domains.billing;

import com.acme.audit.v1.AnnotateRetentionPolicyVersionRequest;
import com.acme.audit.v1.AnnotateRetentionPolicyVersionResponse;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyDiscountAction;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountValidationServiceGrpc;
import com.acme.billing.v1.CalculateInvoiceRequest;
import com.acme.billing.v1.CalculateInvoiceResponse;
import com.acme.billing.v1.CalculatePaymentTermRequest;
import com.acme.billing.v1.CalculatePaymentTermResponse;
import com.acme.billing.v1.ComparePaymentTermOptionsRequest;
import com.acme.billing.v1.ComparePaymentTermOptionsResponse;
import com.acme.billing.v1.EstimatePaymentTermRequest;
import com.acme.billing.v1.EstimatePaymentTermResponse;
import com.acme.billing.v1.ExplainInvoiceCalculationRequest;
import com.acme.billing.v1.ExplainInvoiceCalculationResponse;
import com.acme.billing.v1.ExplainPaymentTermCalculationRequest;
import com.acme.billing.v1.ExplainPaymentTermCalculationResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceBankSettlement;
import com.acme.billing.v1.InvoiceCalculationServiceGrpc;
import com.acme.billing.v1.InvoiceLine;
import com.acme.billing.v1.InvoiceLineTax;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.QuotePaymentTermRequest;
import com.acme.billing.v1.QuotePaymentTermResponse;
import com.acme.billing.v1.RecalculateInvoicesRequest;
import com.acme.billing.v1.RecalculateInvoicesResponse;
import com.acme.billing.v1.RecalculatePaymentTermsRequest;
import com.acme.billing.v1.RecalculatePaymentTermsResponse;
import com.acme.billing.v1.ScoreBillingAccountRequest;
import com.acme.billing.v1.ScoreBillingAccountResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.ForecastLine;
import com.acme.budgeting.v1.ForecastLineTax;
import com.acme.budgeting.v1.QuoteForecastRequest;
import com.acme.budgeting.v1.QuoteForecastResponse;
import com.acme.collections.v1.CloneCollectionCaseRequest;
import com.acme.collections.v1.CloneCollectionCaseResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.collections.v1.PaymentPromiseRef;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.AttestationRef;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.ValidateComplianceCaseRequest;
import com.acme.compliance.v1.ValidateComplianceCaseResponse;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.SubscribeLedgerAccountUpdatesRequest;
import com.acme.ledger.v1.SubscribeLedgerAccountUpdatesResponse;
import com.acme.messaging.v1.ConversationQueryServiceGrpc;
import com.acme.messaging.v1.ExportConversationsRequest;
import com.acme.messaging.v1.ExportConversationsResponse;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubLine;
import com.acme.payroll.v1.PayStubLineTax;
import com.acme.payroll.v1.PayStubValidationServiceGrpc;
import com.acme.payroll.v1.ValidatePayStubRequest;
import com.acme.payroll.v1.ValidatePayStubResponse;
import com.google.protobuf.Any;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a payment term, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class PaymentTermCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(PaymentTermCalculationApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BillingAccountValidationServiceGrpc.BillingAccountValidationServiceBlockingStub billingBillingAccountValidationServiceBlocking;

    private final InvoiceCalculationServiceGrpc.InvoiceCalculationServiceBlockingStub billingInvoiceCalculationServiceBlocking;

    private final ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking;

    private final CollectionCaseServiceGrpc.CollectionCaseServiceBlockingStub collectionsCollectionCaseServiceBlocking;

    private final ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking;

    private final LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking;

    private final PaymentTermCalculationRequestMapper mapper;

    private final ConversationQueryServiceGrpc.ConversationQueryServiceBlockingStub messagingConversationQueryServiceBlocking;

    private final PayStubValidationServiceGrpc.PayStubValidationServiceBlockingStub payrollPayStubValidationServiceBlocking;

    private final BillingRepository repository;

    private final Executor rpcExecutor;

    public PaymentTermCalculationApplication(
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BillingAccountValidationServiceGrpc.BillingAccountValidationServiceBlockingStub billingBillingAccountValidationServiceBlocking,
            InvoiceCalculationServiceGrpc.InvoiceCalculationServiceBlockingStub billingInvoiceCalculationServiceBlocking,
            ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking,
            CollectionCaseServiceGrpc.CollectionCaseServiceBlockingStub collectionsCollectionCaseServiceBlocking,
            ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking,
            LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking,
            PaymentTermCalculationRequestMapper mapper,
            ConversationQueryServiceGrpc.ConversationQueryServiceBlockingStub messagingConversationQueryServiceBlocking,
            PayStubValidationServiceGrpc.PayStubValidationServiceBlockingStub payrollPayStubValidationServiceBlocking,
            BillingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditRetentionPolicyHistoryServiceBlocking = auditRetentionPolicyHistoryServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.billingBillingAccountValidationServiceBlocking = billingBillingAccountValidationServiceBlocking;
        this.billingInvoiceCalculationServiceBlocking = billingInvoiceCalculationServiceBlocking;
        this.budgetingForecastCalculationServiceBlocking = budgetingForecastCalculationServiceBlocking;
        this.collectionsCollectionCaseServiceBlocking = collectionsCollectionCaseServiceBlocking;
        this.complianceComplianceCaseValidationServiceBlocking = complianceComplianceCaseValidationServiceBlocking;
        this.ledgerLedgerAccountNotificationServiceBlocking = ledgerLedgerAccountNotificationServiceBlocking;
        this.mapper = mapper;
        this.messagingConversationQueryServiceBlocking = messagingConversationQueryServiceBlocking;
        this.payrollPayStubValidationServiceBlocking = payrollPayStubValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Calculates a payment term.
     */
    public CalculatePaymentTermResponse calculatePaymentTerm(CalculatePaymentTermRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculatePaymentTerm")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentTerm(request.getContext().getTenantId(), request.getPaymentTerm());

        var outbound0 = CalculateInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateInvoiceRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .build())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPaymentTerm().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<CalculateInvoiceRequest, CalculateInvoiceResponse>() {
            @Override
            public CalculateInvoiceResponse apply(CalculateInvoiceRequest input) {
                return billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateInvoice(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = mapper.calculatePaymentTermStage1(request, reply0);
        var reply1 = billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateInvoices(outbound1);

        var outbound2 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateAuditEventsResponse.getDefaultInstance());
        var input2 = auditAuditEventServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateAuditEvents(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPaymentTerm().getAudit().getCreatedAt())
                                .setCreatedBy(request.getPaymentTerm().getAudit().getCreatedBy())
                                .setEtag(request.getPaymentTerm().getAudit().getEtag())
                                .setUpdatedAt(request.getPaymentTerm().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getPaymentTerm().getPaymentTermId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculatePaymentTerm", request.getPaymentTerm().getPaymentTermId());
        return CalculatePaymentTermResponse.newBuilder()
                .setBreakdown(CalculatePaymentTermResponse.Breakdown.newBuilder()
                        .setTotal(reply0.getBreakdown().getTotal())
                        .build())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getInvoice().getAudit().getUpdatedBy())
                                .build())
                        .putAllParameters(request.getPaymentTerm().getParametersMap())
                        .setPaymentTermId(request.getPaymentTermId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Estimates a payment term.
     */
    public EstimatePaymentTermResponse estimatePaymentTerm(EstimatePaymentTermRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimatePaymentTerm")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentTerm(request.getContext().getTenantId(), request.getPaymentTerm());

        var outbound0 = CloneCollectionCaseRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getPaymentTerm().getAudit().getEtag())
                                .build())
                        .setCustomer(PaymentPromiseRef.newBuilder()
                                .setDisplayName(request.getPaymentTerm().getOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> collectionsCollectionCaseServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneCollectionCase(outbound0), rpcExecutor);

        var outbound1 = ValidatePayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .addLines(PayStubLine.newBuilder()
                                .setTax(PayStubLineTax.newBuilder()
                                        .setAmount(request.getPaymentTerm().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> payrollPayStubValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePayStub(outbound1), rpcExecutor);

        var outbound2 = ExplainInvoiceCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .build();
        var pending2 = CompletableFuture.supplyAsync(() -> billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainInvoiceCalculation(outbound2), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1, pending2).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        var reply2 = pending2.join();
        log.atDebug().addKeyValue("auditValue", request.getPaymentTerm().getPaymentTermId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimatePaymentTerm", request.getPaymentTerm().getPaymentTermId());
        return EstimatePaymentTermResponse.newBuilder()
                .setBreakdown(EstimatePaymentTermResponse.Breakdown.newBuilder()
                        .setTotal(reply2.getInvoice().getTotal())
                        .build())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getPayStub().getCounterparty().getDisplayName())
                                .build())
                        .setPaymentTermId(request.getPaymentTermId())
                        .setPriority(request.getPaymentTerm().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Quotes a payment term.
     */
    public QuotePaymentTermResponse quotePaymentTerm(QuotePaymentTermRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuotePaymentTerm")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentTerm(request.getContext().getTenantId(), request.getPaymentTerm());

        var outbound0 = CalculateInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .addLines(InvoiceLine.newBuilder()
                                .setTax(InvoiceLineTax.newBuilder()
                                        .setAmount(request.getPaymentTerm().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateInvoice(outbound0), rpcExecutor);

        var outbound1 = SubscribeLedgerAccountUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeLedgerAccountUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getPaymentTerm().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> ledgerLedgerAccountNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeLedgerAccountUpdates(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getPaymentTerm().getPaymentTermId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuotePaymentTerm", request.getPaymentTerm().getPaymentTermId());
        return QuotePaymentTermResponse.newBuilder()
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getInvoice().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply1.getLedgerAccount().getContact().getEmail())
                                .build())
                        .putAllParameters(request.getPaymentTerm().getParametersMap())
                        .setPaymentTermId(request.getPaymentTermId())
                        .build())
                .build();
    }

    /**
     * Compares a payment term.
     */
    public ComparePaymentTermOptionsResponse comparePaymentTermOptions(ComparePaymentTermOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ComparePaymentTermOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentTerm(request.getContext().getTenantId(), request.getPaymentTerm());

        var outbound0 = ValidateComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getPaymentTerm().getAudit().getUpdatedBy())
                                .build())
                        .setCustomer(AttestationRef.newBuilder()
                                .setDisplayName(request.getPaymentTerm().getOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> complianceComplianceCaseValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateComplianceCase(value));
        log.atDebug().addKeyValue("auditValue", request.getPaymentTerm().getPaymentTermId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ComparePaymentTermOptions", request.getPaymentTerm().getPaymentTermId());
        return ComparePaymentTermOptionsResponse.newBuilder()
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getComplianceCase().getAudit().getEtag())
                                .build())
                        .setPaymentTermId(request.getPaymentTermId())
                        .setVersion(request.getPaymentTerm().getVersion())
                        .build())
                .build();
    }

    /**
     * Recalculates a payment term.
     */
    public RecalculatePaymentTermsResponse recalculatePaymentTerms(RecalculatePaymentTermsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculatePaymentTerms")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentTerm(request.getContext().getTenantId(), request.getPaymentTerm());

        var outbound0 = (AnnotateRetentionPolicyVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AnnotateRetentionPolicyVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPaymentTerm().getAudit().getCreatedAt())
                                .setEtag(request.getPaymentTerm().getAudit().getEtag())
                                .build())
                        .setDiscount(RetentionPolicyDiscountAction.newBuilder()
                                .setAmount(request.getPaymentTerm().getDiscount().getAmount())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getPaymentTerm().getOwner().getDisplayName())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotateRetentionPolicyVersion(outbound0);

        var outbound1 = ExplainInvoiceCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getRetentionPolicy().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        RpcPort<ExplainInvoiceCalculationRequest, ExplainInvoiceCalculationResponse> port1 = RpcPort.adapt(billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainInvoiceCalculation);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = ExportConversationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportConversationsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getInvoice().getAudit().getCreatedAt())
                        .setRegion(reply1.getInvoice().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream2 = messagingConversationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportConversations(outbound2);
        var reply2 = ExportConversationsResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }

        var outbound3 = JsonCodec.roundTrip(ScoreBillingAccountRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getInvoice().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getRetentionPolicy().getAudit().getCreatedBy())
                                .setUpdatedAt(reply1.getInvoice().getAudit().getUpdatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getRetentionPolicy().getOwner().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), ScoreBillingAccountRequest.class);
        var reply3 = billingBillingAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreBillingAccount(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPaymentTerm().getPaymentTermId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculatePaymentTerms", request.getPaymentTerm().getPaymentTermId());
        return RecalculatePaymentTermsResponse.newBuilder()
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getInvoice().getAudit().getCreatedAt())
                                .setCreatedBy(reply3.getBillingAccount().getAudit().getCreatedBy())
                                .setEtag(reply0.getRetentionPolicy().getAudit().getEtag())
                                .setUpdatedAt(reply1.getInvoice().getAudit().getUpdatedAt())
                                .build())
                        .setPaymentTermId(request.getPaymentTermId())
                        .setPriority(request.getPaymentTerm().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Explains a payment term.
     */
    public ExplainPaymentTermCalculationResponse explainPaymentTermCalculation(ExplainPaymentTermCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainPaymentTermCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePaymentTerm(request.getContext().getTenantId(), request.getPaymentTerm());

        var outbound0 = QuoteForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getPaymentTerm().getAudit().getUpdatedBy())
                                .build())
                        .addLines(ForecastLine.newBuilder()
                                .setDescription(request.getPaymentTerm().getDescription())
                                .setTax(ForecastLineTax.newBuilder()
                                        .setAmount(request.getPaymentTerm().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<QuoteForecastRequest, QuoteForecastResponse>() {
            @Override
            public QuoteForecastResponse apply(QuoteForecastRequest input) {
                return budgetingForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteForecast(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPaymentTerm().getPaymentTermId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainPaymentTermCalculation", request.getPaymentTerm().getPaymentTermId());
        return ExplainPaymentTermCalculationResponse.newBuilder()
                .setOutcome(ExplainPaymentTermCalculationResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setPaymentTermId(request.getPaymentTermId())
                        .setVersion(request.getPaymentTerm().getVersion())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }
}
