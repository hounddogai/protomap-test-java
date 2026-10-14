package com.acme.finance.domains.billing;

import com.acme.admissions.v1.Admission;
import com.acme.admissions.v1.AdmissionCalculationServiceGrpc;
import com.acme.admissions.v1.AdmissionMeasurement;
import com.acme.admissions.v1.RecalculateAdmissionsRequest;
import com.acme.admissions.v1.RecalculateAdmissionsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesResponse;
import com.acme.billing.v1.BillingAccountQueryServiceGrpc;
import com.acme.billing.v1.BrowseInvoicesRequest;
import com.acme.billing.v1.BrowseInvoicesResponse;
import com.acme.billing.v1.CountInvoicesRequest;
import com.acme.billing.v1.CountInvoicesResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteCalculationServiceGrpc;
import com.acme.billing.v1.CreditNoteLine;
import com.acme.billing.v1.FindBillingAccountsRequest;
import com.acme.billing.v1.FindBillingAccountsResponse;
import com.acme.billing.v1.FindInvoicesRequest;
import com.acme.billing.v1.FindInvoicesResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceLine;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.LintPaymentTermsRequest;
import com.acme.billing.v1.LintPaymentTermsResponse;
import com.acme.billing.v1.LookupInvoiceRequest;
import com.acme.billing.v1.LookupInvoiceResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermValidationServiceGrpc;
import com.acme.billing.v1.PeekInvoiceRequest;
import com.acme.billing.v1.PeekInvoiceResponse;
import com.acme.billing.v1.QuoteCreditNoteRequest;
import com.acme.billing.v1.QuoteCreditNoteResponse;
import com.acme.billing.v1.ResolveInvoicesRequest;
import com.acme.billing.v1.ResolveInvoicesResponse;
import com.acme.billing.v1.SuggestInvoicesRequest;
import com.acme.billing.v1.SuggestInvoicesResponse;
import com.acme.billing.v1.SummarizeInvoicesRequest;
import com.acme.billing.v1.SummarizeInvoicesResponse;
import com.acme.budgeting.v1.BatchCreateBudgetsRequest;
import com.acme.budgeting.v1.BatchCreateBudgetsResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetServiceGrpc;
import com.acme.budgeting.v1.NormalizeBudgetLineRequest;
import com.acme.budgeting.v1.NormalizeBudgetLineResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.acme.procurement.v1.WatchPurchaseRequisitionsRequest;
import com.acme.procurement.v1.WatchPurchaseRequisitionsResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionValidationServiceGrpc;
import com.acme.treasury.v1.CheckCashPositionEligibilityRequest;
import com.acme.treasury.v1.CheckCashPositionEligibilityResponse;
import com.acme.treasury.v1.ExplainCashPositionDecisionRequest;
import com.acme.treasury.v1.ExplainCashPositionDecisionResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about invoices, for screens and reports. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class InvoiceQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(InvoiceQueryApplication.class);

    private final AdmissionCalculationServiceGrpc.AdmissionCalculationServiceBlockingStub admissionsAdmissionCalculationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceBlockingStub billingCreditNoteCalculationServiceBlocking;

    private final PaymentTermValidationServiceGrpc.PaymentTermValidationServiceBlockingStub billingPaymentTermValidationServiceBlocking;

    private final BudgetServiceGrpc.BudgetServiceStub budgetingBudgetServiceAsync;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceBlockingStub procurementPurchaseRequisitionServiceBlocking;

    private final BillingRepository repository;

    private final CashPositionValidationServiceGrpc.CashPositionValidationServiceBlockingStub treasuryCashPositionValidationServiceBlocking;

    public InvoiceQueryApplication(
            AdmissionCalculationServiceGrpc.AdmissionCalculationServiceBlockingStub admissionsAdmissionCalculationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceBlockingStub billingCreditNoteCalculationServiceBlocking,
            PaymentTermValidationServiceGrpc.PaymentTermValidationServiceBlockingStub billingPaymentTermValidationServiceBlocking,
            BudgetServiceGrpc.BudgetServiceStub budgetingBudgetServiceAsync,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceBlockingStub procurementPurchaseRequisitionServiceBlocking,
            BillingRepository repository,
            CashPositionValidationServiceGrpc.CashPositionValidationServiceBlockingStub treasuryCashPositionValidationServiceBlocking) {
        this.admissionsAdmissionCalculationServiceBlocking = admissionsAdmissionCalculationServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.billingCreditNoteCalculationServiceBlocking = billingCreditNoteCalculationServiceBlocking;
        this.billingPaymentTermValidationServiceBlocking = billingPaymentTermValidationServiceBlocking;
        this.budgetingBudgetServiceAsync = budgetingBudgetServiceAsync;
        this.channels = channels;
        this.events = events;
        this.procurementPurchaseRequisitionServiceBlocking = procurementPurchaseRequisitionServiceBlocking;
        this.repository = repository;
        this.treasuryCashPositionValidationServiceBlocking = treasuryCashPositionValidationServiceBlocking;
    }

    public record LookupInvoiceStage0Requested(NormalizeBudgetLineRequest request, CompletableFuture<NormalizeBudgetLineResponse> completion, io.grpc.Context context) {}

    /**
     * Returns an invoice.
     */
    public LookupInvoiceResponse lookupInvoice(LookupInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Invoice", request.getInvoiceId(), "LookupInvoice");

        var outbound0 = NormalizeBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeBudgetLineRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var completion0 = new CompletableFuture<NormalizeBudgetLineResponse>();
        events.publishEvent(new LookupInvoiceStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = BatchCreateBudgetsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateBudgetsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateBudgetsResponse.getDefaultInstance());
        var input1 = budgetingBudgetServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateBudgets(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input2 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        return LookupInvoiceResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getBudgetLine().getBillingAddress().getCountryCode())
                                .setLocality(reply0.getBudgetLine().getBillingAddress().getLocality())
                                .build())
                        .setInvoiceId(request.getInvoiceId())
                        .build())
                .build();
    }

    /**
     * Lists invoices a page at a time.
     */
    public BrowseInvoicesResponse browseInvoices(BrowseInvoicesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseInvoices")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Invoice", request.getContext().getTraceId(), "BrowseInvoices");

        var outbound0 = WatchPurchaseRequisitionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchPurchaseRequisitionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .build();
        var stream0 = procurementPurchaseRequisitionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchPurchaseRequisitions(outbound0);
        var reply0 = WatchPurchaseRequisitionsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseInvoices", request.getPage().getPageToken());
        return BrowseInvoicesResponse.getDefaultInstance();
    }

    /**
     * Lists invoices a page at a time.
     */
    public FindInvoicesResponse findInvoices(FindInvoicesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindInvoices")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Invoice", request.getContext().getTraceId(), "FindInvoices");

        var outbound0 = WatchPurchaseRequisitionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchPurchaseRequisitionsRequest.Filter.newBuilder()
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var stream0 = procurementPurchaseRequisitionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchPurchaseRequisitions(outbound0);
        var reply0 = WatchPurchaseRequisitionsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindInvoices", request.getPage().getPageToken());
        return FindInvoicesResponse.getDefaultInstance();
    }

    /**
     * Counts invoices by status.
     */
    public CountInvoicesResponse countInvoices(CountInvoicesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountInvoices")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Invoice", request.getContext().getTraceId(), "CountInvoices");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountInvoices", request.getPage().getPageToken());
        return CountInvoicesResponse.getDefaultInstance();
    }

    /**
     * Returns several invoices at once.
     */
    public ResolveInvoicesResponse resolveInvoices(ResolveInvoicesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveInvoices")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Invoice", request.getContext().getTraceId(), "ResolveInvoices");

        var builder0 = CheckCashPositionEligibilityRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(CheckCashPositionEligibilityRequest.Details.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = treasuryCashPositionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkCashPositionEligibility(outbound0);
        return ResolveInvoicesResponse.newBuilder()
                .addInvoices(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCashPosition().getAudit().getCreatedBy())
                                .build())
                        .setNumber(reply0.getCashPosition().getHolder().getNumber())
                        .build())
                .build();
    }

    /**
     * Summarizes an invoice.
     */
    public SummarizeInvoicesResponse summarizeInvoices(SummarizeInvoicesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeInvoices")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Invoice", request.getInvoiceId(), "SummarizeInvoices");

        var outbound0 = QuoteCreditNoteRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setCounterparty(InvoiceRef.newBuilder()
                                .setInvoiceId(request.getInvoiceId())
                                .build())
                        .addLines(CreditNoteLine.newBuilder()
                                .putAllAttributes(request.getDetails().getFiltersMap())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var reply0 = billingCreditNoteCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCreditNote(outbound0);

        var outbound1 = FindBillingAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindBillingAccountsRequest.Filter.newBuilder()
                        .setRegion(reply0.getCreditNote().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.billing.v1.BillingAccountQueryService");
        var selected1 = BillingAccountQueryServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.findBillingAccounts(outbound1);

        var outbound2 = SubscribeRetentionPolicyUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeRetentionPolicyUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .build())
                .setReason(request.getReason())
                .build();
        var channel2 = channels.lookup("acme.audit.v1.RetentionPolicyNotificationService");
        var serviceName2 = "acme.audit.v1" + "." + "RetentionPolicyNotificationService";
        var methodName2 = "Sub" + "scribeRetentionPolicyUpdates";
        var descriptor2 = MethodDescriptor.<SubscribeRetentionPolicyUpdatesRequest, SubscribeRetentionPolicyUpdatesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(SubscribeRetentionPolicyUpdatesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SubscribeRetentionPolicyUpdatesResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = LintPaymentTermsRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCreditNote().getAudit().getCreatedBy())
                                .setEtag(reply0.getCreditNote().getAudit().getEtag())
                                .setUpdatedAt(reply0.getCreditNote().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var reply3 = billingPaymentTermValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintPaymentTerms(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getFormat()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeInvoices", request.getDetails().getFormat());
        return SummarizeInvoicesResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists invoices a page at a time.
     */
    public SuggestInvoicesResponse suggestInvoices(SuggestInvoicesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestInvoices")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Invoice", request.getContext().getTraceId(), "SuggestInvoices");

        var outbound0 = RecalculateAdmissionsRequest.newBuilder()
                .setAdmission(Admission.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .addMeasurements(AdmissionMeasurement.newBuilder()
                                .setObservedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setSource(request.getFilter().getOwnerId())
                        .build())
                .setContext(request.getContext())
                .setDetails(RecalculateAdmissionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        RpcPort<RecalculateAdmissionsRequest, RecalculateAdmissionsResponse> port0 = RpcPort.adapt(admissionsAdmissionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::recalculateAdmissions);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestInvoices", request.getPage().getPageToken());
        return SuggestInvoicesResponse.newBuilder()
                .addInvoices(Invoice.newBuilder()
                        .addLines(InvoiceLine.newBuilder()
                                .putAllAttributes(reply0.getAdmission().getAttributesMap())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getAdmission().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns an invoice.
     */
    public PeekInvoiceResponse peekInvoice(PeekInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Invoice", request.getInvoiceId(), "PeekInvoice");

        var outbound0 = ExplainCashPositionDecisionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainCashPositionDecisionRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> treasuryCashPositionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCashPositionDecision(value)).orElseThrow();
        return PeekInvoiceResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getCashPosition().getAudit().getUpdatedBy())
                                .build())
                        .setInvoiceId(request.getInvoiceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
