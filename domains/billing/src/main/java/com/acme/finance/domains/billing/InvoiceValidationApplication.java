package com.acme.finance.domains.billing;

import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.SearchFixedAssetsRequest;
import com.acme.assets.v1.SearchFixedAssetsResponse;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.BrowseAuditEventsRequest;
import com.acme.audit.v1.BrowseAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountCardFunding;
import com.acme.billing.v1.BillingAccountValidationServiceGrpc;
import com.acme.billing.v1.CheckInvoiceEligibilityRequest;
import com.acme.billing.v1.CheckInvoiceEligibilityResponse;
import com.acme.billing.v1.CompareCreditNoteOptionsRequest;
import com.acme.billing.v1.CompareCreditNoteOptionsResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteAttachment;
import com.acme.billing.v1.CreditNoteCalculationServiceGrpc;
import com.acme.billing.v1.CreditNoteCardSettlement;
import com.acme.billing.v1.ExplainInvoiceDecisionRequest;
import com.acme.billing.v1.ExplainInvoiceDecisionResponse;
import com.acme.billing.v1.GetInvoiceRulesRequest;
import com.acme.billing.v1.GetInvoiceRulesResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.LintBillingAccountsRequest;
import com.acme.billing.v1.LintBillingAccountsResponse;
import com.acme.billing.v1.LintInvoicesRequest;
import com.acme.billing.v1.LintInvoicesResponse;
import com.acme.billing.v1.NormalizeInvoiceRequest;
import com.acme.billing.v1.NormalizeInvoiceResponse;
import com.acme.billing.v1.PreviewInvoiceRequest;
import com.acme.billing.v1.PreviewInvoiceResponse;
import com.acme.billing.v1.QuoteCreditNoteRequest;
import com.acme.billing.v1.QuoteCreditNoteResponse;
import com.acme.billing.v1.RecalculateCreditNotesRequest;
import com.acme.billing.v1.RecalculateCreditNotesResponse;
import com.acme.billing.v1.ScoreBillingAccountRequest;
import com.acme.billing.v1.ScoreBillingAccountResponse;
import com.acme.billing.v1.ScoreInvoiceRequest;
import com.acme.billing.v1.ScoreInvoiceResponse;
import com.acme.billing.v1.SimulateCreditNoteRequest;
import com.acme.billing.v1.SimulateCreditNoteResponse;
import com.acme.billing.v1.ValidateInvoiceRequest;
import com.acme.billing.v1.ValidateInvoiceResponse;
import com.acme.collections.v1.BatchCreateCollectionCasesRequest;
import com.acme.collections.v1.BatchCreateCollectionCasesResponse;
import com.acme.collections.v1.CheckWriteOffEligibilityRequest;
import com.acme.collections.v1.CheckWriteOffEligibilityResponse;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffLine;
import com.acme.collections.v1.WriteOffLineTax;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.expenses.v1.CheckExpensePolicyEligibilityRequest;
import com.acme.expenses.v1.CheckExpensePolicyEligibilityResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyValidationServiceGrpc;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.invoicing.v1.FindInvoiceBatchesRequest;
import com.acme.invoicing.v1.FindInvoiceBatchesResponse;
import com.acme.invoicing.v1.InvoiceBatchQueryServiceGrpc;
import com.acme.ledger.v1.CreateLedgerAccountRequest;
import com.acme.ledger.v1.CreateLedgerAccountResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.metrics.v1.SuggestMetricSeriesesRequest;
import com.acme.metrics.v1.SuggestMetricSeriesesResponse;
import com.acme.metrics.v1.SummarizeMetricSeriesesRequest;
import com.acme.metrics.v1.SummarizeMetricSeriesesResponse;
import com.acme.notifications.v1.FindNotificationsRequest;
import com.acme.notifications.v1.FindNotificationsResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.notifications.v1.ResolveNotificationsRequest;
import com.acme.notifications.v1.ResolveNotificationsResponse;
import com.acme.payroll.v1.EstimatePayStubRequest;
import com.acme.payroll.v1.EstimatePayStubResponse;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.recommendations.v1.BatchGetRecommendationSetsRequest;
import com.acme.recommendations.v1.BatchGetRecommendationSetsResponse;
import com.acme.recommendations.v1.RecommendationSetServiceGrpc;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.SearchSearchIndexesRequest;
import com.acme.search.v1.SearchSearchIndexesResponse;
import com.acme.treasury.v1.BankTransferQueryServiceGrpc;
import com.acme.treasury.v1.BrowseBankTransfersRequest;
import com.acme.treasury.v1.BrowseBankTransfersResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
 * Checks invoices against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class InvoiceValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(InvoiceValidationApplication.class);

    private final FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking;

    private final AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BillingAccountValidationServiceGrpc.BillingAccountValidationServiceBlockingStub billingBillingAccountValidationServiceBlocking;

    private final CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceBlockingStub billingCreditNoteCalculationServiceBlocking;

    private final CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceFutureStub billingCreditNoteCalculationServiceFuture;

    private final CollectionCaseServiceGrpc.CollectionCaseServiceStub collectionsCollectionCaseServiceAsync;

    private final WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ExpensePolicyValidationServiceGrpc.ExpensePolicyValidationServiceBlockingStub expensesExpensePolicyValidationServiceBlocking;

    private final InvoiceBatchQueryServiceGrpc.InvoiceBatchQueryServiceBlockingStub invoicingInvoiceBatchQueryServiceBlocking;

    private final LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking;

    private final MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking;

    private final NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking;

    private final PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking;

    private final RecommendationSetServiceGrpc.RecommendationSetServiceBlockingStub recommendationsRecommendationSetServiceBlocking;

    private final BillingRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking;

    private final BankTransferQueryServiceGrpc.BankTransferQueryServiceBlockingStub treasuryBankTransferQueryServiceBlocking;

    public InvoiceValidationApplication(
            FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking,
            AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BillingAccountValidationServiceGrpc.BillingAccountValidationServiceBlockingStub billingBillingAccountValidationServiceBlocking,
            CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceBlockingStub billingCreditNoteCalculationServiceBlocking,
            CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceFutureStub billingCreditNoteCalculationServiceFuture,
            CollectionCaseServiceGrpc.CollectionCaseServiceStub collectionsCollectionCaseServiceAsync,
            WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking,
            ApplicationEventPublisher events,
            ExpensePolicyValidationServiceGrpc.ExpensePolicyValidationServiceBlockingStub expensesExpensePolicyValidationServiceBlocking,
            InvoiceBatchQueryServiceGrpc.InvoiceBatchQueryServiceBlockingStub invoicingInvoiceBatchQueryServiceBlocking,
            LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking,
            MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking,
            NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking,
            PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking,
            RecommendationSetServiceGrpc.RecommendationSetServiceBlockingStub recommendationsRecommendationSetServiceBlocking,
            BillingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking,
            BankTransferQueryServiceGrpc.BankTransferQueryServiceBlockingStub treasuryBankTransferQueryServiceBlocking) {
        this.assetsFixedAssetServiceBlocking = assetsFixedAssetServiceBlocking;
        this.auditAuditEventQueryServiceBlocking = auditAuditEventQueryServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.billingBillingAccountValidationServiceBlocking = billingBillingAccountValidationServiceBlocking;
        this.billingCreditNoteCalculationServiceBlocking = billingCreditNoteCalculationServiceBlocking;
        this.billingCreditNoteCalculationServiceFuture = billingCreditNoteCalculationServiceFuture;
        this.collectionsCollectionCaseServiceAsync = collectionsCollectionCaseServiceAsync;
        this.collectionsWriteOffValidationServiceBlocking = collectionsWriteOffValidationServiceBlocking;
        this.events = events;
        this.expensesExpensePolicyValidationServiceBlocking = expensesExpensePolicyValidationServiceBlocking;
        this.invoicingInvoiceBatchQueryServiceBlocking = invoicingInvoiceBatchQueryServiceBlocking;
        this.ledgerLedgerAccountServiceBlocking = ledgerLedgerAccountServiceBlocking;
        this.metricsMetricSeriesQueryServiceBlocking = metricsMetricSeriesQueryServiceBlocking;
        this.notificationsNotificationQueryServiceBlocking = notificationsNotificationQueryServiceBlocking;
        this.payrollPayStubCalculationServiceBlocking = payrollPayStubCalculationServiceBlocking;
        this.recommendationsRecommendationSetServiceBlocking = recommendationsRecommendationSetServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexServiceBlocking = searchSearchIndexServiceBlocking;
        this.treasuryBankTransferQueryServiceBlocking = treasuryBankTransferQueryServiceBlocking;
    }

    public record ScoreInvoiceStage1Requested(EstimatePayStubRequest request, CompletableFuture<EstimatePayStubResponse> completion, io.grpc.Context context) {}

    /**
     * Validates an invoice.
     */
    public ValidateInvoiceResponse validateInvoice(ValidateInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = FindInvoiceBatchesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindInvoiceBatchesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoice().getIssuedAt())
                        .setOwnerId(request.getInvoiceId())
                        .setRegion(request.getInvoice().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> invoicingInvoiceBatchQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findInvoiceBatches(outbound0), rpcExecutor);

        var outbound1 = EstimatePayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimatePayStubRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getInvoice().getBillingAddress().getLocality())
                                .build())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getInvoice().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimatePayStub(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = CheckExpensePolicyEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getPayStub().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        final class Stage2Adapter {
            CheckExpensePolicyEligibilityResponse fetch(CheckExpensePolicyEligibilityRequest input) {
                return expensesExpensePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkExpensePolicyEligibility(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);

        var outbound3 = BrowseAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoice().getAudit().getUpdatedAt())
                        .setOwnerId(request.getInvoice().getCounterparty().getCustomerId())
                        .setRegion(reply1.getPayStub().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply3 = RpcSupport.retryRead(() -> auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseAuditEvents(outbound3));

        var outbound4 = BatchCreateCollectionCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateCollectionCasesRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getContact())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(BatchCreateCollectionCasesResponse.getDefaultInstance());
        var input4 = collectionsCollectionCaseServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateCollectionCases(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();

        var outbound5 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getInvoice().getAudit().getCreatedAt())
                                .setCreatedBy(request.getInvoice().getAudit().getCreatedBy())
                                .setEtag(request.getInvoice().getAudit().getEtag())
                                .setUpdatedAt(request.getInvoice().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector5 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input5 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector5);
        input5.onNext(outbound5);
        input5.onCompleted();
        var reply5 = collector5.await();
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateInvoice", request.getInvoice().getInvoiceId());
        return ValidateInvoiceResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply1.getPayStub().getBillingAddress().getLinesList())
                                .build())
                        .setInvoiceId(request.getInvoiceId())
                        .setNotes(request.getInvoice().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Previews an invoice.
     */
    public PreviewInvoiceResponse previewInvoice(PreviewInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = SearchFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchFixedAssetsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoice().getDueAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setRegion(request.getInvoice().getBillingAddress().getRegion())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.assets.v1.FixedAssetService/SearchFixedAssets", outbound0);
        var reply0 = assetsFixedAssetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchFixedAssets(command0.payload());

        List<CheckWriteOffEligibilityResponse> results1 = request.getInvoice().getLinesList().stream()
                .map(element -> CheckWriteOffEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .addLines(WriteOffLine.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .setTax(WriteOffLineTax.newBuilder()
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkWriteOffEligibility(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? CheckWriteOffEligibilityResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewInvoice", request.getInvoice().getInvoiceId());
        return PreviewInvoiceResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setBillingAddress(request.getInvoice().getBillingAddress())
                        .setInvoiceId(request.getInvoiceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Checks an invoice.
     */
    public CheckInvoiceEligibilityResponse checkInvoiceEligibility(CheckInvoiceEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckInvoiceEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = BatchGetRecommendationSetsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> recommendationsRecommendationSetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetRecommendationSets(outbound0), rpcExecutor);

        var outbound1 = ResolveNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> notificationsNotificationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveNotifications(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = SearchSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchSearchIndexesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoice().getAudit().getCreatedAt())
                        .putAllLabels(request.getInvoice().getMetadataMap())
                        .setOwnerId(reply1.getResult().getCorrelationId())
                        .setRegion(request.getInvoice().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(searchSearchIndexServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchSearchIndexes(value)))
                .collect(Collectors.toList()).getFirst();

        var results3 = new ArrayList<RecalculateCreditNotesResponse>();
        for (var element : request.getInvoice().getAttachmentsList()) {
            RecalculateCreditNotesRequest itemRequest = RecalculateCreditNotesRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .addAttachments(CreditNoteAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setContentType(element.getContentType())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .build();
            results3.add(billingCreditNoteCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateCreditNotes(itemRequest));
        }
        var reply3 = results3.isEmpty() ? RecalculateCreditNotesResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckInvoiceEligibility", request.getInvoice().getInvoiceId());
        return CheckInvoiceEligibilityResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setBillingAddress(request.getInvoice().getBillingAddress())
                        .setInvoiceId(request.getInvoiceId())
                        .build())
                .setOutcome(CheckInvoiceEligibilityResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Normalizes an invoice.
     */
    public NormalizeInvoiceResponse normalizeInvoice(NormalizeInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = QuoteCreditNoteRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getInvoice().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getInvoice().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setDetails(QuoteCreditNoteRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getInvoice().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var pending0 = billingCreditNoteCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCreditNote(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = (CompareCreditNoteOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getCreditNote().getAudit().getCreatedAt())
                                .setUpdatedBy(request.getInvoice().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getCreditNote().getBillingAddress().getLocality())
                                .build())
                        .setCounterparty(InvoiceRef.newBuilder()
                                .setDisplayName(reply0.getCreditNote().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = billingCreditNoteCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareCreditNoteOptions(outbound1);

        var outbound2 = RecalculateCreditNotesRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply1.getCreditNote().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var command2 = new DispatchCommand<>("acme.billing.v1.CreditNoteCalculationService/RecalculateCreditNotes", outbound2);
        var reply2 = billingCreditNoteCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateCreditNotes(command2.payload());
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeInvoice", request.getInvoice().getInvoiceId());
        return NormalizeInvoiceResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getCreditNote().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply1.getCreditNote().getBillingAddress().getLinesList())
                                .build())
                        .setInvoiceId(request.getInvoiceId())
                        .setNotes(request.getInvoice().getNotes())
                        .build())
                .setOutcome(NormalizeInvoiceResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Explains an invoice.
     */
    public ExplainInvoiceDecisionResponse explainInvoiceDecision(ExplainInvoiceDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainInvoiceDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = RecalculateCreditNotesRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setCard(CreditNoteCardSettlement.newBuilder()
                                .setLast4(switch (request.getInvoice().getSettlementCase()) {
                                    case CARD -> request.getInvoice().getCard().getLast4();
                                    default -> "";
                                })
                                .setNetwork(switch (request.getInvoice().getSettlementCase()) {
                                    case CARD -> request.getInvoice().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = billingCreditNoteCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateCreditNotes(outbound0);

        var outbound1 = SimulateCreditNoteRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getInvoice().getBillingAddress().getRegion())
                                .build())
                        .setCard(CreditNoteCardSettlement.newBuilder()
                                .setNetwork(switch (reply0.getCreditNote().getSettlementCase()) {
                                    case CARD -> reply0.getCreditNote().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setDetails(SimulateCreditNoteRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getInvoice().getBillingAddress().getCountryCode())
                                .setPostalCode(reply0.getCreditNote().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var reply1 = billingCreditNoteCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateCreditNote(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainInvoiceDecision", request.getInvoice().getInvoiceId());
        return ExplainInvoiceDecisionResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCreditNote().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply1.getCreditNote().getBillingAddress().getRecipient())
                                .build())
                        .setInvoiceId(request.getInvoiceId())
                        .putAllMetadata(request.getInvoice().getMetadataMap())
                        .setNotes(reply0.getCreditNote().getNotes())
                        .build())
                .build();
    }

    /**
     * Scores an invoice.
     */
    public ScoreInvoiceResponse scoreInvoice(ScoreInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = SuggestMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestMetricSeriesesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoice().getAudit().getCreatedAt())
                        .setOwnerId(request.getInvoice().getCounterparty().getCustomerId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        RpcPort<SuggestMetricSeriesesRequest, SuggestMetricSeriesesResponse> port0 = RpcPort.adapt(metricsMetricSeriesQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::suggestMetricSerieses);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = EstimatePayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimatePayStubRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getInvoice().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getInvoice().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<EstimatePayStubResponse>();
        events.publishEvent(new ScoreInvoiceStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = BrowseBankTransfersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseBankTransfersRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getPayStub().getAudit().getCreatedAt())
                        .putAllLabels(request.getInvoice().getMetadataMap())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var reply2 = Optional.of(outbound2).map(value -> value.toBuilder().build())
                .map(value -> treasuryBankTransferQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseBankTransfers(value)).orElseThrow();

        var outbound3 = ScoreBillingAccountRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setCard(BillingAccountCardFunding.newBuilder()
                                .setHolderName(request.getInvoice().getCard().getHolderName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply3 = billingBillingAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreBillingAccount(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreInvoice", request.getInvoice().getInvoiceId());
        return ScoreInvoiceResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getPayStub().getAudit().getCreatedAt())
                                .build())
                        .setInvoiceId(request.getInvoiceId())
                        .setIssuedAt(request.getInvoice().getIssuedAt())
                        .setNumber(reply3.getBillingAccount().getHolder().getNumber())
                        .setTotal(reply1.getPayStub().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lints an invoice.
     */
    public LintInvoicesResponse lintInvoices(LintInvoicesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintInvoices")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var results0 = new ArrayList<SimulateCreditNoteResponse>();
        for (var element : request.getInvoice().getAttachmentsList()) {
            SimulateCreditNoteRequest itemRequest = SimulateCreditNoteRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .addAttachments(CreditNoteAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setContentType(element.getContentType())
                                .setSizeBytes(element.getSizeBytes())
                                .build())
                        .build())
                .build();
            results0.add(billingCreditNoteCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateCreditNote(itemRequest));
        }
        var reply0 = results0.isEmpty() ? SimulateCreditNoteResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = SummarizeMetricSeriesesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        final class Stage1Adapter {
            SummarizeMetricSeriesesResponse fetch(SummarizeMetricSeriesesRequest input) {
                return metricsMetricSeriesQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeMetricSerieses(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = FindNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindNotificationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoice().getAudit().getUpdatedAt())
                        .setRegion(reply0.getCreditNote().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = RpcSupport.retryRead(() -> notificationsNotificationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findNotifications(outbound2));

        var outbound3 = LintBillingAccountsRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setCard(BillingAccountCardFunding.newBuilder()
                                .setNetwork(request.getInvoice().getCard().getNetwork())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(LintBillingAccountsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply3 = RpcSupport.call(outbound3, value -> billingBillingAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintBillingAccounts(value));
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintInvoices", request.getInvoice().getInvoiceId());
        return LintInvoicesResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setInvoiceId(request.getInvoiceId())
                        .setStatus(request.getInvoice().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns an invoice.
     */
    public GetInvoiceRulesResponse getInvoiceRules(GetInvoiceRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetInvoiceRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Invoice", request.getInvoiceId(), "GetInvoiceRules");

        var outbound0 = CreateLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<CreateLedgerAccountRequest, CreateLedgerAccountResponse>() {
            @Override
            public CreateLedgerAccountResponse apply(CreateLedgerAccountRequest input) {
                return ledgerLedgerAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createLedgerAccount(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        return GetInvoiceRulesResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getLedgerAccount().getAudit().getCreatedBy())
                                .build())
                        .setInvoiceId(request.getInvoiceId())
                        .build())
                .build();
    }
}
