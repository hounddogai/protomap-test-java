package com.acme.finance.domains.expenses;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamAuditTrailExportRequest;
import com.acme.audit.v1.StreamAuditTrailExportResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateLine;
import com.acme.banking.v1.MandateLineTax;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.PreviewMandateRequest;
import com.acme.banking.v1.PreviewMandateResponse;
import com.acme.banking.v1.ScoreMandateRequest;
import com.acme.banking.v1.ScoreMandateResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.content.v1.ArticleServiceGrpc;
import com.acme.content.v1.SearchArticlesRequest;
import com.acme.content.v1.SearchArticlesResponse;
import com.acme.drivers.v1.DriverRef;
import com.acme.expenses.v1.CalculateExpensePolicyRequest;
import com.acme.expenses.v1.CalculateExpensePolicyResponse;
import com.acme.expenses.v1.CalculateExpenseReportRequest;
import com.acme.expenses.v1.CalculateExpenseReportResponse;
import com.acme.expenses.v1.CompareExpenseReportOptionsRequest;
import com.acme.expenses.v1.CompareExpenseReportOptionsResponse;
import com.acme.expenses.v1.EstimateExpensePolicyRequest;
import com.acme.expenses.v1.EstimateExpensePolicyResponse;
import com.acme.expenses.v1.EstimateExpenseReportRequest;
import com.acme.expenses.v1.EstimateExpenseReportResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExpenseReportLineTax;
import com.acme.expenses.v1.ExplainExpensePolicyCalculationRequest;
import com.acme.expenses.v1.ExplainExpensePolicyCalculationResponse;
import com.acme.expenses.v1.GetExpensePolicyRatesRequest;
import com.acme.expenses.v1.GetExpensePolicyRatesResponse;
import com.acme.expenses.v1.LintReceiptsRequest;
import com.acme.expenses.v1.LintReceiptsResponse;
import com.acme.expenses.v1.PreviewReceiptRequest;
import com.acme.expenses.v1.PreviewReceiptResponse;
import com.acme.expenses.v1.QuoteExpensePolicyRequest;
import com.acme.expenses.v1.QuoteExpensePolicyResponse;
import com.acme.expenses.v1.QuoteExpenseReportRequest;
import com.acme.expenses.v1.QuoteExpenseReportResponse;
import com.acme.expenses.v1.RecalculateExpensePoliciesRequest;
import com.acme.expenses.v1.RecalculateExpensePoliciesResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptBankSettlement;
import com.acme.expenses.v1.ReceiptLine;
import com.acme.expenses.v1.ReceiptLineTax;
import com.acme.expenses.v1.ReceiptValidationServiceGrpc;
import com.acme.expenses.v1.SimulateExpensePolicyRequest;
import com.acme.expenses.v1.SimulateExpensePolicyResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.ExplainClaimDecisionRequest;
import com.acme.insurance.v1.ExplainClaimDecisionResponse;
import com.acme.routing.v1.RecalculateRoutesRequest;
import com.acme.routing.v1.RecalculateRoutesResponse;
import com.acme.routing.v1.Route;
import com.acme.routing.v1.RouteCalculationServiceGrpc;
import com.acme.ticketing.v1.TicketServiceGrpc;
import com.acme.ticketing.v1.WatchTicketsRequest;
import com.acme.ticketing.v1.WatchTicketsResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of an expense policy, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class ExpensePolicyCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(ExpensePolicyCalculationApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ArticleServiceGrpc.ArticleServiceBlockingStub contentArticleServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceBlockingStub expensesExpenseReportCalculationServiceBlocking;

    private final ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceFutureStub expensesExpenseReportCalculationServiceFuture;

    private final ReceiptValidationServiceGrpc.ReceiptValidationServiceBlockingStub expensesReceiptValidationServiceBlocking;

    private final ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking;

    private final ExpensesRepository repository;

    private final RouteCalculationServiceGrpc.RouteCalculationServiceBlockingStub routingRouteCalculationServiceBlocking;

    private final TicketServiceGrpc.TicketServiceBlockingStub ticketingTicketServiceBlocking;

    public ExpensePolicyCalculationApplication(
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking,
            ChannelRegistry channels,
            ArticleServiceGrpc.ArticleServiceBlockingStub contentArticleServiceBlocking,
            ApplicationEventPublisher events,
            ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceBlockingStub expensesExpenseReportCalculationServiceBlocking,
            ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceFutureStub expensesExpenseReportCalculationServiceFuture,
            ReceiptValidationServiceGrpc.ReceiptValidationServiceBlockingStub expensesReceiptValidationServiceBlocking,
            ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking,
            ExpensesRepository repository,
            RouteCalculationServiceGrpc.RouteCalculationServiceBlockingStub routingRouteCalculationServiceBlocking,
            TicketServiceGrpc.TicketServiceBlockingStub ticketingTicketServiceBlocking) {
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditAuditTrailExportServiceBlocking = auditAuditTrailExportServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.bankingMandateValidationServiceBlocking = bankingMandateValidationServiceBlocking;
        this.channels = channels;
        this.contentArticleServiceBlocking = contentArticleServiceBlocking;
        this.events = events;
        this.expensesExpenseReportCalculationServiceBlocking = expensesExpenseReportCalculationServiceBlocking;
        this.expensesExpenseReportCalculationServiceFuture = expensesExpenseReportCalculationServiceFuture;
        this.expensesReceiptValidationServiceBlocking = expensesReceiptValidationServiceBlocking;
        this.insuranceClaimValidationServiceBlocking = insuranceClaimValidationServiceBlocking;
        this.repository = repository;
        this.routingRouteCalculationServiceBlocking = routingRouteCalculationServiceBlocking;
        this.ticketingTicketServiceBlocking = ticketingTicketServiceBlocking;
    }

    public record RecalculateExpensePoliciesStage1Requested(ScoreMandateRequest request, CompletableFuture<ScoreMandateResponse> completion, io.grpc.Context context) {}

    /**
     * Calculates an expense policy.
     */
    public CalculateExpensePolicyResponse calculateExpensePolicy(CalculateExpensePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateExpensePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = CalculateExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getExpensePolicy().getAudit().getCreatedAt())
                                .build())
                        .addLines(ExpenseReportLine.newBuilder()
                                .setTax(ExpenseReportLineTax.newBuilder()
                                        .setAmount(request.getExpensePolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending0 = expensesExpenseReportCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).calculateExpenseReport(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateAuditEventsResponse.getDefaultInstance());
        var input1 = auditAuditEventServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateAuditEvents(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getExpensePolicy().getAudit().getCreatedAt())
                                .setCreatedBy(request.getExpensePolicy().getAudit().getCreatedBy())
                                .setEtag(request.getExpensePolicy().getAudit().getEtag())
                                .setUpdatedAt(request.getExpensePolicy().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input2 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateExpensePolicy", request.getExpensePolicy().getExpensePolicyId());
        return CalculateExpensePolicyResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getExpenseReport().getAudit().getEtag())
                                .build())
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .setPriority(request.getExpensePolicy().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Estimates an expense policy.
     */
    public EstimateExpensePolicyResponse estimateExpensePolicy(EstimateExpensePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateExpensePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = LintReceiptsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintReceiptsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getExpensePolicy().getOwner().getEmail())
                                .build())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getExpensePolicy().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .addLines(ReceiptLine.newBuilder()
                                .setTax(ReceiptLineTax.newBuilder()
                                        .setAmount(request.getExpensePolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = expensesReceiptValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintReceipts(outbound0);

        var outbound1 = PreviewReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getExpensePolicy().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getReceipt().getAudit().getUpdatedAt())
                                .build())
                        .setBank(ReceiptBankSettlement.newBuilder()
                                .setIban(switch (reply0.getReceipt().getSettlementCase()) {
                                    case BANK -> reply0.getReceipt().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .addLines(ReceiptLine.newBuilder()
                                .setTax(ReceiptLineTax.newBuilder()
                                        .setAmount(request.getExpensePolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = expensesReceiptValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewReceipt(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateExpensePolicy", request.getExpensePolicy().getExpensePolicyId());
        return EstimateExpensePolicyResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getReceipt().getCounterparty().getDisplayName())
                                .build())
                        .setPriority(request.getExpensePolicy().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Quotes an expense policy.
     */
    public QuoteExpensePolicyResponse quoteExpensePolicy(QuoteExpensePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteExpensePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = WatchTicketsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchTicketsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getExpensePolicy().getAudit().getCreatedAt())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .setSince(request.getExpensePolicy().getAudit().getUpdatedAt())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getExpensePolicy().getOwner().getEmail())
                        .build())
                .build();
        var stream0 = ticketingTicketServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchTickets(outbound0);
        var reply0 = WatchTicketsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = CompareExpenseReportOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getExpensePolicy().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<CompareExpenseReportOptionsRequest, CompareExpenseReportOptionsResponse>() {
            @Override
            public CompareExpenseReportOptionsResponse apply(CompareExpenseReportOptionsRequest input) {
                return expensesExpenseReportCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareExpenseReportOptions(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteExpensePolicy", request.getExpensePolicy().getExpensePolicyId());
        return QuoteExpensePolicyResponse.newBuilder()
                .setBreakdown(QuoteExpensePolicyResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .setTotal(reply1.getExpenseReport().getTotal())
                        .build())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .build())
                .build();
    }

    /**
     * Recalculates an expense policy.
     */
    public RecalculateExpensePoliciesResponse recalculateExpensePolicies(RecalculateExpensePoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateExpensePolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = SearchArticlesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchArticlesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getExpensePolicy().getAudit().getUpdatedAt())
                        .putAllLabels(request.getExpensePolicy().getParametersMap())
                        .setOwnerId(request.getExpensePolicy().getOwner().getUserId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var adapter0 = new Function<SearchArticlesRequest, SearchArticlesResponse>() {
            @Override
            public SearchArticlesResponse apply(SearchArticlesRequest input) {
                return contentArticleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchArticles(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = ScoreMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .addLines(MandateLine.newBuilder()
                                .setTax(MandateLineTax.newBuilder()
                                        .setAmount(request.getExpensePolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<ScoreMandateResponse>();
        events.publishEvent(new RecalculateExpensePoliciesStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = BeanCopies.copy(PreviewMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getExpensePolicy().getAudit().getUpdatedAt())
                                .build())
                        .addLines(MandateLine.newBuilder()
                                .setTax(MandateLineTax.newBuilder()
                                        .setAmount(request.getExpensePolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build(), PreviewMandateRequest.class);
        var reply2 = bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewMandate(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateExpensePolicies", request.getExpensePolicy().getExpensePolicyId());
        return RecalculateExpensePoliciesResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getMandate().getAudit().getUpdatedBy())
                                .build())
                        .setDescription(request.getExpensePolicy().getDescription())
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Simulates an expense policy.
     */
    public SimulateExpensePolicyResponse simulateExpensePolicy(SimulateExpensePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateExpensePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = QuoteExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuoteExpenseReportRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .build())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.expenses.v1.ExpenseReportCalculationService");
        var selected0 = ExpenseReportCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.quoteExpenseReport(outbound0);

        var outbound1 = EstimateExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .addLines(ExpenseReportLine.newBuilder()
                                .setAmount(request.getExpensePolicy().getDiscount().getAmount())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<EstimateExpenseReportRequest, EstimateExpenseReportResponse>() {
            @Override
            public EstimateExpenseReportResponse apply(EstimateExpenseReportRequest input) {
                return expensesExpenseReportCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateExpenseReport(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateExpensePolicy", request.getExpensePolicy().getExpensePolicyId());
        return SimulateExpensePolicyResponse.newBuilder()
                .setBreakdown(SimulateExpensePolicyResponse.Breakdown.newBuilder()
                        .setTotal(reply1.getExpenseReport().getTotal())
                        .build())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getExpenseReport().getAudit().getUpdatedBy())
                                .build())
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .setPriority(request.getExpensePolicy().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Explains an expense policy.
     */
    public ExplainExpensePolicyCalculationResponse explainExpensePolicyCalculation(ExplainExpensePolicyCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainExpensePolicyCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = ExplainClaimDecisionRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getExpensePolicy().getOwner().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var adapter0 = new Function<ExplainClaimDecisionRequest, ExplainClaimDecisionResponse>() {
            @Override
            public ExplainClaimDecisionResponse apply(ExplainClaimDecisionRequest input) {
                return insuranceClaimValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainClaimDecision(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = StreamAuditTrailExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamAuditTrailExportRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getClaim().getAudit().getCreatedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(reply0.getClaim().getSla().getPolicyId())
                        .build())
                .build();
        var stream1 = auditAuditTrailExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditTrailExport(outbound1);
        var reply1 = StreamAuditTrailExportResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainExpensePolicyCalculation", request.getExpensePolicy().getExpensePolicyId());
        return ExplainExpensePolicyCalculationResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getClaim().getAudit().getCreatedAt())
                                .build())
                        .setDescription(request.getExpensePolicy().getDescription())
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .build())
                .build();
    }

    /**
     * Returns an expense policy.
     */
    public GetExpensePolicyRatesResponse getExpensePolicyRates(GetExpensePolicyRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetExpensePolicyRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpensePolicy", request.getExpensePolicyId(), "GetExpensePolicyRates");

        var outbound0 = JsonCodec.roundTrip(RecalculateRoutesRequest.newBuilder()
                .setContext(request.getContext())
                .setRoute(Route.newBuilder()
                        .setCounterparty(DriverRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setIssuedAt(request.getContext().getDeadline())
                        .putAllMetadata(request.getContext().getBaggageMap())
                        .build())
                .build(), RecalculateRoutesRequest.class);
        var reply0 = routingRouteCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateRoutes(outbound0);
        return GetExpensePolicyRatesResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getRoute().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
    }
}
