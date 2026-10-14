package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.ApproveBudgetLineRequest;
import com.acme.budgeting.v1.ApproveBudgetLineResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineReportingServiceGrpc;
import com.acme.budgeting.v1.CheckVarianceEligibilityRequest;
import com.acme.budgeting.v1.CheckVarianceEligibilityResponse;
import com.acme.budgeting.v1.CommentBudgetLineApprovalRequest;
import com.acme.budgeting.v1.CommentBudgetLineApprovalResponse;
import com.acme.budgeting.v1.CountVarianceTotalsRequest;
import com.acme.budgeting.v1.CountVarianceTotalsResponse;
import com.acme.budgeting.v1.DelegateBudgetLineApprovalRequest;
import com.acme.budgeting.v1.DelegateBudgetLineApprovalResponse;
import com.acme.budgeting.v1.DeleteBudgetLineReportRequest;
import com.acme.budgeting.v1.DeleteBudgetLineReportResponse;
import com.acme.budgeting.v1.EscalateBudgetLineRequest;
import com.acme.budgeting.v1.EscalateBudgetLineResponse;
import com.acme.budgeting.v1.FetchBudgetLineReportRequest;
import com.acme.budgeting.v1.FetchBudgetLineReportResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastBankSettlement;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.ForecastCardSettlement;
import com.acme.budgeting.v1.ForecastReportingServiceGrpc;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.GetBudgetLineApprovalStatusRequest;
import com.acme.budgeting.v1.GetBudgetLineApprovalStatusResponse;
import com.acme.budgeting.v1.ListPendingBudgetLinesRequest;
import com.acme.budgeting.v1.ListPendingBudgetLinesResponse;
import com.acme.budgeting.v1.QuoteForecastRequest;
import com.acme.budgeting.v1.QuoteForecastResponse;
import com.acme.budgeting.v1.RecallBudgetLineRequest;
import com.acme.budgeting.v1.RecallBudgetLineResponse;
import com.acme.budgeting.v1.RecallBudgetRequest;
import com.acme.budgeting.v1.RecallBudgetResponse;
import com.acme.budgeting.v1.RejectBudgetLineRequest;
import com.acme.budgeting.v1.RejectBudgetLineResponse;
import com.acme.budgeting.v1.ShareForecastReportRequest;
import com.acme.budgeting.v1.ShareForecastReportResponse;
import com.acme.budgeting.v1.SubmitBudgetLineRequest;
import com.acme.budgeting.v1.SubmitBudgetLineResponse;
import com.acme.budgeting.v1.SyncForecastsRequest;
import com.acme.budgeting.v1.SyncForecastsResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceReportingServiceGrpc;
import com.acme.budgeting.v1.VarianceValidationServiceGrpc;
import com.acme.cart.v1.Cart;
import com.acme.cart.v1.EstimateCartRequest;
import com.acme.cart.v1.EstimateCartResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerCalculationRequest;
import com.acme.customers.v1.ExplainCustomerCalculationResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.NotifyLedgerAccountWatchersRequest;
import com.acme.ledger.v1.NotifyLedgerAccountWatchersResponse;
import com.acme.merchandising.v1.SuggestCollectionsRequest;
import com.acme.merchandising.v1.SuggestCollectionsResponse;
import com.acme.suppliers.v1.CountSuppliersRequest;
import com.acme.suppliers.v1.CountSuppliersResponse;
import com.acme.suppliers.v1.ExportSuppliersRequest;
import com.acme.suppliers.v1.ExportSuppliersResponse;
import com.acme.suppliers.v1.SummarizeSuppliersRequest;
import com.acme.suppliers.v1.SummarizeSuppliersResponse;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
 * Routes budget lines through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class BudgetLineApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(BudgetLineApprovalApplication.class);

    private final BudgetLineReportingServiceGrpc.BudgetLineReportingServiceBlockingStub budgetingBudgetLineReportingServiceBlocking;

    private final ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking;

    private final ForecastReportingServiceGrpc.ForecastReportingServiceBlockingStub budgetingForecastReportingServiceBlocking;

    private final ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking;

    private final VarianceReportingServiceGrpc.VarianceReportingServiceBlockingStub budgetingVarianceReportingServiceBlocking;

    private final VarianceValidationServiceGrpc.VarianceValidationServiceBlockingStub budgetingVarianceValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking;

    private final BudgetingRepository repository;

    private final Executor rpcExecutor;

    private final SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking;

    public BudgetLineApprovalApplication(
            BudgetLineReportingServiceGrpc.BudgetLineReportingServiceBlockingStub budgetingBudgetLineReportingServiceBlocking,
            ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking,
            ForecastReportingServiceGrpc.ForecastReportingServiceBlockingStub budgetingForecastReportingServiceBlocking,
            ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking,
            VarianceReportingServiceGrpc.VarianceReportingServiceBlockingStub budgetingVarianceReportingServiceBlocking,
            VarianceValidationServiceGrpc.VarianceValidationServiceBlockingStub budgetingVarianceValidationServiceBlocking,
            ChannelRegistry channels,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking,
            ApplicationEventPublisher events,
            LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking,
            BudgetingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking) {
        this.budgetingBudgetLineReportingServiceBlocking = budgetingBudgetLineReportingServiceBlocking;
        this.budgetingForecastCalculationServiceBlocking = budgetingForecastCalculationServiceBlocking;
        this.budgetingForecastReportingServiceBlocking = budgetingForecastReportingServiceBlocking;
        this.budgetingForecastSyncServiceBlocking = budgetingForecastSyncServiceBlocking;
        this.budgetingVarianceReportingServiceBlocking = budgetingVarianceReportingServiceBlocking;
        this.budgetingVarianceValidationServiceBlocking = budgetingVarianceValidationServiceBlocking;
        this.channels = channels;
        this.customersCustomerCalculationServiceBlocking = customersCustomerCalculationServiceBlocking;
        this.events = events;
        this.ledgerLedgerAccountNotificationServiceBlocking = ledgerLedgerAccountNotificationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierQueryServiceBlocking = suppliersSupplierQueryServiceBlocking;
    }

    public record ListPendingBudgetLinesStage1Requested(SuggestCollectionsRequest request, CompletableFuture<SuggestCollectionsResponse> completion, io.grpc.Context context) {}

    public record RecallBudgetLineStage2Requested(FetchBudgetLineReportRequest request, CompletableFuture<FetchBudgetLineReportResponse> completion, io.grpc.Context context) {}

    public record CommentBudgetLineApprovalStage1Requested(EstimateCartRequest request, CompletableFuture<EstimateCartResponse> completion, io.grpc.Context context) {}

    /**
     * Submits a budget line.
     */
    public SubmitBudgetLineResponse submitBudgetLine(SubmitBudgetLineRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitBudgetLine")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = SyncForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SyncForecastsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getBudgetLine().getAudit().getEtag())
                                .setUpdatedAt(request.getBudgetLine().getAudit().getUpdatedAt())
                                .build())
                        .setBank(ForecastBankSettlement.newBuilder()
                                .setAccountHolder(request.getBudgetLine().getBank().getAccountHolder())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(budgetingForecastSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).syncForecasts(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitBudgetLine", request.getBudgetLine().getBudgetLineId());
        return SubmitBudgetLineResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getForecast().getBillingAddress().getRegion())
                                .build())
                        .setBudgetLineId(request.getBudgetLineId())
                        .setDueAt(request.getBudgetLine().getDueAt())
                        .build())
                .setOutcome(SubmitBudgetLineResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .build();
    }

    /**
     * Approves a budget line.
     */
    public ApproveBudgetLineResponse approveBudgetLine(ApproveBudgetLineRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveBudgetLine")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = RpcSupport.unpack(Any.pack(CheckVarianceEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setVariance(Variance.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getBudgetLine().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build()), CheckVarianceEligibilityRequest.class);
        var reply0 = budgetingVarianceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkVarianceEligibility(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveBudgetLine", request.getBudgetLine().getBudgetLineId());
        return ApproveBudgetLineResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply0.getVariance().getRequester().getEmail())
                                .build())
                        .build())
                .setOutcome(ApproveBudgetLineResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Rejects a budget line.
     */
    public RejectBudgetLineResponse rejectBudgetLine(RejectBudgetLineRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectBudgetLine")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = ExplainCustomerCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setRelationshipManager(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCustomerCalculation(outbound0), rpcExecutor);

        var outbound1 = SummarizeSuppliersRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setSupplierId(request.getBudgetLine().getCounterparty().getSupplierId())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeSuppliers(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectBudgetLine", request.getBudgetLine().getBudgetLineId());
        return RejectBudgetLineResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getCustomer().getAudit().getCreatedAt())
                                .build())
                        .setBudgetLineId(request.getBudgetLineId())
                        .putAllMetadata(request.getBudgetLine().getMetadataMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Escalates a budget line.
     */
    public EscalateBudgetLineResponse escalateBudgetLine(EscalateBudgetLineRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateBudgetLine")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = QuoteForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuoteForecastRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getBudgetLine().getBillingAddress().getCountryCode())
                                .addAllLines(request.getBudgetLine().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getBudgetLine().getAudit().getCreatedAt())
                                .build())
                        .setCard(ForecastCardSettlement.newBuilder()
                                .setHolderName(switch (request.getBudgetLine().getSettlementCase()) {
                                    case CARD -> request.getBudgetLine().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = budgetingForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteForecast(outbound0);

        var outbound1 = CountVarianceTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountVarianceTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getForecast().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getForecast().getMetadataMap())
                        .setOwnerId(request.getDetails().getApprover().getUserId())
                        .setRegion(request.getBudgetLine().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = RpcSupport.retryRead(() -> budgetingVarianceReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countVarianceTotals(outbound1));
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateBudgetLine", request.getBudgetLine().getBudgetLineId());
        return EscalateBudgetLineResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .setIssuedAt(reply0.getForecast().getIssuedAt())
                        .setTotal(request.getBudgetLine().getTotal())
                        .build())
                .setOutcome(EscalateBudgetLineResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists budget lines a page at a time.
     */
    public ListPendingBudgetLinesResponse listPendingBudgetLines(ListPendingBudgetLinesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingBudgetLines")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getContext().getTraceId(), "ListPendingBudgetLines");

        var outbound0 = NotifyLedgerAccountWatchersRequest.newBuilder()
                .putAllAnnotations(request.getContext().getBaggageMap())
                .setContext(request.getContext())
                .setDetails(NotifyLedgerAccountWatchersRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllVariables(request.getFilter().getLabelsMap())
                        .build())
                .build();
        RpcPort<NotifyLedgerAccountWatchersRequest, NotifyLedgerAccountWatchersResponse> port0 = RpcPort.adapt(ledgerLedgerAccountNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::notifyLedgerAccountWatchers);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = SuggestCollectionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestCollectionsRequest.Filter.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .build();
        var completion1 = new CompletableFuture<SuggestCollectionsResponse>();
        events.publishEvent(new ListPendingBudgetLinesStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingBudgetLines", request.getPage().getPageToken());
        return ListPendingBudgetLinesResponse.newBuilder()
                .setPageInfo(PageInfo.newBuilder()
                        .setNextPageToken(reply1.getPageInfo().getNextPageToken())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Delegates a budget line.
     */
    public DelegateBudgetLineApprovalResponse delegateBudgetLineApproval(DelegateBudgetLineApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateBudgetLineApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = ExportSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportSuppliersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBudgetLine().getAudit().getUpdatedAt())
                        .putAllLabels(request.getBudgetLine().getMetadataMap())
                        .setOwnerId(request.getBudgetLine().getCounterparty().getSupplierId())
                        .setRegion(request.getBudgetLine().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream0 = suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportSuppliers(outbound0);
        var reply0 = ExportSuppliersResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateBudgetLineApproval", request.getBudgetLine().getBudgetLineId());
        return DelegateBudgetLineApprovalResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .setCounterparty(request.getBudgetLine().getCounterparty())
                        .build())
                .build();
    }

    /**
     * Recalls a budget line.
     */
    public RecallBudgetLineResponse recallBudgetLine(RecallBudgetLineRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallBudgetLine")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = ShareForecastReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareForecastReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getBudgetLine().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> budgetingForecastReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareForecastReport(value));

        var outbound1 = DeleteBudgetLineReportRequest.newBuilder()
                .setBudgetLineId(request.getBudgetLine().getBudgetLineId())
                .setContext(request.getContext())
                .setEtag(reply0.getForecast().getAudit().getEtag())
                .setReason(request.getReason())
                .build();
        var adapter1 = new Function<DeleteBudgetLineReportRequest, DeleteBudgetLineReportResponse>() {
            @Override
            public DeleteBudgetLineReportResponse apply(DeleteBudgetLineReportRequest input) {
                return budgetingBudgetLineReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).deleteBudgetLineReport(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);

        var outbound2 = FetchBudgetLineReportRequest.newBuilder()
                .setBudgetLineId(request.getBudgetLine().getBudgetLineId())
                .setContext(request.getContext())
                .build();
        var completion2 = new CompletableFuture<FetchBudgetLineReportResponse>();
        events.publishEvent(new RecallBudgetLineStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getCounterparty().getSupplierId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallBudgetLine", request.getBudgetLine().getCounterparty().getSupplierId());
        return RecallBudgetLineResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getForecast().getAudit().getCreatedBy())
                                .setUpdatedBy(reply2.getBudgetLine().getAudit().getUpdatedBy())
                                .build())
                        .setBudgetLineId(request.getBudgetLineId())
                        .setNumber(request.getBudgetLine().getNumber())
                        .build())
                .setOutcome(RecallBudgetLineResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns a budget line.
     */
    public GetBudgetLineApprovalStatusResponse getBudgetLineApprovalStatus(GetBudgetLineApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetBudgetLineApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getBudgetLineId(), "GetBudgetLineApprovalStatus");

        var outbound0 = CountSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountSuppliersRequest.Filter.newBuilder()
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getBudgetLineId())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.suppliers.v1.SupplierQueryService/CountSuppliers", outbound0);
        var reply0 = suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countSuppliers(command0.payload());
        return GetBudgetLineApprovalStatusResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Comments a budget line.
     */
    public CommentBudgetLineApprovalResponse commentBudgetLineApproval(CommentBudgetLineApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentBudgetLineApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = CountSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountSuppliersRequest.Filter.newBuilder()
                        .setRegion(request.getBudgetLine().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countSuppliers(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = EstimateCartRequest.newBuilder()
                .setCart(Cart.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getBudgetLine().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(EstimateCartRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getBudgetLine().getBillingAddress().getLocality())
                                .setPostalCode(request.getBudgetLine().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<EstimateCartResponse>();
        events.publishEvent(new CommentBudgetLineApprovalStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = RecallBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setCard(BudgetCardFunding.newBuilder()
                                .setToken(request.getBudgetLine().getCard().getToken())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel2 = channels.lookup("acme.budgeting.v1.BudgetApprovalService");
        var serviceName2 = "acme.budgeting.v1" + "." + "BudgetApprovalService";
        var methodName2 = "Rec" + "allBudget";
        var descriptor2 = MethodDescriptor.<RecallBudgetRequest, RecallBudgetResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(RecallBudgetRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(RecallBudgetResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentBudgetLineApproval", request.getBudgetLine().getBudgetLineId());
        return CommentBudgetLineApprovalResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply1.getCart().getBillingAddress().getLinesList())
                                .build())
                        .setBudgetLineId(request.getBudgetLineId())
                        .setDueAt(reply1.getCart().getDueAt())
                        .setTotal(request.getBudgetLine().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
