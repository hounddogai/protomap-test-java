package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.NotifyAuditTrailWatchersRequest;
import com.acme.audit.v1.NotifyAuditTrailWatchersResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLimit;
import com.acme.budgeting.v1.BudgetQueryServiceGrpc;
import com.acme.budgeting.v1.QuoteBudgetRequest;
import com.acme.budgeting.v1.QuoteBudgetResponse;
import com.acme.budgeting.v1.RecalculateBudgetsRequest;
import com.acme.budgeting.v1.RecalculateBudgetsResponse;
import com.acme.budgeting.v1.ResolveBudgetsRequest;
import com.acme.budgeting.v1.ResolveBudgetsResponse;
import com.acme.collections.v1.CollectionCaseQueryServiceGrpc;
import com.acme.collections.v1.ExportCollectionCasesRequest;
import com.acme.collections.v1.ExportCollectionCasesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.FindContractClausesRequest;
import com.acme.contracts.v1.FindContractClausesResponse;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.SuggestDocumentsRequest;
import com.acme.documents.v1.SuggestDocumentsResponse;
import com.acme.expenses.v1.CalculateExpenseReportRequest;
import com.acme.expenses.v1.CalculateExpenseReportResponse;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportBankSettlement;
import com.acme.expenses.v1.ReceiptQueryServiceGrpc;
import com.acme.expenses.v1.SuggestReceiptsRequest;
import com.acme.expenses.v1.SuggestReceiptsResponse;
import com.acme.featureflags.v1.CloneFeatureFlagRequest;
import com.acme.featureflags.v1.CloneFeatureFlagResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
import com.acme.featureflags.v1.SuggestFeatureFlagsRequest;
import com.acme.featureflags.v1.SuggestFeatureFlagsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.BatchGetUsersRequest;
import com.acme.identity.v1.BatchGetUsersResponse;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountBankFunding;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.RevertLedgerAccountRequest;
import com.acme.ledger.v1.RevertLedgerAccountResponse;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeRequest;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeResponse;
import com.acme.ledger.v1.UnsubscribeLedgerAccountUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeLedgerAccountUpdatesResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.PreviewLoanRequest;
import com.acme.lending.v1.PreviewLoanResponse;
import com.acme.performance.v1.ReviewQueryServiceGrpc;
import com.acme.performance.v1.SummarizeReviewsRequest;
import com.acme.performance.v1.SummarizeReviewsResponse;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsRequest;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsResponse;
import com.acme.procurement.v1.ExportPurchaseRequisitionsRequest;
import com.acme.procurement.v1.ExportPurchaseRequisitionsResponse;
import com.acme.procurement.v1.PurchaseRequisitionQueryServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.acme.risk.v1.CheckRiskRegisterEligibilityRequest;
import com.acme.risk.v1.CheckRiskRegisterEligibilityResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.treasury.v1.CalculateCashPositionRequest;
import com.acme.treasury.v1.CalculateCashPositionResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionLimit;
import com.acme.treasury.v1.CompareCashPositionOptionsRequest;
import com.acme.treasury.v1.CompareCashPositionOptionsResponse;
import com.acme.treasury.v1.EstimateCashPositionRequest;
import com.acme.treasury.v1.EstimateCashPositionResponse;
import com.acme.treasury.v1.ExplainCashPositionCalculationRequest;
import com.acme.treasury.v1.ExplainCashPositionCalculationResponse;
import com.acme.treasury.v1.FxHedgeRef;
import com.acme.treasury.v1.GetCashPositionRatesRequest;
import com.acme.treasury.v1.GetCashPositionRatesResponse;
import com.acme.treasury.v1.QuoteCashPositionRequest;
import com.acme.treasury.v1.QuoteCashPositionResponse;
import com.acme.treasury.v1.SimulateCashPositionRequest;
import com.acme.treasury.v1.SimulateCashPositionResponse;
import com.acme.venues.v1.ValidateVenueRequest;
import com.acme.venues.v1.ValidateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a cash position, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class CashPositionCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(CashPositionCalculationApplication.class);

    private final AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking;

    private final BudgetQueryServiceGrpc.BudgetQueryServiceBlockingStub budgetingBudgetQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final CollectionCaseQueryServiceGrpc.CollectionCaseQueryServiceBlockingStub collectionsCollectionCaseQueryServiceBlocking;

    private final DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ReceiptQueryServiceGrpc.ReceiptQueryServiceBlockingStub expensesReceiptQueryServiceBlocking;

    private final FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking;

    private final LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking;

    private final CashPositionCalculationRequestMapper mapper;

    private final ReviewQueryServiceGrpc.ReviewQueryServiceBlockingStub performanceReviewQueryServiceBlocking;

    private final PurchaseRequisitionQueryServiceGrpc.PurchaseRequisitionQueryServiceBlockingStub procurementPurchaseRequisitionQueryServiceBlocking;

    private final PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceStub procurementPurchaseRequisitionServiceAsync;

    private final TreasuryRepository repository;

    private final RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking;

    private final VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking;

    public CashPositionCalculationApplication(
            AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking,
            BudgetQueryServiceGrpc.BudgetQueryServiceBlockingStub budgetingBudgetQueryServiceBlocking,
            ChannelRegistry channels,
            CollectionCaseQueryServiceGrpc.CollectionCaseQueryServiceBlockingStub collectionsCollectionCaseQueryServiceBlocking,
            DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking,
            ApplicationEventPublisher events,
            ReceiptQueryServiceGrpc.ReceiptQueryServiceBlockingStub expensesReceiptQueryServiceBlocking,
            FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking,
            LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking,
            CashPositionCalculationRequestMapper mapper,
            ReviewQueryServiceGrpc.ReviewQueryServiceBlockingStub performanceReviewQueryServiceBlocking,
            PurchaseRequisitionQueryServiceGrpc.PurchaseRequisitionQueryServiceBlockingStub procurementPurchaseRequisitionQueryServiceBlocking,
            PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceStub procurementPurchaseRequisitionServiceAsync,
            TreasuryRepository repository,
            RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking,
            VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking) {
        this.auditAuditTrailNotificationServiceBlocking = auditAuditTrailNotificationServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.budgetingBudgetCalculationServiceBlocking = budgetingBudgetCalculationServiceBlocking;
        this.budgetingBudgetQueryServiceBlocking = budgetingBudgetQueryServiceBlocking;
        this.channels = channels;
        this.collectionsCollectionCaseQueryServiceBlocking = collectionsCollectionCaseQueryServiceBlocking;
        this.documentsDocumentQueryServiceBlocking = documentsDocumentQueryServiceBlocking;
        this.events = events;
        this.expensesReceiptQueryServiceBlocking = expensesReceiptQueryServiceBlocking;
        this.featureflagsFeatureFlagQueryServiceBlocking = featureflagsFeatureFlagQueryServiceBlocking;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.ledgerLedgerAccountHistoryServiceBlocking = ledgerLedgerAccountHistoryServiceBlocking;
        this.ledgerLedgerAccountNotificationServiceBlocking = ledgerLedgerAccountNotificationServiceBlocking;
        this.mapper = mapper;
        this.performanceReviewQueryServiceBlocking = performanceReviewQueryServiceBlocking;
        this.procurementPurchaseRequisitionQueryServiceBlocking = procurementPurchaseRequisitionQueryServiceBlocking;
        this.procurementPurchaseRequisitionServiceAsync = procurementPurchaseRequisitionServiceAsync;
        this.repository = repository;
        this.riskRiskRegisterValidationServiceBlocking = riskRiskRegisterValidationServiceBlocking;
        this.venuesVenueValidationServiceBlocking = venuesVenueValidationServiceBlocking;
    }

    public record CalculateCashPositionStage2Requested(CalculateExpenseReportRequest request, CompletableFuture<CalculateExpenseReportResponse> completion, io.grpc.Context context) {}

    public record QuoteCashPositionStage1Requested(PreviewLoanRequest request, CompletableFuture<PreviewLoanResponse> completion, io.grpc.Context context) {}

    public record SimulateCashPositionStage0Requested(FindContractClausesRequest request, CompletableFuture<FindContractClausesResponse> completion, io.grpc.Context context) {}

    /**
     * Calculates a cash position.
     */
    public CalculateCashPositionResponse calculateCashPosition(CalculateCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var builder0 = SuggestDocumentsRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setFilter(SuggestDocumentsRequest.Filter.newBuilder()
                .putAllLabels(request.getCashPosition().getLabelsMap())
                .build());
        var outbound0 = builder0.build();
        var reply0 = documentsDocumentQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestDocuments(outbound0);

        var outbound1 = CloneFeatureFlagRequest.newBuilder()
                .setContext(request.getContext())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getCashPosition().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getCashPosition().getHolder().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.featureflags.v1.FeatureFlagService");
        var serviceName1 = "acme.featureflags.v1" + "." + "FeatureFlagService";
        var methodName1 = "Clo" + "neFeatureFlag";
        var descriptor1 = MethodDescriptor.<CloneFeatureFlagRequest, CloneFeatureFlagResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(CloneFeatureFlagRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CloneFeatureFlagResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var outbound2 = CalculateExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setBank(ExpenseReportBankSettlement.newBuilder()
                                .setIban(request.getCashPosition().getBank().getIban())
                                .build())
                        .build())
                .build();
        var completion2 = new CompletableFuture<CalculateExpenseReportResponse>();
        events.publishEvent(new CalculateCashPositionStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound3 = SendLedgerAccountTestNoticeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SendLedgerAccountTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setPhone(request.getCashPosition().getContact().getPhone())
                                .setPreferredChannel(request.getCashPosition().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        final class Stage3Adapter {
            SendLedgerAccountTestNoticeResponse fetch(SendLedgerAccountTestNoticeRequest input) {
                return ledgerLedgerAccountNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sendLedgerAccountTestNotice(input);
            }
        }
        var reply3 = new Stage3Adapter().fetch(outbound3);

        var outbound4 = BatchCreatePurchaseRequisitionsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreatePurchaseRequisitionsRequest.StreamHeader.newBuilder()
                        .setSource(request.getCashPosition().getCashPositionId())
                        .setSubmittedBy(request.getCashPosition().getContact())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(BatchCreatePurchaseRequisitionsResponse.getDefaultInstance());
        var input4 = procurementPurchaseRequisitionServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreatePurchaseRequisitions(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();

        var outbound5 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCashPosition().getAudit().getCreatedAt())
                                .setCreatedBy(request.getCashPosition().getAudit().getCreatedBy())
                                .setEtag(request.getCashPosition().getAudit().getEtag())
                                .setUpdatedAt(request.getCashPosition().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector5 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input5 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector5);
        input5.onNext(outbound5);
        input5.onCompleted();
        var reply5 = collector5.await();
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getHolder().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateCashPosition", request.getCashPosition().getHolder().getFxHedgeId());
        return CalculateCashPositionResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getFeatureFlag().getAudit().getEtag())
                                .build())
                        .setCashPositionId(request.getCashPositionId())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply3.getLedgerAccount().getContact().getEmail())
                                .build())
                        .setHolder(FxHedgeRef.newBuilder()
                                .setDisplayName(reply2.getExpenseReport().getCounterparty().getDisplayName())
                                .build())
                        .putAllLabels(request.getCashPosition().getLabelsMap())
                        .addLimits(CashPositionLimit.newBuilder()
                                .setAmount(reply1.getFeatureFlag().getDiscount().getAmount())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Estimates a cash position.
     */
    public EstimateCashPositionResponse estimateCashPosition(EstimateCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var outbound0 = ResolveBudgetsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply0 = budgetingBudgetQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveBudgets(outbound0);

        var results1 = new ArrayList<QuoteBudgetResponse>();
        for (var element : request.getCashPosition().getLimitsList()) {
            QuoteBudgetRequest itemRequest = QuoteBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .addLimits(BudgetLimit.newBuilder()
                                .setName(element.getName())
                                .setPeriod(element.getPeriod())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
            results1.add(budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteBudget(itemRequest));
        }
        var reply1 = results1.isEmpty() ? QuoteBudgetResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = UnsubscribeLedgerAccountUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(UnsubscribeLedgerAccountUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setName(reply1.getBudget().getContact().getName())
                                .build())
                        .build())
                .build();
        RpcPort<UnsubscribeLedgerAccountUpdatesRequest, UnsubscribeLedgerAccountUpdatesResponse> port2 = RpcPort.adapt(ledgerLedgerAccountNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::unsubscribeLedgerAccountUpdates);
        var reply2 = port2.exchange(outbound2);

        var outbound3 = BatchGetUsersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply3 = RpcSupport.retryRead(() -> identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetUsers(outbound3));
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getCashPositionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateCashPosition", request.getCashPosition().getCashPositionId());
        return EstimateCashPositionResponse.newBuilder()
                .setBreakdown(EstimateCashPositionResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply2.getLedgerAccount().getCurrencyCode())
                        .build())
                .setCashPosition(CashPosition.newBuilder()
                        .setAvailable(request.getCashPosition().getAvailable())
                        .setCashPositionId(request.getCashPositionId())
                        .setOpenedAt(reply2.getLedgerAccount().getOpenedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Quotes a cash position.
     */
    public QuoteCashPositionResponse quoteCashPosition(QuoteCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var outbound0 = (SuggestReceiptsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestReceiptsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCashPosition().getAudit().getUpdatedAt())
                        .putAllLabels(request.getCashPosition().getLabelsMap())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build()).toBuilder().build();
        var reply0 = expensesReceiptQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestReceipts(outbound0);

        var outbound1 = PreviewLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setDisplayName(request.getCashPosition().getHolder().getDisplayName())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<PreviewLoanResponse>();
        events.publishEvent(new QuoteCashPositionStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = mapper.quoteCashPositionStage2(request, reply1);
        var reply2 = venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateVenue(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getCashPositionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteCashPosition", request.getCashPosition().getCashPositionId());
        return QuoteCashPositionResponse.newBuilder()
                .setBreakdown(QuoteCashPositionResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply1.getLoan().getCurrencyCode())
                        .build())
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getVenue().getAudit().getUpdatedBy())
                                .build())
                        .setCashPositionId(request.getCashPositionId())
                        .setStatus(request.getCashPosition().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Compares a cash position.
     */
    public CompareCashPositionOptionsResponse compareCashPositionOptions(CompareCashPositionOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareCashPositionOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var outbound0 = ExportPurchaseRequisitionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportPurchaseRequisitionsRequest.Filter.newBuilder()
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var stream0 = procurementPurchaseRequisitionQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportPurchaseRequisitions(outbound0);
        var reply0 = ExportPurchaseRequisitionsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        List<CheckRiskRegisterEligibilityResponse> results1 = request.getCashPosition().getLimitsList().stream()
                .map(element -> CheckRiskRegisterEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .addChildren(RiskRegister.newBuilder()
                                                .setName(element.getName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkRiskRegisterEligibility(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? CheckRiskRegisterEligibilityResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = CloneFeatureFlagRequest.newBuilder()
                .setContext(request.getContext())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getCashPosition().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getCashPosition().getHolder().getDisplayName())
                                .setEmail(request.getCashPosition().getContact().getEmail())
                                .setUserId(reply1.getRiskRegister().getManager().getUserId())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.featureflags.v1.FeatureFlagService");
        var serviceName2 = "acme.featureflags.v1" + "." + "FeatureFlagService";
        var methodName2 = "Clo" + "neFeatureFlag";
        var descriptor2 = MethodDescriptor.<CloneFeatureFlagRequest, CloneFeatureFlagResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(CloneFeatureFlagRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CloneFeatureFlagResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = RpcSupport.unpack(Any.pack(NotifyAuditTrailWatchersRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NotifyAuditTrailWatchersRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setPhone(request.getCashPosition().getContact().getPhone())
                                .build())
                        .build())
                .build()), NotifyAuditTrailWatchersRequest.class);
        var reply3 = auditAuditTrailNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).notifyAuditTrailWatchers(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getCashPositionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareCashPositionOptions", request.getCashPosition().getCashPositionId());
        return CompareCashPositionOptionsResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getFeatureFlag().getAudit().getCreatedAt())
                                .setUpdatedBy(reply2.getFeatureFlag().getAudit().getUpdatedBy())
                                .build())
                        .setCashPositionId(request.getCashPositionId())
                        .putAllLabels(request.getCashPosition().getLabelsMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply3.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Simulates a cash position.
     */
    public SimulateCashPositionResponse simulateCashPosition(SimulateCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var outbound0 = FindContractClausesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindContractClausesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCashPosition().getOpenedAt())
                        .putAllLabels(request.getCashPosition().getLabelsMap())
                        .setOwnerId(request.getCashPosition().getHolder().getFxHedgeId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var completion0 = new CompletableFuture<FindContractClausesResponse>();
        events.publishEvent(new SimulateCashPositionStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getCashPositionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateCashPosition", request.getCashPosition().getCashPositionId());
        return SimulateCashPositionResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCashPositionId(request.getCashPositionId())
                        .setContact(request.getCashPosition().getContact())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Explains a cash position.
     */
    public ExplainCashPositionCalculationResponse explainCashPositionCalculation(ExplainCashPositionCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainCashPositionCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var outbound0 = RecalculateBudgetsRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getCashPosition().getAudit().getEtag())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getCashPosition().getContact().getPhone())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateBudgets(outbound0);

        var outbound1 = SuggestFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestFeatureFlagsRequest.Filter.newBuilder()
                        .putAllLabels(request.getCashPosition().getLabelsMap())
                        .build())
                .build();
        var adapter1 = new Function<SuggestFeatureFlagsRequest, SuggestFeatureFlagsResponse>() {
            @Override
            public SuggestFeatureFlagsResponse apply(SuggestFeatureFlagsRequest input) {
                return featureflagsFeatureFlagQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestFeatureFlags(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);

        var outbound2 = RevertLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RevertLedgerAccountRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getCashPosition().getAudit().getUpdatedAt())
                                .build())
                        .setBank(LedgerAccountBankFunding.newBuilder()
                                .setIban(switch (reply0.getBudget().getFundingCase()) {
                                    case BANK -> reply0.getBudget().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = ledgerLedgerAccountHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertLedgerAccount(outbound2);

        var outbound3 = ExportCollectionCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportCollectionCasesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCashPosition().getAudit().getCreatedAt())
                        .putAllLabels(reply2.getLedgerAccount().getLabelsMap())
                        .setOwnerId(request.getCashPosition().getHolder().getFxHedgeId())
                        .build())
                .build();
        var stream3 = collectionsCollectionCaseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportCollectionCases(outbound3);
        var reply3 = ExportCollectionCasesResponse.getDefaultInstance();
        while (stream3.hasNext()) {
            reply3 = stream3.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getCashPositionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainCashPositionCalculation", request.getCashPosition().getCashPositionId());
        return ExplainCashPositionCalculationResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getBudget().getAudit().getEtag())
                                .build())
                        .setBalance(request.getCashPosition().getBalance())
                        .setCashPositionId(request.getCashPositionId())
                        .build())
                .setOutcome(ExplainCashPositionCalculationResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a cash position.
     */
    public GetCashPositionRatesResponse getCashPositionRates(GetCashPositionRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetCashPositionRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getCashPositionId(), "GetCashPositionRates");

        var outbound0 = SummarizeReviewsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SummarizeReviewsRequest.Details.newBuilder()
                        .putAllFilters(request.getContext().getBaggageMap())
                        .build())
                .build();
        final class Stage0Adapter {
            SummarizeReviewsResponse fetch(SummarizeReviewsRequest input) {
                return performanceReviewQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeReviews(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        return GetCashPositionRatesResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCashPositionId(request.getCashPositionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
