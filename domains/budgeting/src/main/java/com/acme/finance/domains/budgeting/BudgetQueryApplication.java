package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RevertAuditTrailRequest;
import com.acme.audit.v1.RevertAuditTrailResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.budgeting.v1.BrowseBudgetsRequest;
import com.acme.budgeting.v1.BrowseBudgetsResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetLimit;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLineCardSettlement;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.CalculateVarianceRequest;
import com.acme.budgeting.v1.CalculateVarianceResponse;
import com.acme.budgeting.v1.CheckBudgetLineEligibilityRequest;
import com.acme.budgeting.v1.CheckBudgetLineEligibilityResponse;
import com.acme.budgeting.v1.CompareVarianceOptionsRequest;
import com.acme.budgeting.v1.CompareVarianceOptionsResponse;
import com.acme.budgeting.v1.CountBudgetsRequest;
import com.acme.budgeting.v1.CountBudgetsResponse;
import com.acme.budgeting.v1.ExplainForecastDecisionRequest;
import com.acme.budgeting.v1.ExplainForecastDecisionResponse;
import com.acme.budgeting.v1.ExportBudgetsRequest;
import com.acme.budgeting.v1.ExportBudgetsResponse;
import com.acme.budgeting.v1.FindBudgetsRequest;
import com.acme.budgeting.v1.FindBudgetsResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.ForecastValidationServiceGrpc;
import com.acme.budgeting.v1.GetBudgetRulesRequest;
import com.acme.budgeting.v1.GetBudgetRulesResponse;
import com.acme.budgeting.v1.LookupBudgetRequest;
import com.acme.budgeting.v1.LookupBudgetResponse;
import com.acme.budgeting.v1.PeekBudgetRequest;
import com.acme.budgeting.v1.PeekBudgetResponse;
import com.acme.budgeting.v1.QuoteBudgetLineRequest;
import com.acme.budgeting.v1.QuoteBudgetLineResponse;
import com.acme.budgeting.v1.ResolveBudgetsRequest;
import com.acme.budgeting.v1.ResolveBudgetsResponse;
import com.acme.budgeting.v1.SuggestBudgetsRequest;
import com.acme.budgeting.v1.SuggestBudgetsResponse;
import com.acme.budgeting.v1.SummarizeBudgetsRequest;
import com.acme.budgeting.v1.SummarizeBudgetsResponse;
import com.acme.budgeting.v1.VarianceCalculationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.EstimateControlRequest;
import com.acme.compliance.v1.EstimateControlResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.PreviewClaimRequest;
import com.acme.insurance.v1.PreviewClaimResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.SendFiscalPeriodTestNoticeRequest;
import com.acme.ledger.v1.SendFiscalPeriodTestNoticeResponse;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesRequest;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesResponse;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.SimulateLoanApplicationRequest;
import com.acme.lending.v1.SimulateLoanApplicationResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
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
 * Answers read-only questions about budgets, for screens and reports. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class BudgetQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(BudgetQueryApplication.class);

    private final FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync;

    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking;

    private final BudgetLineValidationServiceGrpc.BudgetLineValidationServiceBlockingStub budgetingBudgetLineValidationServiceBlocking;

    private final ForecastValidationServiceGrpc.ForecastValidationServiceBlockingStub budgetingForecastValidationServiceBlocking;

    private final VarianceCalculationServiceGrpc.VarianceCalculationServiceBlockingStub budgetingVarianceCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking;

    private final FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking;

    private final JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking;

    private final LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking;

    private final BudgetQueryRequestMapper mapper;

    private final BudgetingRepository repository;

    public BudgetQueryApplication(
            FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync,
            AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking,
            BudgetLineValidationServiceGrpc.BudgetLineValidationServiceBlockingStub budgetingBudgetLineValidationServiceBlocking,
            ForecastValidationServiceGrpc.ForecastValidationServiceBlockingStub budgetingForecastValidationServiceBlocking,
            VarianceCalculationServiceGrpc.VarianceCalculationServiceBlockingStub budgetingVarianceCalculationServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking,
            FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking,
            JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking,
            LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking,
            BudgetQueryRequestMapper mapper,
            BudgetingRepository repository) {
        this.assetsFixedAssetServiceAsync = assetsFixedAssetServiceAsync;
        this.auditAuditTrailHistoryServiceBlocking = auditAuditTrailHistoryServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.budgetingBudgetLineCalculationServiceBlocking = budgetingBudgetLineCalculationServiceBlocking;
        this.budgetingBudgetLineValidationServiceBlocking = budgetingBudgetLineValidationServiceBlocking;
        this.budgetingForecastValidationServiceBlocking = budgetingForecastValidationServiceBlocking;
        this.budgetingVarianceCalculationServiceBlocking = budgetingVarianceCalculationServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.insuranceClaimValidationServiceBlocking = insuranceClaimValidationServiceBlocking;
        this.ledgerFiscalPeriodNotificationServiceBlocking = ledgerFiscalPeriodNotificationServiceBlocking;
        this.ledgerJournalEntryNotificationServiceBlocking = ledgerJournalEntryNotificationServiceBlocking;
        this.lendingLoanApplicationCalculationServiceBlocking = lendingLoanApplicationCalculationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
    }

    public record PeekBudgetStage0Requested(EstimateControlRequest request, CompletableFuture<EstimateControlResponse> completion, io.grpc.Context context) {}

    /**
     * Returns a budget.
     */
    public LookupBudgetResponse lookupBudget(LookupBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getBudgetId(), "LookupBudget");

        var outbound0 = BatchCreateFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateFixedAssetsRequest.StreamHeader.newBuilder()
                        .setSource(request.getBudgetId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector0 = new StreamCollector<>(BatchCreateFixedAssetsResponse.getDefaultInstance());
        var input0 = assetsFixedAssetServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateFixedAssets(collector0);
        input0.onNext(outbound0);
        input0.onCompleted();
        var reply0 = collector0.await();

        var outbound1 = StreamRetentionPolicyChangesRequest.newBuilder()
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
        var collector1 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input1 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();
        return LookupBudgetResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .build())
                .build();
    }

    /**
     * Lists budgets a page at a time.
     */
    public BrowseBudgetsResponse browseBudgets(BrowseBudgetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseBudgets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getContext().getTraceId(), "BrowseBudgets");

        var outbound0 = CalculateVarianceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateVarianceRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var reply0 = budgetingVarianceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateVariance(outbound0);

        var outbound1 = UnsubscribeJournalEntryUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(UnsubscribeJournalEntryUpdatesRequest.Details.newBuilder()
                        .setChannel(switch (reply0.getVariance().getOriginCase()) {
                            case CHANNEL -> reply0.getVariance().getChannel().getChannel();
                            default -> "";
                        })
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setName(reply0.getVariance().getRequester().getName())
                                .setPhone(reply0.getVariance().getRequester().getPhone())
                                .build())
                        .build())
                .build();
        var reply1 = ledgerJournalEntryNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).unsubscribeJournalEntryUpdates(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseBudgets", request.getPage().getPageToken());
        return BrowseBudgetsResponse.newBuilder()
                .addBudgets(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getJournalEntry().getAudit().getUpdatedBy())
                                .build())
                        .setHolder(ForecastRef.newBuilder()
                                .setDisplayName(reply0.getVariance().getCustomer().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists budgets a page at a time.
     */
    public FindBudgetsResponse findBudgets(FindBudgetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindBudgets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getContext().getTraceId(), "FindBudgets");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindBudgets", request.getPage().getPageToken());
        return FindBudgetsResponse.getDefaultInstance();
    }

    /**
     * Counts budgets by status.
     */
    public CountBudgetsResponse countBudgets(CountBudgetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountBudgets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getContext().getTraceId(), "CountBudgets");

        var outbound0 = mapper.countBudgetsStage0(request);
        var reply0 = budgetingVarianceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateVariance(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountBudgets", request.getPage().getPageToken());
        return CountBudgetsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns several budgets at once.
     */
    public ResolveBudgetsResponse resolveBudgets(ResolveBudgetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveBudgets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getContext().getTraceId(), "ResolveBudgets");

        var outbound0 = RevertAuditTrailRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RevertAuditTrailRequest.Details.newBuilder()
                        .setAuthorEmail(request.getContext().getActor().getEmail())
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply0 = auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertAuditTrail(outbound0);

        var outbound1 = CompareVarianceOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareVarianceOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(reply0.getAuditTrail().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.budgeting.v1.VarianceCalculationService");
        var selected1 = VarianceCalculationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.compareVarianceOptions(outbound1);
        return ResolveBudgetsResponse.newBuilder()
                .addBudgets(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getAuditTrail().getAudit().getCreatedAt())
                                .build())
                        .setCurrencyCode(reply1.getBreakdown().getCurrencyCode())
                        .setHolder(ForecastRef.newBuilder()
                                .setDisplayName(reply1.getVariance().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Streams every matching budget.
     */
    public ExportBudgetsResponse exportBudgets(ExportBudgetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportBudgets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getContext().getTraceId(), "ExportBudgets");

        var outbound0 = SimulateLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulateLoanApplicationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> lendingLoanApplicationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateLoanApplication(value)).orElseThrow();

        var outbound1 = SendFiscalPeriodTestNoticeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SendFiscalPeriodTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getLoanApplication().getCounterparty().getEmail())
                                .build())
                        .putAllVariables(reply0.getResult().getDetailsMap())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(ledgerFiscalPeriodNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sendFiscalPeriodTestNotice(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound2 = (QuoteBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getLoanApplication().getBillingAddress().getCountryCode())
                                .setRecipient(reply0.getLoanApplication().getBillingAddress().getRecipient())
                                .build())
                        .setCard(BudgetLineCardSettlement.newBuilder()
                                .setHolderName(reply0.getLoanApplication().getCard().getHolderName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply2 = budgetingBudgetLineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteBudgetLine(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportBudgets", request.getFilter().getQuery());
        return ExportBudgetsResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getLoanApplication().getAudit().getCreatedAt())
                                .build())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Summarizes a budget.
     */
    public SummarizeBudgetsResponse summarizeBudgets(SummarizeBudgetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeBudgets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getBudgetId(), "SummarizeBudgets");

        var outbound0 = RpcSupport.unpack(Any.pack(SubscribeFiscalPeriodUpdatesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SubscribeFiscalPeriodUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build()), SubscribeFiscalPeriodUpdatesRequest.class);
        var reply0 = ledgerFiscalPeriodNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeFiscalPeriodUpdates(outbound0);

        var outbound1 = PreviewClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getFiscalPeriod().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage1Adapter {
            PreviewClaimResponse fetch(PreviewClaimRequest input) {
                return insuranceClaimValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewClaim(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = BeanCopies.copy(ExplainForecastDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getFiscalPeriod().getAudit().getCreatedAt())
                                .setCreatedBy(reply1.getClaim().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build(), ExplainForecastDecisionRequest.class);
        var reply2 = budgetingForecastValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainForecastDecision(outbound2);

        var outbound3 = GetBudgetRulesRequest.newBuilder()
                .setBudgetId(request.getBudgetId())
                .setContext(request.getContext())
                .build();
        var channel3 = channels.lookup("acme.budgeting.v1.BudgetValidationService");
        var serviceName3 = "acme.budgeting.v1" + "." + "BudgetValidationService";
        var methodName3 = "Get" + "BudgetRules";
        var descriptor3 = MethodDescriptor.<GetBudgetRulesRequest, GetBudgetRulesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName3, methodName3))
                .setRequestMarshaller(ProtoUtils.marshaller(GetBudgetRulesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(GetBudgetRulesResponse.getDefaultInstance()))
                .build();
        var reply3 = ClientCalls.blockingUnaryCall(channel3, descriptor3,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getFormat()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeBudgets", request.getDetails().getFormat());
        return SummarizeBudgetsResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply3.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists budgets a page at a time.
     */
    public SuggestBudgetsResponse suggestBudgets(SuggestBudgetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestBudgets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getContext().getTraceId(), "SuggestBudgets");

        var outbound0 = CheckBudgetLineEligibilityRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CheckBudgetLineEligibilityRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        Function<CheckBudgetLineEligibilityRequest, CheckBudgetLineEligibilityResponse> operation0 = budgetingBudgetLineValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::checkBudgetLineEligibility;
        var reply0 = operation0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestBudgets", request.getPage().getPageToken());
        return SuggestBudgetsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a budget.
     */
    public PeekBudgetResponse peekBudget(PeekBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getBudgetId(), "PeekBudget");

        var outbound0 = EstimateControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<EstimateControlResponse>();
        events.publishEvent(new PeekBudgetStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        return PeekBudgetResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getControl().getOwner().getEmail())
                                .build())
                        .addLimits(BudgetLimit.newBuilder()
                                .setAmount(reply0.getControl().getDiscount().getAmount())
                                .build())
                        .build())
                .build();
    }
}
