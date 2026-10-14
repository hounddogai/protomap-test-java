package com.acme.finance.domains.budgeting;

import com.acme.audit.v1.PullRetentionPoliciesRequest;
import com.acme.audit.v1.PullRetentionPoliciesResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.budgeting.v1.BatchCreateBudgetsRequest;
import com.acme.budgeting.v1.BatchCreateBudgetsResponse;
import com.acme.budgeting.v1.BatchGetBudgetsRequest;
import com.acme.budgeting.v1.BatchGetBudgetsResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLineCardSettlement;
import com.acme.budgeting.v1.BudgetLineQueryServiceGrpc;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.BudgetQueryServiceGrpc;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.BudgetValidationServiceGrpc;
import com.acme.budgeting.v1.CheckForecastEligibilityRequest;
import com.acme.budgeting.v1.CheckForecastEligibilityResponse;
import com.acme.budgeting.v1.CountBudgetsRequest;
import com.acme.budgeting.v1.CountBudgetsResponse;
import com.acme.budgeting.v1.CreateBudgetRequest;
import com.acme.budgeting.v1.CreateBudgetResponse;
import com.acme.budgeting.v1.DeleteBudgetRequest;
import com.acme.budgeting.v1.DeleteBudgetResponse;
import com.acme.budgeting.v1.ExplainBudgetCalculationRequest;
import com.acme.budgeting.v1.ExplainBudgetCalculationResponse;
import com.acme.budgeting.v1.ExplainVarianceDecisionRequest;
import com.acme.budgeting.v1.ExplainVarianceDecisionResponse;
import com.acme.budgeting.v1.ExportBudgetLinesRequest;
import com.acme.budgeting.v1.ExportBudgetLinesResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastCardSettlement;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.ForecastValidationServiceGrpc;
import com.acme.budgeting.v1.GetBudgetRequest;
import com.acme.budgeting.v1.GetBudgetResponse;
import com.acme.budgeting.v1.LintBudgetsRequest;
import com.acme.budgeting.v1.LintBudgetsResponse;
import com.acme.budgeting.v1.ListBudgetsRequest;
import com.acme.budgeting.v1.ListBudgetsResponse;
import com.acme.budgeting.v1.NormalizeForecastRequest;
import com.acme.budgeting.v1.NormalizeForecastResponse;
import com.acme.budgeting.v1.PreviewBudgetLineRequest;
import com.acme.budgeting.v1.PreviewBudgetLineResponse;
import com.acme.budgeting.v1.PreviewForecastRequest;
import com.acme.budgeting.v1.PreviewForecastResponse;
import com.acme.budgeting.v1.ResolveBudgetsRequest;
import com.acme.budgeting.v1.ResolveBudgetsResponse;
import com.acme.budgeting.v1.ScoreForecastRequest;
import com.acme.budgeting.v1.ScoreForecastResponse;
import com.acme.budgeting.v1.SimulateBudgetLineRequest;
import com.acme.budgeting.v1.SimulateBudgetLineResponse;
import com.acme.budgeting.v1.SimulateBudgetRequest;
import com.acme.budgeting.v1.SimulateBudgetResponse;
import com.acme.budgeting.v1.UpdateBudgetRequest;
import com.acme.budgeting.v1.UpdateBudgetResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceValidationServiceGrpc;
import com.acme.budgeting.v1.WatchBudgetsRequest;
import com.acme.budgeting.v1.WatchBudgetsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.corporateevents.v1.CloneCorporateEventRequest;
import com.acme.corporateevents.v1.CloneCorporateEventResponse;
import com.acme.corporateevents.v1.CorporateEvent;
import com.acme.corporateevents.v1.CorporateEventParticipant;
import com.acme.corporateevents.v1.CorporateEventServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.ExplainUserDecisionRequest;
import com.acme.identity.v1.ExplainUserDecisionResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionRequest;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.metrics.v1.ListMetricSeriesesRequest;
import com.acme.metrics.v1.ListMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.risk.v1.CountRiskScenariosRequest;
import com.acme.risk.v1.CountRiskScenariosResponse;
import com.acme.risk.v1.NormalizeStressTestRequest;
import com.acme.risk.v1.NormalizeStressTestResponse;
import com.acme.risk.v1.RiskScenarioQueryServiceGrpc;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
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
 * Manages budgets: creates, reads, changes, and lists them. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class BudgetApplication {
    private static final Logger log = LoggerFactory.getLogger(BudgetApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking;

    private final BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking;

    private final BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking;

    private final BudgetLineQueryServiceGrpc.BudgetLineQueryServiceBlockingStub budgetingBudgetLineQueryServiceBlocking;

    private final BudgetLineValidationServiceGrpc.BudgetLineValidationServiceBlockingStub budgetingBudgetLineValidationServiceBlocking;

    private final BudgetQueryServiceGrpc.BudgetQueryServiceBlockingStub budgetingBudgetQueryServiceBlocking;

    private final BudgetValidationServiceGrpc.BudgetValidationServiceBlockingStub budgetingBudgetValidationServiceBlocking;

    private final ForecastValidationServiceGrpc.ForecastValidationServiceBlockingStub budgetingForecastValidationServiceBlocking;

    private final ForecastValidationServiceGrpc.ForecastValidationServiceFutureStub budgetingForecastValidationServiceFuture;

    private final VarianceValidationServiceGrpc.VarianceValidationServiceBlockingStub budgetingVarianceValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final CorporateEventServiceGrpc.CorporateEventServiceBlockingStub corporateeventsCorporateEventServiceBlocking;

    private final UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking;

    private final BudgetRequestMapper mapper;

    private final MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking;

    private final BudgetingRepository repository;

    private final RiskScenarioQueryServiceGrpc.RiskScenarioQueryServiceBlockingStub riskRiskScenarioQueryServiceBlocking;

    private final StressTestValidationServiceGrpc.StressTestValidationServiceBlockingStub riskStressTestValidationServiceBlocking;

    public BudgetApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking,
            BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking,
            BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking,
            BudgetLineQueryServiceGrpc.BudgetLineQueryServiceBlockingStub budgetingBudgetLineQueryServiceBlocking,
            BudgetLineValidationServiceGrpc.BudgetLineValidationServiceBlockingStub budgetingBudgetLineValidationServiceBlocking,
            BudgetQueryServiceGrpc.BudgetQueryServiceBlockingStub budgetingBudgetQueryServiceBlocking,
            BudgetValidationServiceGrpc.BudgetValidationServiceBlockingStub budgetingBudgetValidationServiceBlocking,
            ForecastValidationServiceGrpc.ForecastValidationServiceBlockingStub budgetingForecastValidationServiceBlocking,
            ForecastValidationServiceGrpc.ForecastValidationServiceFutureStub budgetingForecastValidationServiceFuture,
            VarianceValidationServiceGrpc.VarianceValidationServiceBlockingStub budgetingVarianceValidationServiceBlocking,
            ChannelRegistry channels,
            CorporateEventServiceGrpc.CorporateEventServiceBlockingStub corporateeventsCorporateEventServiceBlocking,
            UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking,
            BudgetRequestMapper mapper,
            MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking,
            BudgetingRepository repository,
            RiskScenarioQueryServiceGrpc.RiskScenarioQueryServiceBlockingStub riskRiskScenarioQueryServiceBlocking,
            StressTestValidationServiceGrpc.StressTestValidationServiceBlockingStub riskStressTestValidationServiceBlocking) {
        this.auditRetentionPolicySyncServiceBlocking = auditRetentionPolicySyncServiceBlocking;
        this.budgetingBudgetCalculationServiceBlocking = budgetingBudgetCalculationServiceBlocking;
        this.budgetingBudgetLineCalculationServiceBlocking = budgetingBudgetLineCalculationServiceBlocking;
        this.budgetingBudgetLineQueryServiceBlocking = budgetingBudgetLineQueryServiceBlocking;
        this.budgetingBudgetLineValidationServiceBlocking = budgetingBudgetLineValidationServiceBlocking;
        this.budgetingBudgetQueryServiceBlocking = budgetingBudgetQueryServiceBlocking;
        this.budgetingBudgetValidationServiceBlocking = budgetingBudgetValidationServiceBlocking;
        this.budgetingForecastValidationServiceBlocking = budgetingForecastValidationServiceBlocking;
        this.budgetingForecastValidationServiceFuture = budgetingForecastValidationServiceFuture;
        this.budgetingVarianceValidationServiceBlocking = budgetingVarianceValidationServiceBlocking;
        this.channels = channels;
        this.corporateeventsCorporateEventServiceBlocking = corporateeventsCorporateEventServiceBlocking;
        this.identityUserValidationServiceBlocking = identityUserValidationServiceBlocking;
        this.ledgerLedgerAccountHistoryServiceBlocking = ledgerLedgerAccountHistoryServiceBlocking;
        this.mapper = mapper;
        this.metricsMetricSeriesServiceBlocking = metricsMetricSeriesServiceBlocking;
        this.repository = repository;
        this.riskRiskScenarioQueryServiceBlocking = riskRiskScenarioQueryServiceBlocking;
        this.riskStressTestValidationServiceBlocking = riskStressTestValidationServiceBlocking;
    }

    /**
     * Creates a budget.
     */
    public CreateBudgetResponse createBudget(CreateBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CreateBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var builder0 = PullRetentionPoliciesRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(PullRetentionPoliciesRequest.Details.newBuilder()
                .setPayload(JsonCodec.encode(request.getContext()))
                .build());
        builder0.setRetentionPolicy(RetentionPolicy.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedAt(request.getBudget().getAudit().getCreatedAt())
                        .build())
                .setOwner(UserRef.newBuilder()
                        .setEmail(request.getBudget().getContact().getEmail())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = auditRetentionPolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullRetentionPolicies(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(SimulateBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setHolder(ForecastRef.newBuilder()
                                .setDisplayName(reply0.getRetentionPolicy().getOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()), SimulateBudgetRequest.class);
        var reply1 = budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateBudget(outbound1);

        var outbound2 = ResolveBudgetsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var channel2 = channels.lookup("acme.budgeting.v1.BudgetQueryService");
        var serviceName2 = "acme.budgeting.v1" + "." + "BudgetQueryService";
        var methodName2 = "Res" + "olveBudgets";
        var descriptor2 = MethodDescriptor.<ResolveBudgetsRequest, ResolveBudgetsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(ResolveBudgetsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ResolveBudgetsResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = NormalizeForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setForecast(Forecast.newBuilder()
                        .setCard(ForecastCardSettlement.newBuilder()
                                .setNetwork(request.getBudget().getCard().getNetwork())
                                .build())
                        .setCounterparty(BudgetRef.newBuilder()
                                .setDisplayName(request.getBudget().getHolder().getDisplayName())
                                .build())
                        .build())
                .build();
        Function<NormalizeForecastRequest, NormalizeForecastResponse> operation3 = budgetingForecastValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::normalizeForecast;
        var reply3 = operation3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CreateBudget", request.getBudget().getBudgetId());
        return CreateBudgetResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getRetentionPolicy().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply1.getBudget().getAudit().getUpdatedBy())
                                .build())
                        .setContact(request.getBudget().getContact())
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a budget.
     */
    public GetBudgetResponse getBudget(GetBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getBudgetId(), "GetBudget");

        var outbound0 = ExplainVarianceDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainVarianceDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> budgetingVarianceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainVarianceDecision(value)).orElseThrow();
        return GetBudgetResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getVariance().getAudit().getCreatedAt())
                                .build())
                        .setBudgetId(request.getBudgetId())
                        .setHolder(ForecastRef.newBuilder()
                                .setDisplayName(reply0.getVariance().getCustomer().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Changes a budget.
     */
    public UpdateBudgetResponse updateBudget(UpdateBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UpdateBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = CheckForecastEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckForecastEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getBudget().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<CheckForecastEligibilityRequest, CheckForecastEligibilityResponse>() {
            @Override
            public CheckForecastEligibilityResponse apply(CheckForecastEligibilityRequest input) {
                return budgetingForecastValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkForecastEligibility(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = ScoreForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreForecastRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(reply0.getForecast().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setCard(ForecastCardSettlement.newBuilder()
                                .setLast4(request.getBudget().getCard().getLast4())
                                .build())
                        .build())
                .build();
        RpcPort<ScoreForecastRequest, ScoreForecastResponse> port1 = RpcPort.adapt(budgetingForecastValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scoreForecast);
        var reply1 = port1.exchange(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UpdateBudget", request.getBudget().getBudgetId());
        return UpdateBudgetResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getForecast().getAudit().getCreatedBy())
                                .build())
                        .setAvailable(request.getBudget().getAvailable())
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Deletes a budget.
     */
    public DeleteBudgetResponse deleteBudget(DeleteBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getBudgetId(), "DeleteBudget");

        var outbound0 = ExplainUserDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getReason())
                .setUser(User.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getEtag())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setUserId(request.getContext().getActor().getUserId())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(identityUserValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainUserDecision(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = CloneCorporateEventRequest.newBuilder()
                .setContext(request.getContext())
                .setCorporateEvent(CorporateEvent.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(reply0.getUser().getPrimaryAddress().getLinesList())
                                .setRecipient(reply0.getUser().getPrimaryAddress().getRecipient())
                                .build())
                        .addParticipants(CorporateEventParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPhone(reply0.getUser().getContact().getPhone())
                                        .build())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.corporateevents.v1.CorporateEventService/CloneCorporateEvent", outbound1);
        var reply1 = corporateeventsCorporateEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneCorporateEvent(command1.payload());

        var outbound2 = AnnotateLedgerAccountVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AnnotateLedgerAccountVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getUser().getAudit().getUpdatedBy())
                                .build())
                        .setHolder(PostingBatchRef.newBuilder()
                                .setNumber(reply0.getUser().getNationalId().getNumber())
                                .build())
                        .build())
                .build();
        var reply2 = Optional.of(outbound2).map(value -> value.toBuilder().build())
                .map(value -> ledgerLedgerAccountHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotateLedgerAccountVersion(value)).orElseThrow();

        var outbound3 = RpcSupport.unpack(Any.pack(ExplainBudgetCalculationRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getLedgerAccount().getAudit().getEtag())
                                .setUpdatedAt(reply2.getLedgerAccount().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()), ExplainBudgetCalculationRequest.class);
        var reply3 = budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainBudgetCalculation(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteBudget", request.getBudgetId());
        return DeleteBudgetResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply2.getOutcome().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists budgets a page at a time.
     */
    public ListBudgetsResponse listBudgets(ListBudgetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListBudgets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getContext().getTraceId(), "ListBudgets");

        var outbound0 = ListMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListMetricSeriesesRequest.Filter.newBuilder()
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .setPage(PageRequest.newBuilder()
                        .setPageToken(request.getPage().getPageToken())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> metricsMetricSeriesServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listMetricSerieses(value));

        var outbound1 = ExportBudgetLinesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportBudgetLinesRequest.Filter.newBuilder()
                        .setOwnerId(request.getFilter().getOwnerId())
                        .build())
                .build();
        var stream1 = budgetingBudgetLineQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportBudgetLines(outbound1);
        var reply1 = ExportBudgetLinesResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getFilter().getRegion()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListBudgets", request.getFilter().getRegion());
        return ListBudgetsResponse.newBuilder()
                .setPageInfo(PageInfo.newBuilder()
                        .setNextPageToken(reply0.getPageInfo().getNextPageToken())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns several budgets at once.
     */
    public BatchGetBudgetsResponse batchGetBudgets(BatchGetBudgetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchGetBudgets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getContext().getTraceId(), "BatchGetBudgets");

        var outbound0 = CountBudgetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountBudgetsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getContext().getTenantId())
                        .build())
                .build();
        var reply0 = budgetingBudgetQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countBudgets(outbound0);

        var outbound1 = PreviewBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewBudgetLineRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> budgetingBudgetLineValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewBudgetLine(value)).orElseThrow();
        return BatchGetBudgetsResponse.newBuilder()
                .addBudgets(Budget.newBuilder()
                        .setHolder(ForecastRef.newBuilder()
                                .setNumber(reply1.getBudgetLine().getNumber())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Streams the changes of budgets as they happen.
     */
    public WatchBudgetsResponse watchBudgets(WatchBudgetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "WatchBudgets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getContext().getTraceId(), "WatchBudgets");

        var outbound0 = PreviewForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewForecastRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .setName(request.getSubscriber().getName())
                                .setPreferredChannel(request.getSubscriber().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending0 = budgetingForecastValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).previewForecast(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = CountRiskScenariosRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountRiskScenariosRequest.Filter.newBuilder()
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(riskRiskScenarioQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countRiskScenarios(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound2 = SimulateBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setCard(BudgetLineCardSettlement.newBuilder()
                                .setLast4(switch (reply0.getForecast().getSettlementCase()) {
                                    case CARD -> reply0.getForecast().getCard().getLast4();
                                    default -> "";
                                })
                                .setToken(switch (reply0.getForecast().getSettlementCase()) {
                                    case CARD -> reply0.getForecast().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(SimulateBudgetLineRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getForecast().getBillingAddress().getCountryCode())
                                .addAllLines(reply0.getForecast().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var reply2 = budgetingBudgetLineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateBudgetLine(outbound2);

        var outbound3 = LintBudgetsRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getBudgetLine().getAudit().getCreatedAt())
                                .setUpdatedBy(reply2.getBudgetLine().getAudit().getUpdatedBy())
                                .build())
                        .setCard(BudgetCardFunding.newBuilder()
                                .setNetwork(reply2.getBudgetLine().getCard().getNetwork())
                                .build())
                        .setHolder(ForecastRef.newBuilder()
                                .setDisplayName(reply0.getForecast().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var adapter3 = new Function<LintBudgetsRequest, LintBudgetsResponse>() {
            @Override
            public LintBudgetsResponse apply(LintBudgetsRequest input) {
                return budgetingBudgetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintBudgets(input);
            }
        };
        var reply3 = adapter3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getOwnerId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "WatchBudgets", request.getFilter().getOwnerId());
        return WatchBudgetsResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getForecast().getAudit().getUpdatedAt())
                                .build())
                        .setBudgetId(reply3.getBudget().getBudgetId())
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getSubscriber().getName())
                                .build())
                        .setHolder(ForecastRef.newBuilder()
                                .setNumber(reply2.getBudgetLine().getNumber())
                                .build())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Takes a stream of budgets and answers once at the end.
     */
    public BatchCreateBudgetsResponse batchCreateBudgets(BatchCreateBudgetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchCreateBudgets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.saveBudget(request.getContext().getTenantId(), request.getRecord());
        }

        var outbound0 = mapper.batchCreateBudgetsStage0(request);
        var reply0 = budgetingVarianceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainVarianceDecision(outbound0);

        var outbound1 = mapper.batchCreateBudgetsStage1(request, reply0);
        var reply1 = riskStressTestValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeStressTest(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BatchCreateBudgets", request.getHeader().getSource());
        return BatchCreateBudgetsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }
}
