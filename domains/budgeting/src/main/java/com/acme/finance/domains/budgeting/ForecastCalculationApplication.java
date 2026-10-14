package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalValidationServiceGrpc;
import com.acme.assets.v1.LintDisposalsRequest;
import com.acme.assets.v1.LintDisposalsResponse;
import com.acme.assets.v1.PreviewDisposalRequest;
import com.acme.assets.v1.PreviewDisposalResponse;
import com.acme.audit.v1.PullRetentionPoliciesRequest;
import com.acme.audit.v1.PullRetentionPoliciesResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyDiscountAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.banking.v1.ExplainMandateCalculationRequest;
import com.acme.banking.v1.ExplainMandateCalculationResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateAttachment;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetBankFunding;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.BudgetLimit;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.BudgetValidationServiceGrpc;
import com.acme.budgeting.v1.CalculateForecastRequest;
import com.acme.budgeting.v1.CalculateForecastResponse;
import com.acme.budgeting.v1.CompareForecastOptionsRequest;
import com.acme.budgeting.v1.CompareForecastOptionsResponse;
import com.acme.budgeting.v1.EstimateBudgetRequest;
import com.acme.budgeting.v1.EstimateBudgetResponse;
import com.acme.budgeting.v1.EstimateForecastRequest;
import com.acme.budgeting.v1.EstimateForecastResponse;
import com.acme.budgeting.v1.ExplainForecastCalculationRequest;
import com.acme.budgeting.v1.ExplainForecastCalculationResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastLine;
import com.acme.budgeting.v1.ForecastLineTax;
import com.acme.budgeting.v1.GetForecastRatesRequest;
import com.acme.budgeting.v1.GetForecastRatesResponse;
import com.acme.budgeting.v1.LintBudgetsRequest;
import com.acme.budgeting.v1.LintBudgetsResponse;
import com.acme.budgeting.v1.QuoteForecastRequest;
import com.acme.budgeting.v1.QuoteForecastResponse;
import com.acme.budgeting.v1.RecalculateForecastsRequest;
import com.acme.budgeting.v1.RecalculateForecastsResponse;
import com.acme.budgeting.v1.ScoreBudgetRequest;
import com.acme.budgeting.v1.ScoreBudgetResponse;
import com.acme.budgeting.v1.SimulateForecastRequest;
import com.acme.budgeting.v1.SimulateForecastResponse;
import com.acme.budgeting.v1.ValidateBudgetRequest;
import com.acme.budgeting.v1.ValidateBudgetResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.featureflags.v1.CheckFeatureFlagEligibilityRequest;
import com.acme.featureflags.v1.CheckFeatureFlagEligibilityResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.ListUsersRequest;
import com.acme.identity.v1.ListUsersResponse;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.EstimateClaimRequest;
import com.acme.insurance.v1.EstimateClaimResponse;
import com.acme.lending.v1.CountLoanApplicationsRequest;
import com.acme.lending.v1.CountLoanApplicationsResponse;
import com.acme.lending.v1.ExportLoanApplicationsRequest;
import com.acme.lending.v1.ExportLoanApplicationsResponse;
import com.acme.lending.v1.LoanApplicationQueryServiceGrpc;
import com.acme.lending.v1.SuggestLoanApplicationsRequest;
import com.acme.lending.v1.SuggestLoanApplicationsResponse;
import com.acme.seating.v1.CountSeatMapsRequest;
import com.acme.seating.v1.CountSeatMapsResponse;
import com.acme.ticketing.v1.ExportTicketsRequest;
import com.acme.ticketing.v1.ExportTicketsResponse;
import com.acme.ticketing.v1.TicketQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a forecast, such as totals, taxes, and estimates. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ForecastCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(ForecastCalculationApplication.class);

    private final DisposalValidationServiceGrpc.DisposalValidationServiceBlockingStub assetsDisposalValidationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking;

    private final MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking;

    private final BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking;

    private final BudgetValidationServiceGrpc.BudgetValidationServiceBlockingStub budgetingBudgetValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking;

    private final LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceBlockingStub lendingLoanApplicationQueryServiceBlocking;

    private final BudgetingRepository repository;

    private final TicketQueryServiceGrpc.TicketQueryServiceBlockingStub ticketingTicketQueryServiceBlocking;

    public ForecastCalculationApplication(
            DisposalValidationServiceGrpc.DisposalValidationServiceBlockingStub assetsDisposalValidationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking,
            MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking,
            BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking,
            BudgetValidationServiceGrpc.BudgetValidationServiceBlockingStub budgetingBudgetValidationServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking,
            LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceBlockingStub lendingLoanApplicationQueryServiceBlocking,
            BudgetingRepository repository,
            TicketQueryServiceGrpc.TicketQueryServiceBlockingStub ticketingTicketQueryServiceBlocking) {
        this.assetsDisposalValidationServiceBlocking = assetsDisposalValidationServiceBlocking;
        this.auditRetentionPolicySyncServiceBlocking = auditRetentionPolicySyncServiceBlocking;
        this.bankingMandateCalculationServiceBlocking = bankingMandateCalculationServiceBlocking;
        this.budgetingBudgetCalculationServiceBlocking = budgetingBudgetCalculationServiceBlocking;
        this.budgetingBudgetValidationServiceBlocking = budgetingBudgetValidationServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.insuranceClaimCalculationServiceBlocking = insuranceClaimCalculationServiceBlocking;
        this.lendingLoanApplicationQueryServiceBlocking = lendingLoanApplicationQueryServiceBlocking;
        this.repository = repository;
        this.ticketingTicketQueryServiceBlocking = ticketingTicketQueryServiceBlocking;
    }

    public record CompareForecastOptionsStage0Requested(CheckFeatureFlagEligibilityRequest request, CompletableFuture<CheckFeatureFlagEligibilityResponse> completion, io.grpc.Context context) {}

    public record CompareForecastOptionsStage1Requested(CountLoanApplicationsRequest request, CompletableFuture<CountLoanApplicationsResponse> completion, io.grpc.Context context) {}

    public record RecalculateForecastsStage2Requested(CountLoanApplicationsRequest request, CompletableFuture<CountLoanApplicationsResponse> completion, io.grpc.Context context) {}

    /**
     * Calculates a forecast.
     */
    public CalculateForecastResponse calculateForecast(CalculateForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = ListUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListUsersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getForecast().getAudit().getCreatedAt())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var reply0 = RpcSupport.retryRead(() -> identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listUsers(outbound0));

        var outbound1 = (ScoreBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getForecast().getAudit().getCreatedBy())
                                .setEtag(request.getForecast().getAudit().getEtag())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ScoreBudgetRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getForecast().getBillingAddress().getCountryCode())
                                .addAllLines(request.getForecast().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = budgetingBudgetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreBudget(outbound1);

        var outbound2 = SuggestLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestLoanApplicationsRequest.Filter.newBuilder()
                        .setRegion(request.getForecast().getBillingAddress().getRegion())
                        .build())
                .build();
        RpcPort<SuggestLoanApplicationsRequest, SuggestLoanApplicationsResponse> port2 = RpcPort.adapt(lendingLoanApplicationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::suggestLoanApplications);
        var reply2 = port2.exchange(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateForecast", request.getForecast().getForecastId());
        return CalculateForecastResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setDueAt(request.getForecast().getDueAt())
                        .setForecastId(request.getForecastId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Estimates a forecast.
     */
    public EstimateForecastResponse estimateForecast(EstimateForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = LintDisposalsRequest.newBuilder()
                .setContext(request.getContext())
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getForecast().getCounterparty().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getForecast().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        RpcPort<LintDisposalsRequest, LintDisposalsResponse> port0 = RpcPort.adapt(assetsDisposalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::lintDisposals);
        var reply0 = port0.exchange(outbound0);

        var results1 = new ArrayList<PullRetentionPoliciesResponse>();
        for (var element : request.getForecast().getLinesList()) {
            PullRetentionPoliciesRequest itemRequest = PullRetentionPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PullRetentionPoliciesRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setDiscount(RetentionPolicyDiscountAction.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .build())
                .build();
            results1.add(auditRetentionPolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullRetentionPolicies(itemRequest));
        }
        var reply1 = results1.isEmpty() ? PullRetentionPoliciesResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateForecast", request.getForecast().getForecastId());
        return EstimateForecastResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setBillingAddress(request.getForecast().getBillingAddress())
                        .setCounterparty(BudgetRef.newBuilder()
                                .setDisplayName(reply0.getDisposal().getCustomer().getDisplayName())
                                .build())
                        .setForecastId(request.getForecastId())
                        .build())
                .build();
    }

    /**
     * Quotes a forecast.
     */
    public QuoteForecastResponse quoteForecast(QuoteForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var results0 = new ArrayList<LintBudgetsResponse>();
        for (var element : request.getForecast().getLinesList()) {
            LintBudgetsRequest itemRequest = LintBudgetsRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getForecast().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getForecast().getAudit().getUpdatedBy())
                                .build())
                        .addLimits(BudgetLimit.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
            results0.add(budgetingBudgetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintBudgets(itemRequest));
        }
        var reply0 = results0.isEmpty() ? LintBudgetsResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteForecast", request.getForecast().getForecastId());
        return QuoteForecastResponse.newBuilder()
                .setBreakdown(QuoteForecastResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setForecastId(request.getForecastId())
                        .build())
                .build();
    }

    /**
     * Compares a forecast.
     */
    public CompareForecastOptionsResponse compareForecastOptions(CompareForecastOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareForecastOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = CheckFeatureFlagEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getForecast().getAudit().getCreatedBy())
                                .setEtag(request.getForecast().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<CheckFeatureFlagEligibilityResponse>();
        events.publishEvent(new CompareForecastOptionsStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = CountLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountLoanApplicationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getForecast().getIssuedAt())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var completion1 = new CompletableFuture<CountLoanApplicationsResponse>();
        events.publishEvent(new CompareForecastOptionsStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = LintBudgetsRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getForecast().getAudit().getCreatedAt())
                                .setCreatedBy(request.getForecast().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getForecast().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> budgetingBudgetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintBudgets(value));
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareForecastOptions", request.getForecast().getForecastId());
        return CompareForecastOptionsResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setForecastId(request.getForecastId())
                        .addLines(ForecastLine.newBuilder()
                                .setDescription(reply0.getFeatureFlag().getDescription())
                                .setTax(ForecastLineTax.newBuilder()
                                        .setAmount(reply0.getFeatureFlag().getDiscount().getAmount())
                                        .build())
                                .build())
                        .setNumber(request.getForecast().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply2.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Recalculates a forecast.
     */
    public RecalculateForecastsResponse recalculateForecasts(RecalculateForecastsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateForecasts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = ExportLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportLoanApplicationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getForecast().getAudit().getCreatedAt())
                        .setRegion(request.getForecast().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream0 = lendingLoanApplicationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportLoanApplications(outbound0);
        var reply0 = ExportLoanApplicationsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var results1 = new ArrayList<ExportTicketsResponse>();
        for (var element : request.getForecast().getLinesList()) {
            ExportTicketsRequest itemRequest = ExportTicketsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportTicketsRequest.Filter.newBuilder()
                        .putAllLabels(element.getAttributesMap())
                        .setOwnerId(element.getLineId())
                        .setRegion(request.getForecast().getBillingAddress().getRegion())
                        .build())
                .build();
            var responses = ticketingTicketQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportTickets(itemRequest);
            responses.forEachRemaining(results1::add);
        }
        var reply1 = results1.isEmpty() ? ExportTicketsResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = CountLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountLoanApplicationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getForecast().getAudit().getCreatedAt())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var completion2 = new CompletableFuture<CountLoanApplicationsResponse>();
        events.publishEvent(new RecalculateForecastsStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound3 = JsonCodec.roundTrip(EstimateClaimRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateClaimRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getForecast().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build(), EstimateClaimRequest.class);
        var reply3 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateClaim(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateForecasts", request.getForecast().getForecastId());
        return RecalculateForecastsResponse.newBuilder()
                .setBreakdown(RecalculateForecastsResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply3.getBreakdown().getCurrencyCode())
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setCounterparty(BudgetRef.newBuilder()
                                .setDisplayName(reply3.getClaim().getAssignee().getDisplayName())
                                .build())
                        .setForecastId(request.getForecastId())
                        .setNotes(request.getForecast().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Simulates a forecast.
     */
    public SimulateForecastResponse simulateForecast(SimulateForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        List<ExplainMandateCalculationResponse> results0 = request.getForecast().getAttachmentsList().stream()
                .map(element -> ExplainMandateCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .addAttachments(MandateAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setContentType(element.getContentType())
                                .setSizeBytes(element.getSizeBytes())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainMandateCalculation(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? ExplainMandateCalculationResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = CountSeatMapsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountSeatMapsRequest.Filter.newBuilder()
                        .setRegion(request.getForecast().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.seating.v1.SeatMapQueryService");
        var serviceName1 = "acme.seating.v1" + "." + "SeatMapQueryService";
        var methodName1 = "Cou" + "ntSeatMaps";
        var descriptor1 = MethodDescriptor.<CountSeatMapsRequest, CountSeatMapsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(CountSeatMapsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CountSeatMapsResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var outbound2 = ValidateBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getMandate().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getMandate().getAudit().getUpdatedBy())
                                .build())
                        .setBank(BudgetBankFunding.newBuilder()
                                .setIban(switch (reply0.getMandate().getSettlementCase()) {
                                    case BANK -> reply0.getMandate().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getMandate().getCounterparty().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = budgetingBudgetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateBudget(outbound2);

        var outbound3 = ScoreBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getForecast().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getMandate().getAudit().getUpdatedAt())
                                .build())
                        .setBank(BudgetBankFunding.newBuilder()
                                .setAccountHolder(switch (request.getForecast().getSettlementCase()) {
                                    case BANK -> request.getForecast().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply3 = budgetingBudgetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreBudget(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateForecast", request.getForecast().getForecastId());
        return SimulateForecastResponse.newBuilder()
                .setBreakdown(SimulateForecastResponse.Breakdown.newBuilder()
                        .setTotal(request.getForecast().getTotal())
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getBudget().getAudit().getCreatedAt())
                                .build())
                        .setCounterparty(BudgetRef.newBuilder()
                                .setDisplayName(reply2.getBudget().getHolder().getDisplayName())
                                .build())
                        .setForecastId(request.getForecastId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply3.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Explains a forecast.
     */
    public ExplainForecastCalculationResponse explainForecastCalculation(ExplainForecastCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainForecastCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = RpcSupport.unpack(Any.pack(ValidateBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getForecast().getAudit().getUpdatedAt())
                                .build())
                        .setCard(BudgetCardFunding.newBuilder()
                                .setLast4(request.getForecast().getCard().getLast4())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()), ValidateBudgetRequest.class);
        var reply0 = budgetingBudgetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateBudget(outbound0);

        var outbound1 = EstimateBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBank(BudgetBankFunding.newBuilder()
                                .setAccountHolder(switch (request.getForecast().getSettlementCase()) {
                                    case BANK -> request.getForecast().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateBudget(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainForecastCalculation", request.getForecast().getForecastId());
        return ExplainForecastCalculationResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setCounterparty(request.getForecast().getCounterparty())
                        .setForecastId(request.getForecastId())
                        .build())
                .setOutcome(ExplainForecastCalculationResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a forecast.
     */
    public GetForecastRatesResponse getForecastRates(GetForecastRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetForecastRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Forecast", request.getForecastId(), "GetForecastRates");

        var outbound0 = PreviewDisposalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewDisposalRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setExternalReference(request.getForecastId())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.assets.v1.DisposalValidationService");
        var serviceName0 = "acme.assets.v1" + "." + "DisposalValidationService";
        var methodName0 = "Pre" + "viewDisposal";
        var descriptor0 = MethodDescriptor.<PreviewDisposalRequest, PreviewDisposalResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(PreviewDisposalRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(PreviewDisposalResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        return GetForecastRatesResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setForecastId(request.getForecastId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }
}
