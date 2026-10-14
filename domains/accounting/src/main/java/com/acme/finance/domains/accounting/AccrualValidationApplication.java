package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualLine;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationBlockAction;
import com.acme.accounting.v1.AllocationCalculationServiceGrpc;
import com.acme.accounting.v1.CalculateAllocationRequest;
import com.acme.accounting.v1.CalculateAllocationResponse;
import com.acme.accounting.v1.CheckAccrualEligibilityRequest;
import com.acme.accounting.v1.CheckAccrualEligibilityResponse;
import com.acme.accounting.v1.CompareAllocationOptionsRequest;
import com.acme.accounting.v1.CompareAllocationOptionsResponse;
import com.acme.accounting.v1.EstimateAllocationRequest;
import com.acme.accounting.v1.EstimateAllocationResponse;
import com.acme.accounting.v1.ExplainAccrualDecisionRequest;
import com.acme.accounting.v1.ExplainAccrualDecisionResponse;
import com.acme.accounting.v1.GetAccrualRulesRequest;
import com.acme.accounting.v1.GetAccrualRulesResponse;
import com.acme.accounting.v1.LintAccrualsRequest;
import com.acme.accounting.v1.LintAccrualsResponse;
import com.acme.accounting.v1.NormalizeAccrualRequest;
import com.acme.accounting.v1.NormalizeAccrualResponse;
import com.acme.accounting.v1.PreviewAccrualRequest;
import com.acme.accounting.v1.PreviewAccrualResponse;
import com.acme.accounting.v1.RecalculateAllocationsRequest;
import com.acme.accounting.v1.RecalculateAllocationsResponse;
import com.acme.accounting.v1.ScoreAccrualRequest;
import com.acme.accounting.v1.ScoreAccrualResponse;
import com.acme.accounting.v1.SimulateAllocationRequest;
import com.acme.accounting.v1.SimulateAllocationResponse;
import com.acme.accounting.v1.ValidateAccrualRequest;
import com.acme.accounting.v1.ValidateAccrualResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.ScoreFixedAssetRequest;
import com.acme.assets.v1.ScoreFixedAssetResponse;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.CountAuditEventsRequest;
import com.acme.audit.v1.CountAuditEventsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.expenses.v1.BrowseExpenseReportsRequest;
import com.acme.expenses.v1.BrowseExpenseReportsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.ExplainUserDecisionRequest;
import com.acme.identity.v1.ExplainUserDecisionResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageBlockAction;
import com.acme.insurance.v1.CoverageDiscountAction;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.ExplainCoverageDecisionRequest;
import com.acme.insurance.v1.ExplainCoverageDecisionResponse;
import com.acme.insurance.v1.NormalizeCoverageRequest;
import com.acme.insurance.v1.NormalizeCoverageResponse;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsRequest;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralDimensions;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.ValidateCollateralRequest;
import com.acme.lending.v1.ValidateCollateralResponse;
import com.acme.treasury.v1.EstimateFxHedgeRequest;
import com.acme.treasury.v1.EstimateFxHedgeResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Checks accruals against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class AccrualValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(AccrualValidationApplication.class);

    private final AllocationCalculationServiceGrpc.AllocationCalculationServiceBlockingStub accountingAllocationCalculationServiceBlocking;

    private final FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking;

    private final AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking;

    private final CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking;

    private final CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking;

    private final AccountingRepository repository;

    private final FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking;

    public AccrualValidationApplication(
            AllocationCalculationServiceGrpc.AllocationCalculationServiceBlockingStub accountingAllocationCalculationServiceBlocking,
            FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking,
            AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking,
            CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking,
            CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking,
            AccountingRepository repository,
            FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking) {
        this.accountingAllocationCalculationServiceBlocking = accountingAllocationCalculationServiceBlocking;
        this.assetsFixedAssetValidationServiceBlocking = assetsFixedAssetValidationServiceBlocking;
        this.auditAuditEventQueryServiceBlocking = auditAuditEventQueryServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.identityUserValidationServiceBlocking = identityUserValidationServiceBlocking;
        this.insuranceCoverageValidationServiceBlocking = insuranceCoverageValidationServiceBlocking;
        this.lendingCollateralValidationServiceBlocking = lendingCollateralValidationServiceBlocking;
        this.repository = repository;
        this.treasuryFxHedgeCalculationServiceBlocking = treasuryFxHedgeCalculationServiceBlocking;
    }

    public record ExplainAccrualDecisionStage2Requested(ListFiscalPeriodSubscriptionsRequest request, CompletableFuture<ListFiscalPeriodSubscriptionsResponse> completion, io.grpc.Context context) {}

    /**
     * Validates an accrual.
     */
    public ValidateAccrualResponse validateAccrual(ValidateAccrualRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateAccrual")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAccrual(request.getContext().getTenantId(), request.getAccrual());

        var outbound0 = EstimateAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getAccrual().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(EstimateAllocationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getAccrual().getBillingAddress().getLinesList())
                                .setLocality(request.getAccrual().getBillingAddress().getLocality())
                                .setPostalCode(request.getAccrual().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.accounting.v1.AllocationCalculationService/EstimateAllocation", outbound0);
        var reply0 = accountingAllocationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateAllocation(command0.payload());

        var outbound1 = CalculateAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getAccrual().getAudit().getUpdatedBy())
                                .build())
                        .setBlock(AllocationBlockAction.newBuilder()
                                .setNotifyEmail(reply0.getAllocation().getBlock().getNotifyEmail())
                                .setReason(reply0.getAllocation().getBlock().getReason())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CalculateAllocationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getAccrual().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        var reply1 = accountingAllocationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateAllocation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAccrual().getAccrualId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateAccrual", request.getAccrual().getAccrualId());
        return ValidateAccrualResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getAllocation().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(request.getAccrual().getCounterparty())
                        .addLines(AccrualLine.newBuilder()
                                .setDescription(reply0.getAllocation().getDescription())
                                .build())
                        .build())
                .setOutcome(ValidateAccrualResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Previews an accrual.
     */
    public PreviewAccrualResponse previewAccrual(PreviewAccrualRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewAccrual")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAccrual(request.getContext().getTenantId(), request.getAccrual());

        var outbound0 = RpcSupport.unpack(Any.pack(CompareAllocationOptionsRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareAllocationOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getAccrual().getBillingAddress().getLinesList())
                                .setRecipient(request.getAccrual().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build()), CompareAllocationOptionsRequest.class);
        var reply0 = accountingAllocationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareAllocationOptions(outbound0);

        var outbound1 = ExplainCoverageDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setBlock(CoverageBlockAction.newBuilder()
                                .setReason(reply0.getAllocation().getBlock().getReason())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply0.getAllocation().getOwner().getUserId())
                                .build())
                        .build())
                .setDetails(ExplainCoverageDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .build();
        Function<ExplainCoverageDecisionRequest, ExplainCoverageDecisionResponse> operation1 = insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainCoverageDecision;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAccrual().getAccrualId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewAccrual", request.getAccrual().getAccrualId());
        return PreviewAccrualResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getCoverage().getAudit().getCreatedAt())
                                .build())
                        .setDueAt(request.getAccrual().getDueAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Checks an accrual.
     */
    public CheckAccrualEligibilityResponse checkAccrualEligibility(CheckAccrualEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckAccrualEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAccrual(request.getContext().getTenantId(), request.getAccrual());

        var outbound0 = BrowseExpenseReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseExpenseReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getAccrual().getAudit().getCreatedAt())
                        .setOwnerId(request.getAccrual().getCounterparty().getAllocationId())
                        .setRegion(request.getAccrual().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.expenses.v1.ExpenseReportQueryService");
        var serviceName0 = "acme.expenses.v1" + "." + "ExpenseReportQueryService";
        var methodName0 = "Bro" + "wseExpenseReports";
        var descriptor0 = MethodDescriptor.<BrowseExpenseReportsRequest, BrowseExpenseReportsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(BrowseExpenseReportsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(BrowseExpenseReportsResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getAccrual().getAccrualId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckAccrualEligibility", request.getAccrual().getAccrualId());
        return CheckAccrualEligibilityResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .setDueAt(request.getAccrual().getDueAt())
                        .build())
                .setOutcome(CheckAccrualEligibilityResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Normalizes an accrual.
     */
    public NormalizeAccrualResponse normalizeAccrual(NormalizeAccrualRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeAccrual")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAccrual(request.getContext().getTenantId(), request.getAccrual());

        var outbound0 = ScoreFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = assetsFixedAssetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreFixedAsset(outbound0);

        var outbound1 = CountAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getFixedAsset().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getFixedAsset().getSpecsMap())
                        .setRegion(request.getAccrual().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countAuditEvents(outbound1);

        var outbound2 = ValidateCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getAccrual().getBillingAddress().getRecipient())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getFixedAsset().getAudit().getUpdatedBy())
                                .build())
                        .setDimensions(CollateralDimensions.newBuilder()
                                .setHeight(reply0.getFixedAsset().getDimensions().getHeight())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getFixedAsset().getOwner().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> lendingCollateralValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateCollateral(value));

        var outbound3 = EstimateFxHedgeRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedge(FxHedge.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getFixedAsset().getAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var command3 = new DispatchCommand<>("acme.treasury.v1.FxHedgeCalculationService/EstimateFxHedge", outbound3);
        var reply3 = treasuryFxHedgeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateFxHedge(command3.payload());
        log.atDebug().addKeyValue("auditValue", request.getAccrual().getAccrualId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeAccrual", request.getAccrual().getAccrualId());
        return NormalizeAccrualResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply2.getCollateral().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply3.getFxHedge().getBillingAddress().getLinesList())
                                .setRegion(reply0.getFixedAsset().getAddress().getRegion())
                                .build())
                        .setTotal(request.getAccrual().getTotal())
                        .build())
                .setOutcome(NormalizeAccrualResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Explains an accrual.
     */
    public ExplainAccrualDecisionResponse explainAccrualDecision(ExplainAccrualDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainAccrualDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAccrual(request.getContext().getTenantId(), request.getAccrual());

        var outbound0 = RpcSupport.unpack(Any.pack(ExplainUserDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getAccrual().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build()), ExplainUserDecisionRequest.class);
        var reply0 = identityUserValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainUserDecision(outbound0);

        var outbound1 = EstimateAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getUser().getRelationshipManager().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(EstimateAllocationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(reply0.getUser().getPrimaryAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var reply1 = accountingAllocationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateAllocation(outbound1);

        var outbound2 = ListFiscalPeriodSubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListFiscalPeriodSubscriptionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getAccrual().getAudit().getUpdatedAt())
                        .setOwnerId(request.getAccrual().getAccrualId())
                        .setRegion(reply0.getUser().getPrimaryAddress().getRegion())
                        .build())
                .build();
        var completion2 = new CompletableFuture<ListFiscalPeriodSubscriptionsResponse>();
        events.publishEvent(new ExplainAccrualDecisionStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getAccrual().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainAccrualDecision", request.getAccrual().getNumber());
        return ExplainAccrualDecisionResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getUser().getPrimaryAddress().getLinesList())
                                .build())
                        .setIssuedAt(request.getAccrual().getIssuedAt())
                        .addLines(AccrualLine.newBuilder()
                                .putAllAttributes(reply0.getUser().getAttributesMap())
                                .build())
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .setOutcome(ExplainAccrualDecisionResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Scores an accrual.
     */
    public ScoreAccrualResponse scoreAccrual(ScoreAccrualRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreAccrual")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAccrual(request.getContext().getTenantId(), request.getAccrual());

        var outbound0 = RecalculateAllocationsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateAllocationsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .build())
                .build();
        var reply0 = accountingAllocationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateAllocations(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getAccrual().getAccrualId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreAccrual", request.getAccrual().getAccrualId());
        return ScoreAccrualResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .setBillingAddress(request.getAccrual().getBillingAddress())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lints an accrual.
     */
    public LintAccrualsResponse lintAccruals(LintAccrualsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintAccruals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAccrual(request.getContext().getTenantId(), request.getAccrual());

        var outbound0 = (SimulateAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getAccrual().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(SimulateAllocationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getAccrual().getBillingAddress().getCountryCode())
                                .setLocality(request.getAccrual().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = accountingAllocationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateAllocation(outbound0);

        var outbound1 = NormalizeCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getAccrual().getAudit().getCreatedBy())
                                .setEtag(reply0.getAllocation().getAudit().getEtag())
                                .setUpdatedAt(reply0.getAllocation().getAudit().getUpdatedAt())
                                .build())
                        .setDiscount(CoverageDiscountAction.newBuilder()
                                .setPercent(reply0.getAllocation().getDiscount().getPercent())
                                .build())
                        .build())
                .build();
        Function<NormalizeCoverageRequest, NormalizeCoverageResponse> operation1 = insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::normalizeCoverage;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAccrual().getAccrualId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintAccruals", request.getAccrual().getAccrualId());
        return LintAccrualsResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getCoverage().getAudit().getEtag())
                                .build())
                        .setBillingAddress(request.getAccrual().getBillingAddress())
                        .build())
                .setOutcome(LintAccrualsResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns an accrual.
     */
    public GetAccrualRulesResponse getAccrualRules(GetAccrualRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAccrualRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Accrual", request.getAccrualId(), "GetAccrualRules");

        var outbound0 = CompareAllocationOptionsRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = accountingAllocationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareAllocationOptions(outbound0);
        return GetAccrualRulesResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
