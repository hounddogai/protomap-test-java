package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.CalculateReconciliationRequest;
import com.acme.accounting.v1.CalculateReconciliationResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsValidationServiceGrpc;
import com.acme.accounting.v1.CheckChartOfAccountsEligibilityRequest;
import com.acme.accounting.v1.CheckChartOfAccountsEligibilityResponse;
import com.acme.accounting.v1.CompareReconciliationOptionsRequest;
import com.acme.accounting.v1.CompareReconciliationOptionsResponse;
import com.acme.accounting.v1.EstimateReconciliationRequest;
import com.acme.accounting.v1.EstimateReconciliationResponse;
import com.acme.accounting.v1.ExplainChartOfAccountsDecisionRequest;
import com.acme.accounting.v1.ExplainChartOfAccountsDecisionResponse;
import com.acme.accounting.v1.ExplainReconciliationCalculationRequest;
import com.acme.accounting.v1.ExplainReconciliationCalculationResponse;
import com.acme.accounting.v1.GetReconciliationRatesRequest;
import com.acme.accounting.v1.GetReconciliationRatesResponse;
import com.acme.accounting.v1.LintChartOfAccountsesRequest;
import com.acme.accounting.v1.LintChartOfAccountsesResponse;
import com.acme.accounting.v1.NormalizeChartOfAccountsRequest;
import com.acme.accounting.v1.NormalizeChartOfAccountsResponse;
import com.acme.accounting.v1.PreviewChartOfAccountsRequest;
import com.acme.accounting.v1.PreviewChartOfAccountsResponse;
import com.acme.accounting.v1.QuoteReconciliationRequest;
import com.acme.accounting.v1.QuoteReconciliationResponse;
import com.acme.accounting.v1.RecalculateReconciliationsRequest;
import com.acme.accounting.v1.RecalculateReconciliationsResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ScoreChartOfAccountsRequest;
import com.acme.accounting.v1.ScoreChartOfAccountsResponse;
import com.acme.accounting.v1.SimulateReconciliationRequest;
import com.acme.accounting.v1.SimulateReconciliationResponse;
import com.acme.assets.v1.BatchGetFixedAssetsRequest;
import com.acme.assets.v1.BatchGetFixedAssetsResponse;
import com.acme.assets.v1.CreateFixedAssetRequest;
import com.acme.assets.v1.CreateFixedAssetResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.NormalizeAccessPolicyRequest;
import com.acme.authorization.v1.NormalizeAccessPolicyResponse;
import com.acme.broadcasts.v1.Broadcast;
import com.acme.broadcasts.v1.BroadcastServiceGrpc;
import com.acme.broadcasts.v1.UpdateBroadcastRequest;
import com.acme.broadcasts.v1.UpdateBroadcastResponse;
import com.acme.collections.v1.CollectionCaseQueryServiceGrpc;
import com.acme.collections.v1.SuggestCollectionCasesRequest;
import com.acme.collections.v1.SuggestCollectionCasesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.identity.v1.ValidateUserRequest;
import com.acme.identity.v1.ValidateUserResponse;
import com.acme.insurance.v1.CalculateClaimRequest;
import com.acme.insurance.v1.CalculateClaimResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimChannelOrigin;
import com.acme.notifications.v1.CountNotificationsRequest;
import com.acme.notifications.v1.CountNotificationsResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.search.v1.CountSearchIndexesRequest;
import com.acme.search.v1.CountSearchIndexesResponse;
import com.acme.search.v1.FindSearchIndexesRequest;
import com.acme.search.v1.FindSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
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
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a reconciliation, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class ReconciliationCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(ReconciliationCalculationApplication.class);

    private final ChartOfAccountsValidationServiceGrpc.ChartOfAccountsValidationServiceBlockingStub accountingChartOfAccountsValidationServiceBlocking;

    private final ChartOfAccountsValidationServiceGrpc.ChartOfAccountsValidationServiceFutureStub accountingChartOfAccountsValidationServiceFuture;

    private final FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking;

    private final AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking;

    private final BroadcastServiceGrpc.BroadcastServiceBlockingStub broadcastsBroadcastServiceBlocking;

    private final CollectionCaseQueryServiceGrpc.CollectionCaseQueryServiceBlockingStub collectionsCollectionCaseQueryServiceBlocking;

    private final UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking;

    private final ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking;

    private final ReconciliationCalculationRequestMapper mapper;

    private final NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking;

    private final AccountingRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceFutureStub searchSearchIndexQueryServiceFuture;

    public ReconciliationCalculationApplication(
            ChartOfAccountsValidationServiceGrpc.ChartOfAccountsValidationServiceBlockingStub accountingChartOfAccountsValidationServiceBlocking,
            ChartOfAccountsValidationServiceGrpc.ChartOfAccountsValidationServiceFutureStub accountingChartOfAccountsValidationServiceFuture,
            FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking,
            AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking,
            BroadcastServiceGrpc.BroadcastServiceBlockingStub broadcastsBroadcastServiceBlocking,
            CollectionCaseQueryServiceGrpc.CollectionCaseQueryServiceBlockingStub collectionsCollectionCaseQueryServiceBlocking,
            UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking,
            ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking,
            ReconciliationCalculationRequestMapper mapper,
            NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking,
            AccountingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceFutureStub searchSearchIndexQueryServiceFuture) {
        this.accountingChartOfAccountsValidationServiceBlocking = accountingChartOfAccountsValidationServiceBlocking;
        this.accountingChartOfAccountsValidationServiceFuture = accountingChartOfAccountsValidationServiceFuture;
        this.assetsFixedAssetServiceBlocking = assetsFixedAssetServiceBlocking;
        this.authorizationAccessPolicyValidationServiceBlocking = authorizationAccessPolicyValidationServiceBlocking;
        this.broadcastsBroadcastServiceBlocking = broadcastsBroadcastServiceBlocking;
        this.collectionsCollectionCaseQueryServiceBlocking = collectionsCollectionCaseQueryServiceBlocking;
        this.identityUserValidationServiceBlocking = identityUserValidationServiceBlocking;
        this.insuranceClaimCalculationServiceBlocking = insuranceClaimCalculationServiceBlocking;
        this.mapper = mapper;
        this.notificationsNotificationQueryServiceBlocking = notificationsNotificationQueryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexQueryServiceBlocking = searchSearchIndexQueryServiceBlocking;
        this.searchSearchIndexQueryServiceFuture = searchSearchIndexQueryServiceFuture;
    }

    /**
     * Calculates a reconciliation.
     */
    public CalculateReconciliationResponse calculateReconciliation(CalculateReconciliationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateReconciliation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReconciliation(request.getContext().getTenantId(), request.getReconciliation());

        var outbound0 = CreateFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getReconciliation().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getReconciliation().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getReconciliation().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> assetsFixedAssetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createFixedAsset(value)).orElseThrow();

        var outbound1 = UpdateBroadcastRequest.newBuilder()
                .setBroadcast(Broadcast.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getFixedAsset().getAddress().getCountryCode())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = broadcastsBroadcastServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateBroadcast(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getReconciliation().getReconciliationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateReconciliation", request.getReconciliation().getReconciliationId());
        return CalculateReconciliationResponse.newBuilder()
                .setBreakdown(CalculateReconciliationResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getBroadcast().getAudit().getCreatedBy())
                                .setUpdatedAt(reply1.getBroadcast().getAudit().getUpdatedAt())
                                .build())
                        .setReconciliationId(request.getReconciliationId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Estimates a reconciliation.
     */
    public EstimateReconciliationResponse estimateReconciliation(EstimateReconciliationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateReconciliation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReconciliation(request.getContext().getTenantId(), request.getReconciliation());

        var outbound0 = ScoreChartOfAccountsRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getReconciliation().getAssignee().getEmail())
                                                .setUserId(request.getReconciliation().getAssignee().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> accountingChartOfAccountsValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreChartOfAccounts(outbound0), rpcExecutor);

        var outbound1 = BatchGetFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> assetsFixedAssetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetFixedAssets(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = CountNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountNotificationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getReconciliation().getAudit().getCreatedAt())
                        .setRegion(reply0.getChartOfAccounts().getLocation().getRegion())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> notificationsNotificationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countNotifications(value));
        log.atDebug().addKeyValue("auditValue", request.getReconciliation().getReconciliationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateReconciliation", request.getReconciliation().getReconciliationId());
        return EstimateReconciliationResponse.newBuilder()
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(request.getReconciliation().getAssignee())
                        .setReconciliationId(request.getReconciliationId())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getChartOfAccounts().getManager().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Quotes a reconciliation.
     */
    public QuoteReconciliationResponse quoteReconciliation(QuoteReconciliationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteReconciliation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReconciliation(request.getContext().getTenantId(), request.getReconciliation());

        List<NormalizeAccessPolicyResponse> results0 = request.getReconciliation().getCommentsList().stream()
                .map(element -> NormalizeAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getReconciliation().getCustomer().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeAccessPolicyRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(authorizationAccessPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeAccessPolicy(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? NormalizeAccessPolicyResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getReconciliation().getReconciliationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteReconciliation", request.getReconciliation().getReconciliationId());
        return QuoteReconciliationResponse.newBuilder()
                .setReconciliation(Reconciliation.newBuilder()
                        .putAllCustomFields(request.getReconciliation().getCustomFieldsMap())
                        .setReconciliationId(request.getReconciliationId())
                        .build())
                .build();
    }

    /**
     * Compares a reconciliation.
     */
    public CompareReconciliationOptionsResponse compareReconciliationOptions(CompareReconciliationOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareReconciliationOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReconciliation(request.getContext().getTenantId(), request.getReconciliation());

        var outbound0 = SuggestCollectionCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestCollectionCasesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getReconciliation().getAudit().getCreatedAt())
                        .putAllLabels(request.getReconciliation().getCustomFieldsMap())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        RpcPort<SuggestCollectionCasesRequest, SuggestCollectionCasesResponse> port0 = RpcPort.adapt(collectionsCollectionCaseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::suggestCollectionCases);
        var reply0 = port0.exchange(outbound0);

        var futures1 = request.getReconciliation().getCommentsList().stream()
                .map(element -> ExplainChartOfAccountsDecisionRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(element.getCreatedAt())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(element.getAuthorEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> accountingChartOfAccountsValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainChartOfAccountsDecision(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? ExplainChartOfAccountsDecisionResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getReconciliation().getReconciliationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareReconciliationOptions", request.getReconciliation().getReconciliationId());
        return CompareReconciliationOptionsResponse.newBuilder()
                .setReconciliation(Reconciliation.newBuilder()
                        .setReconciliationId(request.getReconciliationId())
                        .setStatus(request.getReconciliation().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Recalculates a reconciliation.
     */
    public RecalculateReconciliationsResponse recalculateReconciliations(RecalculateReconciliationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateReconciliations")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReconciliation(request.getContext().getTenantId(), request.getReconciliation());

        var outbound0 = CalculateClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getReconciliation().getAudit().getEtag())
                                .build())
                        .setChannel(ClaimChannelOrigin.newBuilder()
                                .setChannel(switch (request.getReconciliation().getOriginCase()) {
                                    case CHANNEL -> request.getReconciliation().getChannel().getChannel();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateClaim(outbound0);

        var outbound1 = NormalizeChartOfAccountsRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setUserId(reply0.getClaim().getAssignee().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending1 = accountingChartOfAccountsValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeChartOfAccounts(outbound1);
        var reply1 = RpcSupport.await(pending1);
        log.atDebug().addKeyValue("auditValue", request.getReconciliation().getReconciliationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateReconciliations", request.getReconciliation().getReconciliationId());
        return RecalculateReconciliationsResponse.newBuilder()
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(reply0.getClaim().getAssignee())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getChartOfAccounts().getAudit().getCreatedBy())
                                .build())
                        .putAllCustomFields(request.getReconciliation().getCustomFieldsMap())
                        .setReconciliationId(request.getReconciliationId())
                        .build())
                .build();
    }

    /**
     * Simulates a reconciliation.
     */
    public SimulateReconciliationResponse simulateReconciliation(SimulateReconciliationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateReconciliation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReconciliation(request.getContext().getTenantId(), request.getReconciliation());

        var outbound0 = mapper.simulateReconciliationStage0(request);
        var reply0 = accountingChartOfAccountsValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkChartOfAccountsEligibility(outbound0);

        var outbound1 = LintChartOfAccountsesRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedBy(request.getReconciliation().getAudit().getCreatedBy())
                                                .setEtag(reply0.getChartOfAccounts().getAudit().getEtag())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .setLocality(request.getDetails().getDestination().getLocality())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setUserId(request.getReconciliation().getAssignee().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var command1 = new DispatchCommand<>("acme.accounting.v1.ChartOfAccountsValidationService/LintChartOfAccountses", outbound1);
        var reply1 = accountingChartOfAccountsValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintChartOfAccountses(command1.payload());

        var outbound2 = FindSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindSearchIndexesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getReconciliation().getAudit().getCreatedAt())
                        .putAllLabels(reply1.getChartOfAccounts().getAttributesMap())
                        .setRegion(reply0.getChartOfAccounts().getLocation().getRegion())
                        .build())
                .build();
        var pending2 = searchSearchIndexQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).findSearchIndexes(outbound2);
        var reply2 = RpcSupport.await(pending2);
        log.atDebug().addKeyValue("auditValue", request.getReconciliation().getReconciliationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateReconciliation", request.getReconciliation().getReconciliationId());
        return SimulateReconciliationResponse.newBuilder()
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getChartOfAccounts().getManager().getDisplayName())
                                .setEmail(reply0.getChartOfAccounts().getManager().getEmail())
                                .setUserId(reply1.getChartOfAccounts().getManager().getUserId())
                                .build())
                        .setReconciliationId(request.getReconciliationId())
                        .setStatus(request.getReconciliation().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Explains a reconciliation.
     */
    public ExplainReconciliationCalculationResponse explainReconciliationCalculation(ExplainReconciliationCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainReconciliationCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReconciliation(request.getContext().getTenantId(), request.getReconciliation());

        var outbound0 = PreviewChartOfAccountsRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedBy(request.getReconciliation().getAudit().getCreatedBy())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getReconciliation().getAssignee().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        RpcPort<PreviewChartOfAccountsRequest, PreviewChartOfAccountsResponse> port0 = RpcPort.adapt(accountingChartOfAccountsValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewChartOfAccounts);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = CountSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountSearchIndexesRequest.Filter.newBuilder()
                        .setRegion(reply0.getChartOfAccounts().getLocation().getRegion())
                        .build())
                .build();
        final class Stage1Adapter {
            CountSearchIndexesResponse fetch(CountSearchIndexesRequest input) {
                return searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countSearchIndexes(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getReconciliation().getReconciliationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainReconciliationCalculation", request.getReconciliation().getReconciliationId());
        return ExplainReconciliationCalculationResponse.newBuilder()
                .setOutcome(ExplainReconciliationCalculationResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getChartOfAccounts().getManager().getDisplayName())
                                .setEmail(reply0.getChartOfAccounts().getManager().getEmail())
                                .build())
                        .setReconciliationId(request.getReconciliationId())
                        .setSubject(request.getReconciliation().getSubject())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a reconciliation.
     */
    public GetReconciliationRatesResponse getReconciliationRates(GetReconciliationRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetReconciliationRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Reconciliation", request.getReconciliationId(), "GetReconciliationRates");

        var outbound0 = ValidateUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<ValidateUserRequest, ValidateUserResponse>() {
            @Override
            public ValidateUserResponse apply(ValidateUserRequest input) {
                return identityUserValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateUser(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        return GetReconciliationRatesResponse.newBuilder()
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getUser().getContact().getEmail())
                                .build())
                        .setReconciliationId(request.getReconciliationId())
                        .build())
                .build();
    }
}
