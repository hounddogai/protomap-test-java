package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.CheckFixedAssetEligibilityRequest;
import com.acme.assets.v1.CheckFixedAssetEligibilityResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.PushRetentionPolicyRequest;
import com.acme.audit.v1.PushRetentionPolicyResponse;
import com.acme.audit.v1.ResolveRetentionPolicyConflictRequest;
import com.acme.audit.v1.ResolveRetentionPolicyConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamAuditTrailNoticesRequest;
import com.acme.audit.v1.StreamAuditTrailNoticesResponse;
import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.UpdateBankAccountRequest;
import com.acme.banking.v1.UpdateBankAccountResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.CalculateForecastRequest;
import com.acme.budgeting.v1.CalculateForecastResponse;
import com.acme.budgeting.v1.CalculateVarianceRequest;
import com.acme.budgeting.v1.CalculateVarianceResponse;
import com.acme.budgeting.v1.CompareForecastOptionsRequest;
import com.acme.budgeting.v1.CompareForecastOptionsResponse;
import com.acme.budgeting.v1.CompareVarianceOptionsRequest;
import com.acme.budgeting.v1.CompareVarianceOptionsResponse;
import com.acme.budgeting.v1.EstimateForecastRequest;
import com.acme.budgeting.v1.EstimateForecastResponse;
import com.acme.budgeting.v1.EstimateVarianceRequest;
import com.acme.budgeting.v1.EstimateVarianceResponse;
import com.acme.budgeting.v1.ExplainBudgetCalculationRequest;
import com.acme.budgeting.v1.ExplainBudgetCalculationResponse;
import com.acme.budgeting.v1.ExplainBudgetLineCalculationRequest;
import com.acme.budgeting.v1.ExplainBudgetLineCalculationResponse;
import com.acme.budgeting.v1.ExplainVarianceCalculationRequest;
import com.acme.budgeting.v1.ExplainVarianceCalculationResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.ForecastCardSettlement;
import com.acme.budgeting.v1.GetVarianceRatesRequest;
import com.acme.budgeting.v1.GetVarianceRatesResponse;
import com.acme.budgeting.v1.QuoteForecastRequest;
import com.acme.budgeting.v1.QuoteForecastResponse;
import com.acme.budgeting.v1.QuoteVarianceRequest;
import com.acme.budgeting.v1.QuoteVarianceResponse;
import com.acme.budgeting.v1.RecalculateVariancesRequest;
import com.acme.budgeting.v1.RecalculateVariancesResponse;
import com.acme.budgeting.v1.SimulateVarianceRequest;
import com.acme.budgeting.v1.SimulateVarianceResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceComment;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.DeletePolicyRequest;
import com.acme.insurance.v1.DeletePolicyResponse;
import com.acme.insurance.v1.PolicyServiceGrpc;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.LintCollateralsRequest;
import com.acme.lending.v1.LintCollateralsResponse;
import com.acme.lending.v1.NormalizeCollateralRequest;
import com.acme.lending.v1.NormalizeCollateralResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationComment;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.ValidateNotificationRequest;
import com.acme.notifications.v1.ValidateNotificationResponse;
import com.acme.risk.v1.CalculateStressTestRequest;
import com.acme.risk.v1.CalculateStressTestResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestCalculationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a variance, such as totals, taxes, and estimates. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class VarianceCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(VarianceCalculationApplication.class);

    private final FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking;

    private final AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking;

    private final BankAccountServiceGrpc.BankAccountServiceBlockingStub bankingBankAccountServiceBlocking;

    private final BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking;

    private final BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking;

    private final ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking;

    private final PolicyServiceGrpc.PolicyServiceBlockingStub insurancePolicyServiceBlocking;

    private final CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking;

    private final NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking;

    private final BudgetingRepository repository;

    private final StressTestCalculationServiceGrpc.StressTestCalculationServiceBlockingStub riskStressTestCalculationServiceBlocking;

    private final Executor rpcExecutor;

    public VarianceCalculationApplication(
            FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking,
            AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking,
            BankAccountServiceGrpc.BankAccountServiceBlockingStub bankingBankAccountServiceBlocking,
            BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking,
            BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking,
            ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking,
            PolicyServiceGrpc.PolicyServiceBlockingStub insurancePolicyServiceBlocking,
            CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking,
            NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking,
            BudgetingRepository repository,
            StressTestCalculationServiceGrpc.StressTestCalculationServiceBlockingStub riskStressTestCalculationServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsFixedAssetValidationServiceBlocking = assetsFixedAssetValidationServiceBlocking;
        this.auditAuditTrailNotificationServiceBlocking = auditAuditTrailNotificationServiceBlocking;
        this.auditRetentionPolicySyncServiceBlocking = auditRetentionPolicySyncServiceBlocking;
        this.bankingBankAccountServiceBlocking = bankingBankAccountServiceBlocking;
        this.budgetingBudgetCalculationServiceBlocking = budgetingBudgetCalculationServiceBlocking;
        this.budgetingBudgetLineCalculationServiceBlocking = budgetingBudgetLineCalculationServiceBlocking;
        this.budgetingForecastCalculationServiceBlocking = budgetingForecastCalculationServiceBlocking;
        this.insurancePolicyServiceBlocking = insurancePolicyServiceBlocking;
        this.lendingCollateralValidationServiceBlocking = lendingCollateralValidationServiceBlocking;
        this.notificationsNotificationValidationServiceBlocking = notificationsNotificationValidationServiceBlocking;
        this.repository = repository;
        this.riskStressTestCalculationServiceBlocking = riskStressTestCalculationServiceBlocking;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Calculates a variance.
     */
    public CalculateVarianceResponse calculateVariance(CalculateVarianceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateVariance")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        List<CheckFixedAssetEligibilityResponse> results0 = request.getVariance().getCommentsList().stream()
                .map(element -> CheckFixedAssetEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckFixedAssetEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(assetsFixedAssetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkFixedAssetEligibility(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? CheckFixedAssetEligibilityResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = RpcSupport.unpack(Any.pack(QuoteForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getVariance().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .build()), QuoteForecastRequest.class);
        var reply1 = budgetingForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteForecast(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateVariance", request.getVariance().getVarianceId());
        return CalculateVarianceResponse.newBuilder()
                .setVariance(Variance.newBuilder()
                        .addComments(VarianceComment.newBuilder()
                                .setCreatedAt(reply1.getForecast().getAudit().getCreatedAt())
                                .build())
                        .setStatus(request.getVariance().getStatus())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Estimates a variance.
     */
    public EstimateVarianceResponse estimateVariance(EstimateVarianceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateVariance")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var outbound0 = UpdateBankAccountRequest.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getVariance().getAudit().getEtag())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        RpcPort<UpdateBankAccountRequest, UpdateBankAccountResponse> port0 = RpcPort.adapt(bankingBankAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::updateBankAccount);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = EstimateForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateForecastRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getBankAccount().getAudit().getEtag())
                                .build())
                        .setCard(ForecastCardSettlement.newBuilder()
                                .setNetwork(switch (reply0.getBankAccount().getFundingCase()) {
                                    case CARD -> reply0.getBankAccount().getCard().getNetwork();
                                    default -> "";
                                })
                                .setToken(switch (reply0.getBankAccount().getFundingCase()) {
                                    case CARD -> reply0.getBankAccount().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = budgetingForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateForecast(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateVariance", request.getVariance().getVarianceId());
        return EstimateVarianceResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAssignee(request.getVariance().getAssignee())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getBankAccount().getAudit().getCreatedBy())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(reply0.getBankAccount().getContact().getName())
                                .build())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Quotes a variance.
     */
    public QuoteVarianceResponse quoteVariance(QuoteVarianceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteVariance")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var futures0 = request.getVariance().getCommentsList().stream()
                .map(element -> CalculateStressTestRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateStressTestRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> riskStressTestCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateStressTest(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? CalculateStressTestResponse.getDefaultInstance()
                : results0.getLast();

        var futures1 = request.getVariance().getCommentsList().stream()
                .map(element -> ExplainBudgetLineCalculationRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .setEtag(request.getVariance().getAudit().getEtag())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(reply0.getStressTest().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainBudgetLineCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> budgetingBudgetLineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainBudgetLineCalculation(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? ExplainBudgetLineCalculationResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteVariance", request.getVariance().getVarianceId());
        return QuoteVarianceResponse.newBuilder()
                .setVariance(Variance.newBuilder()
                        .setStatus(request.getVariance().getStatus())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Compares a variance.
     */
    public CompareVarianceOptionsResponse compareVarianceOptions(CompareVarianceOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareVarianceOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var outbound0 = (LintCollateralsRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(LintCollateralsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getVariance().getAssignee().getEmail())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = lendingCollateralValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintCollaterals(outbound0);

        var outbound1 = ValidateNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getCollateral().getAudit().getUpdatedAt())
                                .build())
                        .addComments(NotificationComment.newBuilder()
                                .setCreatedAt(request.getVariance().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getVariance().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply1 = notificationsNotificationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateNotification(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareVarianceOptions", request.getVariance().getVarianceId());
        return CompareVarianceOptionsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getCollateral().getOwner().getDisplayName())
                                .build())
                        .setSubject(request.getVariance().getSubject())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Recalculates a variance.
     */
    public RecalculateVariancesResponse recalculateVariances(RecalculateVariancesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateVariances")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var results0 = new ArrayList<CompareForecastOptionsResponse>();
        for (var element : request.getVariance().getCommentsList()) {
            CompareForecastOptionsRequest itemRequest = CompareForecastOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareForecastOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .setCounterparty(BudgetRef.newBuilder()
                                .setDisplayName(request.getVariance().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
            results0.add(budgetingForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareForecastOptions(itemRequest));
        }
        var reply0 = results0.isEmpty() ? CompareForecastOptionsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = StreamAuditTrailNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamAuditTrailNoticesRequest.Filter.newBuilder()
                        .setRegion(reply0.getForecast().getBillingAddress().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getVariance().getRequester().getEmail())
                        .setName(request.getVariance().getRequester().getName())
                        .build())
                .build();
        var stream1 = auditAuditTrailNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditTrailNotices(outbound1);
        var reply1 = StreamAuditTrailNoticesResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateVariances", request.getVariance().getVarianceId());
        return RecalculateVariancesResponse.newBuilder()
                .setVariance(Variance.newBuilder()
                        .setRequester(request.getVariance().getRequester())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Simulates a variance.
     */
    public SimulateVarianceResponse simulateVariance(SimulateVarianceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateVariance")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var outbound0 = NormalizeCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeCollateralRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getVariance().getRequester().getPhone())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> lendingCollateralValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeCollateral(outbound0), rpcExecutor);

        var outbound1 = DeletePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setEtag(request.getVariance().getAudit().getEtag())
                .setPolicyId(request.getVariance().getSla().getPolicyId())
                .setReason(request.getReason())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insurancePolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).deletePolicy(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateVariance", request.getVariance().getVarianceId());
        return SimulateVarianceResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply0.getCollateral().getOwner().getUserId())
                                .build())
                        .setStatus(request.getVariance().getStatus())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Explains a variance.
     */
    public ExplainVarianceCalculationResponse explainVarianceCalculation(ExplainVarianceCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainVarianceCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var outbound0 = (CheckFixedAssetEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getVariance().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = assetsFixedAssetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkFixedAssetEligibility(outbound0);

        var futures1 = request.getVariance().getHistoryList().stream()
                .map(element -> PushRetentionPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PushRetentionPolicyRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(element.getChangedAt())
                                .setUpdatedBy(request.getVariance().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> auditRetentionPolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushRetentionPolicy(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? PushRetentionPolicyResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = CalculateForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateForecastRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(switch (reply0.getFixedAsset().getLocatorCase()) {
                                    case ADDRESS -> reply0.getFixedAsset().getAddress().getRegion();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getVariance().getAudit().getCreatedBy())
                                .setEtag(reply1.getRetentionPolicy().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getFixedAsset().getAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var reply2 = budgetingForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateForecast(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainVarianceCalculation", request.getVariance().getVarianceId());
        return ExplainVarianceCalculationResponse.newBuilder()
                .setOutcome(ExplainVarianceCalculationResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setVariance(Variance.newBuilder()
                        .putAllCustomFields(request.getVariance().getCustomFieldsMap())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Returns a variance.
     */
    public GetVarianceRatesResponse getVarianceRates(GetVarianceRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetVarianceRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getVarianceId(), "GetVarianceRates");

        var outbound0 = ExplainBudgetCalculationRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainBudgetCalculationRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        RpcPort<ExplainBudgetCalculationRequest, ExplainBudgetCalculationResponse> port0 = RpcPort.adapt(budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainBudgetCalculation);
        var reply0 = port0.exchange(outbound0);

        var builder1 = ResolveRetentionPolicyConflictRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setDetails(ResolveRetentionPolicyConflictRequest.Details.newBuilder()
                .setPayload(JsonCodec.encode(request.getContext()))
                .build());
        builder1.setRetentionPolicy(RetentionPolicy.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedAt(reply0.getBudget().getAudit().getUpdatedAt())
                        .build())
                .setOwner(UserRef.newBuilder()
                        .setDisplayName(reply0.getBudget().getHolder().getDisplayName())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = auditRetentionPolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveRetentionPolicyConflict(outbound1);
        return GetVarianceRatesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getBudget().getAudit().getEtag())
                                .setUpdatedAt(reply1.getRetentionPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getBudget().getContact().getEmail())
                                .build())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }
}
