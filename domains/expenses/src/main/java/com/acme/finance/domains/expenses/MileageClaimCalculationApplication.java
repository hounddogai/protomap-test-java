package com.acme.finance.domains.expenses;

import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.RecalculateFixedAssetsRequest;
import com.acme.assets.v1.RecalculateFixedAssetsResponse;
import com.acme.assets.v1.ScoreFixedAssetRequest;
import com.acme.assets.v1.ScoreFixedAssetResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.ValidateAuditEventRequest;
import com.acme.audit.v1.ValidateAuditEventResponse;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.ResolveBankAccountsRequest;
import com.acme.banking.v1.ResolveBankAccountsResponse;
import com.acme.collections.v1.BatchCreateCollectionCasesRequest;
import com.acme.collections.v1.BatchCreateCollectionCasesResponse;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.expenses.v1.CalculateMileageClaimRequest;
import com.acme.expenses.v1.CalculateMileageClaimResponse;
import com.acme.expenses.v1.CalculateReceiptRequest;
import com.acme.expenses.v1.CalculateReceiptResponse;
import com.acme.expenses.v1.CompareExpenseReportOptionsRequest;
import com.acme.expenses.v1.CompareExpenseReportOptionsResponse;
import com.acme.expenses.v1.CompareMileageClaimOptionsRequest;
import com.acme.expenses.v1.CompareMileageClaimOptionsResponse;
import com.acme.expenses.v1.EstimateExpenseReportRequest;
import com.acme.expenses.v1.EstimateExpenseReportResponse;
import com.acme.expenses.v1.EstimateMileageClaimRequest;
import com.acme.expenses.v1.EstimateMileageClaimResponse;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportBankSettlement;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ExplainMileageClaimCalculationRequest;
import com.acme.expenses.v1.ExplainMileageClaimCalculationResponse;
import com.acme.expenses.v1.GetMileageClaimRatesRequest;
import com.acme.expenses.v1.GetMileageClaimRatesResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.QuoteMileageClaimRequest;
import com.acme.expenses.v1.QuoteMileageClaimResponse;
import com.acme.expenses.v1.RecalculateExpenseReportsRequest;
import com.acme.expenses.v1.RecalculateExpenseReportsResponse;
import com.acme.expenses.v1.RecalculateMileageClaimsRequest;
import com.acme.expenses.v1.RecalculateMileageClaimsResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptBankSettlement;
import com.acme.expenses.v1.ReceiptCalculationServiceGrpc;
import com.acme.expenses.v1.ReceiptRef;
import com.acme.expenses.v1.SimulateExpenseReportRequest;
import com.acme.expenses.v1.SimulateExpenseReportResponse;
import com.acme.expenses.v1.SimulateReceiptRequest;
import com.acme.expenses.v1.SimulateReceiptResponse;
import com.acme.featurestore.v1.FeatureViewQueryServiceGrpc;
import com.acme.featurestore.v1.SuggestFeatureViewsRequest;
import com.acme.featurestore.v1.SuggestFeatureViewsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountBankFunding;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.NormalizeLedgerAccountRequest;
import com.acme.ledger.v1.NormalizeLedgerAccountResponse;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.lending.v1.ExplainLoanDecisionRequest;
import com.acme.lending.v1.ExplainLoanDecisionResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanValidationServiceGrpc;
import com.acme.licensing.v1.CreateSoftwareLicenseRequest;
import com.acme.licensing.v1.CreateSoftwareLicenseResponse;
import com.acme.licensing.v1.LicenseAssignmentRef;
import com.acme.licensing.v1.SoftwareLicense;
import com.acme.licensing.v1.SoftwareLicenseBankFunding;
import com.acme.licensing.v1.SoftwareLicenseServiceGrpc;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.UpdateMetricSeriesRequest;
import com.acme.metrics.v1.UpdateMetricSeriesResponse;
import com.acme.ratelimiting.v1.ListRateLimitsRequest;
import com.acme.ratelimiting.v1.ListRateLimitsResponse;
import com.acme.ratelimiting.v1.RateLimitServiceGrpc;
import com.acme.risk.v1.ScoreStressTestRequest;
import com.acme.risk.v1.ScoreStressTestResponse;
import com.acme.risk.v1.StressTest;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
 * Computes the figures of a mileage claim, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class MileageClaimCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(MileageClaimCalculationApplication.class);

    private final FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking;

    private final FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking;

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final CollectionCaseServiceGrpc.CollectionCaseServiceStub collectionsCollectionCaseServiceAsync;

    private final ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceBlockingStub expensesExpenseReportCalculationServiceBlocking;

    private final ReceiptCalculationServiceGrpc.ReceiptCalculationServiceBlockingStub expensesReceiptCalculationServiceBlocking;

    private final FeatureViewQueryServiceGrpc.FeatureViewQueryServiceBlockingStub featurestoreFeatureViewQueryServiceBlocking;

    private final LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking;

    private final SoftwareLicenseServiceGrpc.SoftwareLicenseServiceBlockingStub licensingSoftwareLicenseServiceBlocking;

    private final RateLimitServiceGrpc.RateLimitServiceBlockingStub ratelimitingRateLimitServiceBlocking;

    private final ExpensesRepository repository;

    private final Executor rpcExecutor;

    public MileageClaimCalculationApplication(
            FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking,
            FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking,
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking,
            ChannelRegistry channels,
            CollectionCaseServiceGrpc.CollectionCaseServiceStub collectionsCollectionCaseServiceAsync,
            ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceBlockingStub expensesExpenseReportCalculationServiceBlocking,
            ReceiptCalculationServiceGrpc.ReceiptCalculationServiceBlockingStub expensesReceiptCalculationServiceBlocking,
            FeatureViewQueryServiceGrpc.FeatureViewQueryServiceBlockingStub featurestoreFeatureViewQueryServiceBlocking,
            LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking,
            SoftwareLicenseServiceGrpc.SoftwareLicenseServiceBlockingStub licensingSoftwareLicenseServiceBlocking,
            RateLimitServiceGrpc.RateLimitServiceBlockingStub ratelimitingRateLimitServiceBlocking,
            ExpensesRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsFixedAssetCalculationServiceBlocking = assetsFixedAssetCalculationServiceBlocking;
        this.assetsFixedAssetValidationServiceBlocking = assetsFixedAssetValidationServiceBlocking;
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.bankingBankAccountQueryServiceBlocking = bankingBankAccountQueryServiceBlocking;
        this.channels = channels;
        this.collectionsCollectionCaseServiceAsync = collectionsCollectionCaseServiceAsync;
        this.expensesExpenseReportCalculationServiceBlocking = expensesExpenseReportCalculationServiceBlocking;
        this.expensesReceiptCalculationServiceBlocking = expensesReceiptCalculationServiceBlocking;
        this.featurestoreFeatureViewQueryServiceBlocking = featurestoreFeatureViewQueryServiceBlocking;
        this.ledgerLedgerAccountValidationServiceBlocking = ledgerLedgerAccountValidationServiceBlocking;
        this.licensingSoftwareLicenseServiceBlocking = licensingSoftwareLicenseServiceBlocking;
        this.ratelimitingRateLimitServiceBlocking = ratelimitingRateLimitServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Calculates a mileage claim.
     */
    public CalculateMileageClaimResponse calculateMileageClaim(CalculateMileageClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateMileageClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        var outbound0 = NormalizeLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getMileageClaim().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getMileageClaim().getAudit().getUpdatedBy())
                                .build())
                        .setBank(LedgerAccountBankFunding.newBuilder()
                                .setIban(switch (request.getMileageClaim().getSettlementCase()) {
                                    case BANK -> request.getMileageClaim().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setHolder(PostingBatchRef.newBuilder()
                                .setDisplayName(request.getMileageClaim().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = ledgerLedgerAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeLedgerAccount(outbound0);

        var outbound1 = BatchCreateCollectionCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateCollectionCasesRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateCollectionCasesResponse.getDefaultInstance());
        var input1 = collectionsCollectionCaseServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateCollectionCases(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getMileageClaim().getAudit().getCreatedAt())
                                .setCreatedBy(request.getMileageClaim().getAudit().getCreatedBy())
                                .setEtag(request.getMileageClaim().getAudit().getEtag())
                                .setUpdatedAt(request.getMileageClaim().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input2 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateMileageClaim", request.getMileageClaim().getMileageClaimId());
        return CalculateMileageClaimResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getLedgerAccount().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(ReceiptRef.newBuilder()
                                .setNumber(request.getMileageClaim().getNumber())
                                .build())
                        .setMileageClaimId(request.getMileageClaimId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Estimates a mileage claim.
     */
    public EstimateMileageClaimResponse estimateMileageClaim(EstimateMileageClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateMileageClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        var outbound0 = CreateSoftwareLicenseRequest.newBuilder()
                .setContext(request.getContext())
                .setSoftwareLicense(SoftwareLicense.newBuilder()
                        .setBank(SoftwareLicenseBankFunding.newBuilder()
                                .setAccountHolder(switch (request.getMileageClaim().getSettlementCase()) {
                                    case BANK -> request.getMileageClaim().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .setHolder(LicenseAssignmentRef.newBuilder()
                                .setDisplayName(request.getMileageClaim().getCounterparty().getDisplayName())
                                .setNumber(request.getMileageClaim().getCounterparty().getNumber())
                                .build())
                        .build())
                .build();
        var reply0 = licensingSoftwareLicenseServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createSoftwareLicense(outbound0);

        var outbound1 = UpdateMetricSeriesRequest.newBuilder()
                .setContext(request.getContext())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getMileageClaim().getBillingAddress().getLinesList())
                                .setLocality(request.getMileageClaim().getBillingAddress().getLocality())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getMileageClaim().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.metrics.v1.MetricSeriesService");
        var selected1 = MetricSeriesServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.updateMetricSeries(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateMileageClaim", request.getMileageClaim().getMileageClaimId());
        return EstimateMileageClaimResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getMetricSeries().getAudit().getCreatedAt())
                                .build())
                        .setMileageClaimId(request.getMileageClaimId())
                        .setStatus(request.getMileageClaim().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Quotes a mileage claim.
     */
    public QuoteMileageClaimResponse quoteMileageClaim(QuoteMileageClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteMileageClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        var outbound0 = EstimateExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateExpenseReportRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getMileageClaim().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.expenses.v1.ExpenseReportCalculationService");
        var selected0 = ExpenseReportCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.estimateExpenseReport(outbound0);

        var outbound1 = RecalculateExpenseReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.expenses.v1.ExpenseReportCalculationService");
        var selected1 = ExpenseReportCalculationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.recalculateExpenseReports(outbound1);

        var outbound2 = CompareExpenseReportOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getExpenseReport().getAudit().getCreatedAt())
                                .build())
                        .setBank(ExpenseReportBankSettlement.newBuilder()
                                .setAccountHolder(reply1.getExpenseReport().getBank().getAccountHolder())
                                .build())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(expensesExpenseReportCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareExpenseReportOptions(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteMileageClaim", request.getMileageClaim().getMileageClaimId());
        return QuoteMileageClaimResponse.newBuilder()
                .setBreakdown(QuoteMileageClaimResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .setTotal(reply2.getExpenseReport().getTotal())
                        .build())
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getExpenseReport().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply1.getExpenseReport().getBillingAddress().getLocality())
                                .build())
                        .setDueAt(reply1.getExpenseReport().getDueAt())
                        .setMileageClaimId(request.getMileageClaimId())
                        .setNotes(reply0.getExpenseReport().getNotes())
                        .build())
                .build();
    }

    /**
     * Compares a mileage claim.
     */
    public CompareMileageClaimOptionsResponse compareMileageClaimOptions(CompareMileageClaimOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareMileageClaimOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        var outbound0 = ExplainLoanDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setNumber(request.getMileageClaim().getCounterparty().getNumber())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.lending.v1.LoanValidationService");
        var selected0 = LoanValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.explainLoanDecision(outbound0);

        var outbound1 = ScoreStressTestRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreStressTestRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .build())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.risk.v1.StressTestValidationService");
        var serviceName1 = "acme.risk.v1" + "." + "StressTestValidationService";
        var methodName1 = "Sco" + "reStressTest";
        var descriptor1 = MethodDescriptor.<ScoreStressTestRequest, ScoreStressTestResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(ScoreStressTestRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ScoreStressTestResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareMileageClaimOptions", request.getMileageClaim().getMileageClaimId());
        return CompareMileageClaimOptionsResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setCounterparty(ReceiptRef.newBuilder()
                                .setNumber(reply1.getStressTest().getNumber())
                                .build())
                        .setMileageClaimId(request.getMileageClaimId())
                        .setNumber(request.getMileageClaim().getNumber())
                        .setTotal(reply1.getStressTest().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Recalculates a mileage claim.
     */
    public RecalculateMileageClaimsResponse recalculateMileageClaims(RecalculateMileageClaimsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateMileageClaims")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        var outbound0 = ResolveBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        final class Stage0Adapter {
            ResolveBankAccountsResponse fetch(ResolveBankAccountsRequest input) {
                return bankingBankAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveBankAccounts(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = BeanCopies.copy(SimulateExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulateExpenseReportRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getMileageClaim().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .build(), SimulateExpenseReportRequest.class);
        var reply1 = expensesExpenseReportCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateExpenseReport(outbound1);

        var results2 = new ArrayList<ValidateAuditEventResponse>();
        for (var element : request.getMileageClaim().getLinesList()) {
            ValidateAuditEventRequest itemRequest = ValidateAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .putAllAttributes(element.getAttributesMap())
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(request.getMileageClaim().getCounterparty().getDisplayName())
                                .setNumber(request.getMileageClaim().getCounterparty().getNumber())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
            results2.add(auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateAuditEvent(itemRequest));
        }
        var reply2 = results2.isEmpty() ? ValidateAuditEventResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateMileageClaims", request.getMileageClaim().getMileageClaimId());
        return RecalculateMileageClaimsResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getExpenseReport().getAudit().getEtag())
                                .build())
                        .setMileageClaimId(request.getMileageClaimId())
                        .setNumber(request.getMileageClaim().getNumber())
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Explains a mileage claim.
     */
    public ExplainMileageClaimCalculationResponse explainMileageClaimCalculation(ExplainMileageClaimCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainMileageClaimCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        var outbound0 = SuggestFeatureViewsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestFeatureViewsRequest.Filter.newBuilder()
                        .setRegion(request.getMileageClaim().getBillingAddress().getRegion())
                        .build())
                .build();
        RpcPort<SuggestFeatureViewsRequest, SuggestFeatureViewsResponse> port0 = RpcPort.adapt(featurestoreFeatureViewQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::suggestFeatureViews);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = (ScoreFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreFixedAssetRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getMileageClaim().getBillingAddress().getCountryCode())
                                .setPostalCode(request.getMileageClaim().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setLocality(request.getMileageClaim().getBillingAddress().getLocality())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getMileageClaim().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = assetsFixedAssetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreFixedAsset(outbound1);

        var outbound2 = SimulateReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setReceipt(Receipt.newBuilder()
                        .setBank(ReceiptBankSettlement.newBuilder()
                                .setIban(switch (request.getMileageClaim().getSettlementCase()) {
                                    case BANK -> request.getMileageClaim().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getMileageClaim().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var reply2 = expensesReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateReceipt(outbound2);

        var outbound3 = RpcSupport.unpack(Any.pack(CalculateReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateReceiptRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(reply1.getFixedAsset().getAddress().getLocality())
                                .build())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setBank(ReceiptBankSettlement.newBuilder()
                                .setBic(request.getMileageClaim().getBank().getBic())
                                .build())
                        .build())
                .build()), CalculateReceiptRequest.class);
        var reply3 = expensesReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateReceipt(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainMileageClaimCalculation", request.getMileageClaim().getMileageClaimId());
        return ExplainMileageClaimCalculationResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getFixedAsset().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply3.getReceipt().getBillingAddress().getLinesList())
                                .build())
                        .setCounterparty(request.getMileageClaim().getCounterparty())
                        .setDueAt(reply3.getReceipt().getDueAt())
                        .setMileageClaimId(request.getMileageClaimId())
                        .setNumber(reply2.getReceipt().getCounterparty().getNumber())
                        .build())
                .setOutcome(ExplainMileageClaimCalculationResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a mileage claim.
     */
    public GetMileageClaimRatesResponse getMileageClaimRates(GetMileageClaimRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetMileageClaimRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "MileageClaim", request.getMileageClaimId(), "GetMileageClaimRates");

        var outbound0 = ListRateLimitsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListRateLimitsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getMileageClaimId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> ratelimitingRateLimitServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listRateLimits(outbound0), rpcExecutor);

        var outbound1 = RecalculateFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .putComponents("primary", FixedAssetComponent.newBuilder()
                                .setComponentId(request.getMileageClaimId())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateFixedAssets(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        return GetMileageClaimRatesResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getFixedAsset().getAudit().getUpdatedBy())
                                .build())
                        .setMileageClaimId(request.getMileageClaimId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
