package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalLifecycleServiceGrpc;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.ScheduleDisposalExpiryRequest;
import com.acme.assets.v1.ScheduleDisposalExpiryResponse;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.FindAccessPoliciesRequest;
import com.acme.authorization.v1.FindAccessPoliciesResponse;
import com.acme.budgeting.v1.BudgetQueryServiceGrpc;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.CountForecastTotalsRequest;
import com.acme.budgeting.v1.CountForecastTotalsResponse;
import com.acme.budgeting.v1.FetchForecastReportRequest;
import com.acme.budgeting.v1.FetchForecastReportResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.GenerateForecastReportRequest;
import com.acme.budgeting.v1.GenerateForecastReportResponse;
import com.acme.budgeting.v1.ListForecastReportsRequest;
import com.acme.budgeting.v1.ListForecastReportsResponse;
import com.acme.budgeting.v1.PullForecastsRequest;
import com.acme.budgeting.v1.PullForecastsResponse;
import com.acme.budgeting.v1.PushForecastRequest;
import com.acme.budgeting.v1.PushForecastResponse;
import com.acme.budgeting.v1.ResolveBudgetsRequest;
import com.acme.budgeting.v1.ResolveBudgetsResponse;
import com.acme.budgeting.v1.ScheduleForecastReportRequest;
import com.acme.budgeting.v1.ScheduleForecastReportResponse;
import com.acme.budgeting.v1.ShareForecastReportRequest;
import com.acme.budgeting.v1.ShareForecastReportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.NormalizeFeatureFlagRequest;
import com.acme.featureflags.v1.NormalizeFeatureFlagResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.forms.v1.Form;
import com.acme.forms.v1.FormServiceGrpc;
import com.acme.forms.v1.UpdateFormRequest;
import com.acme.forms.v1.UpdateFormResponse;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CoverageApprovalServiceGrpc;
import com.acme.insurance.v1.EscalateCoverageRequest;
import com.acme.insurance.v1.EscalateCoverageResponse;
import com.acme.insurance.v1.PullUnderwritingsRequest;
import com.acme.insurance.v1.PullUnderwritingsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.invoicing.v1.BatchCreateInvoiceBatchesRequest;
import com.acme.invoicing.v1.BatchCreateInvoiceBatchesResponse;
import com.acme.invoicing.v1.InvoiceBatchServiceGrpc;
import com.acme.metrics.v1.ListMetricSeriesesRequest;
import com.acme.metrics.v1.ListMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.suppliers.v1.LintSupplierRisksRequest;
import com.acme.suppliers.v1.LintSupplierRisksResponse;
import com.acme.suppliers.v1.PreviewSupplierRiskRequest;
import com.acme.suppliers.v1.PreviewSupplierRiskResponse;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskComment;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Builds and schedules reports about forecasts. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class ForecastReportingApplication {
    private static final Logger log = LoggerFactory.getLogger(ForecastReportingApplication.class);

    private final DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync;

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final BudgetQueryServiceGrpc.BudgetQueryServiceBlockingStub budgetingBudgetQueryServiceBlocking;

    private final ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking;

    private final ChannelRegistry channels;

    private final FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking;

    private final FormServiceGrpc.FormServiceBlockingStub formsFormServiceBlocking;

    private final CoverageApprovalServiceGrpc.CoverageApprovalServiceBlockingStub insuranceCoverageApprovalServiceBlocking;

    private final InvoiceBatchServiceGrpc.InvoiceBatchServiceStub invoicingInvoiceBatchServiceAsync;

    private final MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking;

    private final BudgetingRepository repository;

    private final Executor rpcExecutor;

    private final SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking;

    public ForecastReportingApplication(
            DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync,
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            BudgetQueryServiceGrpc.BudgetQueryServiceBlockingStub budgetingBudgetQueryServiceBlocking,
            ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking,
            ChannelRegistry channels,
            FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking,
            FormServiceGrpc.FormServiceBlockingStub formsFormServiceBlocking,
            CoverageApprovalServiceGrpc.CoverageApprovalServiceBlockingStub insuranceCoverageApprovalServiceBlocking,
            InvoiceBatchServiceGrpc.InvoiceBatchServiceStub invoicingInvoiceBatchServiceAsync,
            MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking,
            BudgetingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking) {
        this.assetsDisposalSyncServiceAsync = assetsDisposalSyncServiceAsync;
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.budgetingBudgetQueryServiceBlocking = budgetingBudgetQueryServiceBlocking;
        this.budgetingForecastSyncServiceBlocking = budgetingForecastSyncServiceBlocking;
        this.channels = channels;
        this.featureflagsFeatureFlagValidationServiceBlocking = featureflagsFeatureFlagValidationServiceBlocking;
        this.formsFormServiceBlocking = formsFormServiceBlocking;
        this.insuranceCoverageApprovalServiceBlocking = insuranceCoverageApprovalServiceBlocking;
        this.invoicingInvoiceBatchServiceAsync = invoicingInvoiceBatchServiceAsync;
        this.metricsMetricSeriesServiceBlocking = metricsMetricSeriesServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierRiskValidationServiceBlocking = suppliersSupplierRiskValidationServiceBlocking;
    }

    /**
     * Generates a forecast.
     */
    public GenerateForecastReportResponse generateForecastReport(GenerateForecastReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GenerateForecastReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Forecast", request.getForecastId(), "GenerateForecastReport");

        var outbound0 = ListMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListMetricSeriesesRequest.Filter.newBuilder()
                        .setOwnerId(request.getForecastId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> metricsMetricSeriesServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listMetricSerieses(outbound0), rpcExecutor);

        var outbound1 = ResolveBudgetsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> budgetingBudgetQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveBudgets(outbound1), rpcExecutor);

        var outbound2 = UpdateFormRequest.newBuilder()
                .setContext(request.getContext())
                .setForm(Form.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var pending2 = CompletableFuture.supplyAsync(() -> formsFormServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateForm(outbound2), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1, pending2).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        var reply2 = pending2.join();

        var outbound3 = BatchCreateInvoiceBatchesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateInvoiceBatchesRequest.StreamHeader.newBuilder()
                        .setFormat(request.getDetails().getFormat())
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(BatchCreateInvoiceBatchesResponse.getDefaultInstance());
        var input3 = invoicingInvoiceBatchServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateInvoiceBatches(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamDisposalChangesRequest.newBuilder()
                .setChange(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamDisposalChangesResponse.getDefaultInstance());
        var input4 = assetsDisposalSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamDisposalChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getCron()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "GenerateForecastReport", request.getDetails().getCron());
        return GenerateForecastReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a forecast.
     */
    public FetchForecastReportResponse fetchForecastReport(FetchForecastReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FetchForecastReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Forecast", request.getForecastId(), "FetchForecastReport");

        var builder0 = EscalateCoverageRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(EscalateCoverageRequest.Details.newBuilder()
                .setApprover(UserRef.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = insuranceCoverageApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateCoverage(outbound0);
        return FetchForecastReportResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getCoverage().getAudit().getUpdatedBy())
                                .build())
                        .setForecastId(request.getForecastId())
                        .build())
                .build();
    }

    /**
     * Schedules a forecast.
     */
    public ScheduleForecastReportResponse scheduleForecastReport(ScheduleForecastReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleForecastReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Forecast", request.getForecastId(), "ScheduleForecastReport");

        var outbound0 = ScheduleDisposalExpiryRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var channel0 = channels.lookup("acme.assets.v1.DisposalLifecycleService");
        var selected0 = DisposalLifecycleServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.scheduleDisposalExpiry(outbound0);

        var outbound1 = NormalizeFeatureFlagRequest.newBuilder()
                .setContext(request.getContext())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getDisposal().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply0.getDisposal().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.featureflags.v1.FeatureFlagValidationService/NormalizeFeatureFlag", outbound1);
        var reply1 = featureflagsFeatureFlagValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeFeatureFlag(command1.payload());

        var outbound2 = JsonCodec.roundTrip(PullForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PullForecastsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getFeatureFlag().getAudit().getCreatedAt())
                                .setCreatedBy(reply1.getFeatureFlag().getAudit().getCreatedBy())
                                .setEtag(reply0.getDisposal().getAudit().getEtag())
                                .build())
                        .build())
                .build(), PullForecastsRequest.class);
        var reply2 = budgetingForecastSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullForecasts(outbound2);

        var outbound3 = PreviewSupplierRiskRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(switch (reply0.getDisposal().getOriginCase()) {
                                    case CUSTOMER -> reply0.getDisposal().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getDisposal().getAudit().getUpdatedBy())
                                .build())
                        .addComments(SupplierRiskComment.newBuilder()
                                .setCreatedAt(reply2.getForecast().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var reply3 = suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewSupplierRisk(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleForecastReport", request.getForecastId());
        return ScheduleForecastReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .setMessage(reply3.getResult().getMessage())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists forecasts a page at a time.
     */
    public ListForecastReportsResponse listForecastReports(ListForecastReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListForecastReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Forecast", request.getContext().getTraceId(), "ListForecastReports");

        var builder0 = LintSupplierRisksRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(LintSupplierRisksRequest.Details.newBuilder()
                .setLocale(request.getContext().getLocale())
                .build());
        builder0.setSupplierRisk(SupplierRisk.newBuilder()
                .setAssignee(UserRef.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .setUserId(request.getContext().getActor().getUserId())
                        .build())
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedAt(request.getFilter().getCreatedAfter())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintSupplierRisks(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListForecastReports", request.getPage().getPageToken());
        return ListForecastReportsResponse.newBuilder()
                .addForecasts(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getSupplierRisk().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(BudgetRef.newBuilder()
                                .setDisplayName(reply0.getSupplierRisk().getCustomer().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Shares a forecast.
     */
    public ShareForecastReportResponse shareForecastReport(ShareForecastReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ShareForecastReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Forecast", request.getForecastId(), "ShareForecastReport");

        var outbound0 = PushForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PushForecastRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setForecastId(request.getForecastId())
                        .build())
                .build();
        final class Stage0Adapter {
            PushForecastResponse fetch(PushForecastRequest input) {
                return budgetingForecastSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushForecast(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = (FindAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindAccessPoliciesRequest.Filter.newBuilder()
                        .setRegion(reply0.getForecast().getBillingAddress().getRegion())
                        .build())
                .build()).toBuilder().build();
        var reply1 = authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findAccessPolicies(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ShareForecastReport", request.getDetails().getRecipient().getEmail());
        return ShareForecastReportResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setForecastId(request.getForecastId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Counts forecasts by status.
     */
    public CountForecastTotalsResponse countForecastTotals(CountForecastTotalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountForecastTotals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Forecast", request.getContext().getTraceId(), "CountForecastTotals");

        var outbound0 = PullUnderwritingsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PullUnderwritingsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.insurance.v1.UnderwritingSyncService");
        var selected0 = UnderwritingSyncServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.pullUnderwritings(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountForecastTotals", request.getPage().getPageToken());
        return CountForecastTotalsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }
}
