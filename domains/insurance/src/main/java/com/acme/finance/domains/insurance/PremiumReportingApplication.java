package com.acme.finance.domains.insurance;

import com.acme.admissions.v1.AdmissionServiceGrpc;
import com.acme.admissions.v1.WatchAdmissionsRequest;
import com.acme.admissions.v1.WatchAdmissionsResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.CountAccessPoliciesRequest;
import com.acme.authorization.v1.CountAccessPoliciesResponse;
import com.acme.budgeting.v1.BrowseBudgetLinesRequest;
import com.acme.budgeting.v1.BrowseBudgetLinesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.ExplainUserDecisionRequest;
import com.acme.identity.v1.ExplainUserDecisionResponse;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.insurance.v1.CountPremiumTotalsRequest;
import com.acme.insurance.v1.CountPremiumTotalsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.DeletePremiumReportRequest;
import com.acme.insurance.v1.DeletePremiumReportResponse;
import com.acme.insurance.v1.EstimatePremiumRequest;
import com.acme.insurance.v1.EstimatePremiumResponse;
import com.acme.insurance.v1.FetchPremiumReportRequest;
import com.acme.insurance.v1.FetchPremiumReportResponse;
import com.acme.insurance.v1.GeneratePremiumReportRequest;
import com.acme.insurance.v1.GeneratePremiumReportResponse;
import com.acme.insurance.v1.ListPremiumReportsRequest;
import com.acme.insurance.v1.ListPremiumReportsResponse;
import com.acme.insurance.v1.NormalizePremiumRequest;
import com.acme.insurance.v1.NormalizePremiumResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyBankFunding;
import com.acme.insurance.v1.PolicyLifecycleServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumCalculationServiceGrpc;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.insurance.v1.SchedulePolicyExpiryRequest;
import com.acme.insurance.v1.SchedulePolicyExpiryResponse;
import com.acme.insurance.v1.SchedulePremiumReportRequest;
import com.acme.insurance.v1.SchedulePremiumReportResponse;
import com.acme.insurance.v1.SharePremiumReportRequest;
import com.acme.insurance.v1.SharePremiumReportResponse;
import com.acme.insurance.v1.SimulateCoverageRequest;
import com.acme.insurance.v1.SimulateCoverageResponse;
import com.acme.insurance.v1.StreamPremiumMetricsRequest;
import com.acme.insurance.v1.StreamPremiumMetricsResponse;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.ShareFiscalPeriodExportRequest;
import com.acme.ledger.v1.ShareFiscalPeriodExportResponse;
import com.acme.ledger.v1.StreamFiscalPeriodExportRequest;
import com.acme.ledger.v1.StreamFiscalPeriodExportResponse;
import com.acme.payroll.v1.CheckDeductionEligibilityRequest;
import com.acme.payroll.v1.CheckDeductionEligibilityResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionValidationServiceGrpc;
import com.acme.payroll.v1.FindPayRunsRequest;
import com.acme.payroll.v1.FindPayRunsResponse;
import com.acme.payroll.v1.PayRunQueryServiceGrpc;
import com.acme.suppliers.v1.BatchGetSuppliersRequest;
import com.acme.suppliers.v1.BatchGetSuppliersResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Builds and schedules reports about premiums. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class PremiumReportingApplication {
    private static final Logger log = LoggerFactory.getLogger(PremiumReportingApplication.class);

    private final AdmissionServiceGrpc.AdmissionServiceBlockingStub admissionsAdmissionServiceBlocking;

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking;

    private final CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking;

    private final PolicyLifecycleServiceGrpc.PolicyLifecycleServiceBlockingStub insurancePolicyLifecycleServiceBlocking;

    private final PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking;

    private final PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking;

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking;

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceFutureStub ledgerFiscalPeriodExportServiceFuture;

    private final DeductionValidationServiceGrpc.DeductionValidationServiceBlockingStub payrollDeductionValidationServiceBlocking;

    private final PayRunQueryServiceGrpc.PayRunQueryServiceBlockingStub payrollPayRunQueryServiceBlocking;

    private final InsuranceRepository repository;

    private final SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking;

    public PremiumReportingApplication(
            AdmissionServiceGrpc.AdmissionServiceBlockingStub admissionsAdmissionServiceBlocking,
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            ChannelRegistry channels,
            UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking,
            CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking,
            PolicyLifecycleServiceGrpc.PolicyLifecycleServiceBlockingStub insurancePolicyLifecycleServiceBlocking,
            PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking,
            PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking,
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking,
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceFutureStub ledgerFiscalPeriodExportServiceFuture,
            DeductionValidationServiceGrpc.DeductionValidationServiceBlockingStub payrollDeductionValidationServiceBlocking,
            PayRunQueryServiceGrpc.PayRunQueryServiceBlockingStub payrollPayRunQueryServiceBlocking,
            InsuranceRepository repository,
            SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking) {
        this.admissionsAdmissionServiceBlocking = admissionsAdmissionServiceBlocking;
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.channels = channels;
        this.identityUserValidationServiceBlocking = identityUserValidationServiceBlocking;
        this.insuranceCoverageCalculationServiceBlocking = insuranceCoverageCalculationServiceBlocking;
        this.insurancePolicyLifecycleServiceBlocking = insurancePolicyLifecycleServiceBlocking;
        this.insurancePremiumCalculationServiceBlocking = insurancePremiumCalculationServiceBlocking;
        this.insurancePremiumValidationServiceBlocking = insurancePremiumValidationServiceBlocking;
        this.ledgerFiscalPeriodExportServiceBlocking = ledgerFiscalPeriodExportServiceBlocking;
        this.ledgerFiscalPeriodExportServiceFuture = ledgerFiscalPeriodExportServiceFuture;
        this.payrollDeductionValidationServiceBlocking = payrollDeductionValidationServiceBlocking;
        this.payrollPayRunQueryServiceBlocking = payrollPayRunQueryServiceBlocking;
        this.repository = repository;
        this.suppliersSupplierServiceBlocking = suppliersSupplierServiceBlocking;
    }

    /**
     * Generates a premium.
     */
    public GeneratePremiumReportResponse generatePremiumReport(GeneratePremiumReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GeneratePremiumReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Premium", request.getPremiumId(), "GeneratePremiumReport");

        var outbound0 = ExplainUserDecisionRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .build();
        var adapter0 = new Function<ExplainUserDecisionRequest, ExplainUserDecisionResponse>() {
            @Override
            public ExplainUserDecisionResponse apply(ExplainUserDecisionRequest input) {
                return identityUserValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainUserDecision(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var builder1 = FindPayRunsRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setFilter(FindPayRunsRequest.Filter.newBuilder()
                .setCreatedAfter(reply0.getUser().getAudit().getUpdatedAt())
                .setOwnerId(reply0.getUser().getRelationshipManager().getUserId())
                .setRegion(reply0.getUser().getPrimaryAddress().getRegion())
                .build());
        var outbound1 = builder1.build();
        var reply1 = payrollPayRunQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findPayRuns(outbound1);

        var outbound2 = (CheckDeductionEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getUser().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setDetails(CheckDeductionEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply0.getUser().getContact().getName())
                                .setPreferredChannel(reply0.getUser().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = payrollDeductionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkDeductionEligibility(outbound2);

        var outbound3 = NormalizePremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getUser().getPrimaryAddress().getRecipient())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply2.getDeduction().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var reply3 = RpcSupport.call(outbound3, value -> insurancePremiumValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizePremium(value));
        log.atDebug().addKeyValue("auditValue", request.getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "GeneratePremiumReport", request.getPremiumId());
        return GeneratePremiumReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply2.getOutcome().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a premium.
     */
    public FetchPremiumReportResponse fetchPremiumReport(FetchPremiumReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FetchPremiumReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Premium", request.getPremiumId(), "FetchPremiumReport");
        return FetchPremiumReportResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setPremiumId(request.getPremiumId())
                        .build())
                .build();
    }

    /**
     * Schedules a premium.
     */
    public SchedulePremiumReportResponse schedulePremiumReport(SchedulePremiumReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SchedulePremiumReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Premium", request.getPremiumId(), "SchedulePremiumReport");

        var outbound0 = StreamFiscalPeriodExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamFiscalPeriodExportRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getDetails().getFiltersMap())
                        .build())
                .setFormat(request.getDetails().getFormat())
                .build();
        var stream0 = ledgerFiscalPeriodExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamFiscalPeriodExport(outbound0);
        var reply0 = StreamFiscalPeriodExportResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = BatchGetSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        final class Stage1Adapter {
            BatchGetSuppliersResponse fetch(BatchGetSuppliersRequest input) {
                return suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetSuppliers(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPremiumId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SchedulePremiumReport", request.getPremiumId());
        return SchedulePremiumReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Streams the changes of premiums as they happen.
     */
    public StreamPremiumMetricsResponse streamPremiumMetrics(StreamPremiumMetricsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamPremiumMetrics")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Premium", request.getContext().getTraceId(), "StreamPremiumMetrics");

        var outbound0 = ShareFiscalPeriodExportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareFiscalPeriodExportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .setName(request.getSubscriber().getName())
                                .build())
                        .build())
                .build();
        var pending0 = ledgerFiscalPeriodExportServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).shareFiscalPeriodExport(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = JsonCodec.roundTrip(SimulateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getFiscalPeriod().getOrganizer().getDisplayName())
                                .setEmail(reply0.getFiscalPeriod().getOrganizer().getEmail())
                                .setUserId(reply0.getFiscalPeriod().getOrganizer().getUserId())
                                .build())
                        .build())
                .setDetails(SimulateCoverageRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build(), SimulateCoverageRequest.class);
        var reply1 = insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateCoverage(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamPremiumMetrics", request.getFilter().getQuery());
        return StreamPremiumMetricsResponse.newBuilder()
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getFiscalPeriod().getAudit().getCreatedBy())
                                .setEtag(reply1.getCoverage().getAudit().getEtag())
                                .setUpdatedAt(reply1.getCoverage().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists premiums a page at a time.
     */
    public ListPremiumReportsResponse listPremiumReports(ListPremiumReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPremiumReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Premium", request.getContext().getTraceId(), "ListPremiumReports");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPremiumReports", request.getPage().getPageToken());
        return ListPremiumReportsResponse.getDefaultInstance();
    }

    /**
     * Shares a premium.
     */
    public SharePremiumReportResponse sharePremiumReport(SharePremiumReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SharePremiumReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Premium", request.getPremiumId(), "SharePremiumReport");

        var outbound0 = (CountAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountAccessPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getDetails().getVariablesMap())
                        .setOwnerId(request.getDetails().getTemplateId())
                        .build())
                .build()).toBuilder().build();
        var reply0 = authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countAccessPolicies(outbound0);

        var outbound1 = (EstimatePremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .setReason(request.getReason())
                .build()).toBuilder().build();
        var reply1 = insurancePremiumCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimatePremium(outbound1);

        var outbound2 = SchedulePolicyExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SchedulePolicyExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .setName(request.getDetails().getRecipient().getName())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setBank(PolicyBankFunding.newBuilder()
                                .setIban(switch (reply1.getPremium().getSettlementCase()) {
                                    case BANK -> reply1.getPremium().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = insurancePolicyLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).schedulePolicyExpiry(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SharePremiumReport", request.getDetails().getRecipient().getPhone());
        return SharePremiumReportResponse.newBuilder()
                .setOutcome(SharePremiumReportResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setBillingAddress(reply1.getPremium().getBillingAddress())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .setIssuedAt(reply1.getPremium().getIssuedAt())
                        .setPremiumId(request.getPremiumId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Counts premiums by status.
     */
    public CountPremiumTotalsResponse countPremiumTotals(CountPremiumTotalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountPremiumTotals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Premium", request.getContext().getTraceId(), "CountPremiumTotals");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountPremiumTotals", request.getPage().getPageToken());
        return CountPremiumTotalsResponse.getDefaultInstance();
    }

    /**
     * Deletes a premium.
     */
    public DeletePremiumReportResponse deletePremiumReport(DeletePremiumReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeletePremiumReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Premium", request.getPremiumId(), "DeletePremiumReport");

        var outbound0 = BrowseBudgetLinesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseBudgetLinesRequest.Filter.newBuilder()
                        .setOwnerId(request.getPremiumId())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.budgeting.v1.BudgetLineQueryService");
        var serviceName0 = "acme.budgeting.v1" + "." + "BudgetLineQueryService";
        var methodName0 = "Bro" + "wseBudgetLines";
        var descriptor0 = MethodDescriptor.<BrowseBudgetLinesRequest, BrowseBudgetLinesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(BrowseBudgetLinesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(BrowseBudgetLinesResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = WatchAdmissionsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .build();
        var stream1 = admissionsAdmissionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchAdmissions(outbound1);
        var reply1 = WatchAdmissionsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getReason()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeletePremiumReport", request.getReason());
        return DeletePremiumReportResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
