package com.acme.finance.domains.expenses;

import com.acme.audit.v1.ListRetentionPolicyVersionsRequest;
import com.acme.audit.v1.ListRetentionPolicyVersionsResponse;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.certificates.v1.CertificateQueryServiceGrpc;
import com.acme.certificates.v1.SuggestCertificatesRequest;
import com.acme.certificates.v1.SuggestCertificatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.PreviewHouseholdRequest;
import com.acme.customers.v1.PreviewHouseholdResponse;
import com.acme.customers.v1.RecalculateHouseholdsRequest;
import com.acme.customers.v1.RecalculateHouseholdsResponse;
import com.acme.customers.v1.ValidateHouseholdRequest;
import com.acme.customers.v1.ValidateHouseholdResponse;
import com.acme.expenses.v1.CalculateExpenseReportRequest;
import com.acme.expenses.v1.CalculateExpenseReportResponse;
import com.acme.expenses.v1.CountMileageClaimTotalsRequest;
import com.acme.expenses.v1.CountMileageClaimTotalsResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyBlockAction;
import com.acme.expenses.v1.ExpensePolicyCalculationServiceGrpc;
import com.acme.expenses.v1.ExpensePolicySyncServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ExplainMileageClaimCalculationRequest;
import com.acme.expenses.v1.ExplainMileageClaimCalculationResponse;
import com.acme.expenses.v1.FetchMileageClaimReportRequest;
import com.acme.expenses.v1.FetchMileageClaimReportResponse;
import com.acme.expenses.v1.GenerateMileageClaimReportRequest;
import com.acme.expenses.v1.GenerateMileageClaimReportResponse;
import com.acme.expenses.v1.ListMileageClaimReportsRequest;
import com.acme.expenses.v1.ListMileageClaimReportsResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimCalculationServiceGrpc;
import com.acme.expenses.v1.PreviewReceiptRequest;
import com.acme.expenses.v1.PreviewReceiptResponse;
import com.acme.expenses.v1.ReceiptRef;
import com.acme.expenses.v1.ReceiptValidationServiceGrpc;
import com.acme.expenses.v1.ResolveExpensePolicyConflictRequest;
import com.acme.expenses.v1.ResolveExpensePolicyConflictResponse;
import com.acme.expenses.v1.ShareMileageClaimReportRequest;
import com.acme.expenses.v1.ShareMileageClaimReportResponse;
import com.acme.expenses.v1.SimulateExpensePolicyRequest;
import com.acme.expenses.v1.SimulateExpensePolicyResponse;
import com.acme.expenses.v1.StreamMileageClaimMetricsRequest;
import com.acme.expenses.v1.StreamMileageClaimMetricsResponse;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Builds and schedules reports about mileage claims. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class MileageClaimReportingApplication {
    private static final Logger log = LoggerFactory.getLogger(MileageClaimReportingApplication.class);

    private final RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking;

    private final CertificateQueryServiceGrpc.CertificateQueryServiceBlockingStub certificatesCertificateQueryServiceBlocking;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final ExpensePolicyCalculationServiceGrpc.ExpensePolicyCalculationServiceBlockingStub expensesExpensePolicyCalculationServiceBlocking;

    private final ExpensePolicySyncServiceGrpc.ExpensePolicySyncServiceBlockingStub expensesExpensePolicySyncServiceBlocking;

    private final ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceBlockingStub expensesExpenseReportCalculationServiceBlocking;

    private final MileageClaimCalculationServiceGrpc.MileageClaimCalculationServiceBlockingStub expensesMileageClaimCalculationServiceBlocking;

    private final ReceiptValidationServiceGrpc.ReceiptValidationServiceFutureStub expensesReceiptValidationServiceFuture;

    private final MileageClaimReportingRequestMapper mapper;

    private final ExpensesRepository repository;

    public MileageClaimReportingApplication(
            RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking,
            CertificateQueryServiceGrpc.CertificateQueryServiceBlockingStub certificatesCertificateQueryServiceBlocking,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            ExpensePolicyCalculationServiceGrpc.ExpensePolicyCalculationServiceBlockingStub expensesExpensePolicyCalculationServiceBlocking,
            ExpensePolicySyncServiceGrpc.ExpensePolicySyncServiceBlockingStub expensesExpensePolicySyncServiceBlocking,
            ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceBlockingStub expensesExpenseReportCalculationServiceBlocking,
            MileageClaimCalculationServiceGrpc.MileageClaimCalculationServiceBlockingStub expensesMileageClaimCalculationServiceBlocking,
            ReceiptValidationServiceGrpc.ReceiptValidationServiceFutureStub expensesReceiptValidationServiceFuture,
            MileageClaimReportingRequestMapper mapper,
            ExpensesRepository repository) {
        this.auditRetentionPolicyHistoryServiceBlocking = auditRetentionPolicyHistoryServiceBlocking;
        this.certificatesCertificateQueryServiceBlocking = certificatesCertificateQueryServiceBlocking;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.expensesExpensePolicyCalculationServiceBlocking = expensesExpensePolicyCalculationServiceBlocking;
        this.expensesExpensePolicySyncServiceBlocking = expensesExpensePolicySyncServiceBlocking;
        this.expensesExpenseReportCalculationServiceBlocking = expensesExpenseReportCalculationServiceBlocking;
        this.expensesMileageClaimCalculationServiceBlocking = expensesMileageClaimCalculationServiceBlocking;
        this.expensesReceiptValidationServiceFuture = expensesReceiptValidationServiceFuture;
        this.mapper = mapper;
        this.repository = repository;
    }

    /**
     * Generates a mileage claim.
     */
    public GenerateMileageClaimReportResponse generateMileageClaimReport(GenerateMileageClaimReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GenerateMileageClaimReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "MileageClaim", request.getMileageClaimId(), "GenerateMileageClaimReport");

        var outbound0 = mapper.generateMileageClaimReportStage0(request);
        var reply0 = expensesMileageClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainMileageClaimCalculation(outbound0);

        var outbound1 = mapper.generateMileageClaimReportStage1(request, reply0);
        var reply1 = customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateHousehold(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getFormat()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "GenerateMileageClaimReport", request.getDetails().getFormat());
        return GenerateMileageClaimReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a mileage claim.
     */
    public FetchMileageClaimReportResponse fetchMileageClaimReport(FetchMileageClaimReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FetchMileageClaimReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "MileageClaim", request.getMileageClaimId(), "FetchMileageClaimReport");
        return FetchMileageClaimReportResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setMileageClaimId(request.getMileageClaimId())
                        .build())
                .build();
    }

    /**
     * Streams the changes of mileage claims as they happen.
     */
    public StreamMileageClaimMetricsResponse streamMileageClaimMetrics(StreamMileageClaimMetricsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamMileageClaimMetrics")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "MileageClaim", request.getContext().getTraceId(), "StreamMileageClaimMetrics");

        var outbound0 = RpcSupport.unpack(Any.pack(ListRetentionPolicyVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListRetentionPolicyVersionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setQuery(request.getFilter().getQuery())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build()), ListRetentionPolicyVersionsRequest.class);
        var reply0 = auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listRetentionPolicyVersions(outbound0);

        var outbound1 = ResolveExpensePolicyConflictRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ResolveExpensePolicyConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setBlock(ExpensePolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getSubscriber().getEmail())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<ResolveExpensePolicyConflictRequest, ResolveExpensePolicyConflictResponse>() {
            @Override
            public ResolveExpensePolicyConflictResponse apply(ResolveExpensePolicyConflictRequest input) {
                return expensesExpensePolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveExpensePolicyConflict(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getOwnerId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamMileageClaimMetrics", request.getFilter().getOwnerId());
        return StreamMileageClaimMetricsResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Lists mileage claims a page at a time.
     */
    public ListMileageClaimReportsResponse listMileageClaimReports(ListMileageClaimReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListMileageClaimReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "MileageClaim", request.getContext().getTraceId(), "ListMileageClaimReports");

        var outbound0 = CalculateExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllMetadata(request.getFilter().getLabelsMap())
                        .build())
                .build();
        final class Stage0Adapter {
            CalculateExpenseReportResponse fetch(CalculateExpenseReportRequest input) {
                return expensesExpenseReportCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateExpenseReport(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListMileageClaimReports", request.getPage().getPageToken());
        return ListMileageClaimReportsResponse.newBuilder()
                .addMileageClaims(MileageClaim.newBuilder()
                        .setCounterparty(ReceiptRef.newBuilder()
                                .setNumber(reply0.getExpenseReport().getNumber())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Shares a mileage claim.
     */
    public ShareMileageClaimReportResponse shareMileageClaimReport(ShareMileageClaimReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ShareMileageClaimReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "MileageClaim", request.getMileageClaimId(), "ShareMileageClaimReport");

        var outbound0 = PreviewReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewReceiptRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .setName(request.getDetails().getRecipient().getName())
                                .setPhone(request.getDetails().getRecipient().getPhone())
                                .setPreferredChannel(request.getDetails().getRecipient().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending0 = expensesReceiptValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).previewReceipt(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = PreviewHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedAt(reply0.getReceipt().getAudit().getUpdatedAt())
                                                .setUpdatedBy(reply0.getReceipt().getAudit().getUpdatedBy())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .setCountryCode(reply0.getReceipt().getBillingAddress().getCountryCode())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewHousehold(value));

        var outbound2 = SimulateExpensePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulateExpensePolicyRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(reply1.getHousehold().getLocation().getLocality())
                                .build())
                        .build())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getHousehold().getAudit().getCreatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply1.getHousehold().getManager().getUserId())
                                .build())
                        .build())
                .build();
        var reply2 = expensesExpensePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateExpensePolicy(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ShareMileageClaimReport", request.getMileageClaimId());
        return ShareMileageClaimReportResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getExpensePolicy().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply1.getHousehold().getLocation().getLocality())
                                .build())
                        .setMileageClaimId(reply0.getReceipt().getCounterparty().getMileageClaimId())
                        .build())
                .setOutcome(ShareMileageClaimReportResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Counts mileage claims by status.
     */
    public CountMileageClaimTotalsResponse countMileageClaimTotals(CountMileageClaimTotalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountMileageClaimTotals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "MileageClaim", request.getContext().getTraceId(), "CountMileageClaimTotals");

        var outbound0 = RecalculateHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRegion(request.getFilter().getRegion())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getContext().getActor().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateHouseholds(outbound0);

        var outbound1 = mapper.countMileageClaimTotalsStage1(request);
        var reply1 = certificatesCertificateQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestCertificates(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountMileageClaimTotals", request.getPage().getPageToken());
        return CountMileageClaimTotalsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
