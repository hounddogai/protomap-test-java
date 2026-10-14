package com.acme.finance.domains.payroll;

import com.acme.audit.v1.AnnotateAuditTrailVersionRequest;
import com.acme.audit.v1.AnnotateAuditTrailVersionResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.budgeting.v1.ImportVariancesRequest;
import com.acme.budgeting.v1.ImportVariancesResponse;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CoverageApprovalServiceGrpc;
import com.acme.insurance.v1.RejectCoverageRequest;
import com.acme.insurance.v1.RejectCoverageResponse;
import com.acme.insurance.v1.ShareUnderwritingReportRequest;
import com.acme.insurance.v1.ShareUnderwritingReportResponse;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingReportingServiceGrpc;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionBlockAction;
import com.acme.payroll.v1.DeductionCalculationServiceGrpc;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.DeductionValidationServiceGrpc;
import com.acme.payroll.v1.DeleteTaxWithholdingReportRequest;
import com.acme.payroll.v1.DeleteTaxWithholdingReportResponse;
import com.acme.payroll.v1.EstimatePayStubRequest;
import com.acme.payroll.v1.EstimatePayStubResponse;
import com.acme.payroll.v1.EstimateTaxWithholdingRequest;
import com.acme.payroll.v1.EstimateTaxWithholdingResponse;
import com.acme.payroll.v1.ExplainDeductionCalculationRequest;
import com.acme.payroll.v1.ExplainDeductionCalculationResponse;
import com.acme.payroll.v1.FetchTaxWithholdingReportRequest;
import com.acme.payroll.v1.FetchTaxWithholdingReportResponse;
import com.acme.payroll.v1.GenerateTaxWithholdingReportRequest;
import com.acme.payroll.v1.GenerateTaxWithholdingReportResponse;
import com.acme.payroll.v1.ListPayRunsRequest;
import com.acme.payroll.v1.ListPayRunsResponse;
import com.acme.payroll.v1.ListTaxWithholdingReportsRequest;
import com.acme.payroll.v1.ListTaxWithholdingReportsResponse;
import com.acme.payroll.v1.NormalizeDeductionRequest;
import com.acme.payroll.v1.NormalizeDeductionResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunBankSettlement;
import com.acme.payroll.v1.PayRunQueryServiceGrpc;
import com.acme.payroll.v1.PayRunServiceGrpc;
import com.acme.payroll.v1.PayRunValidationServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.PayStubLine;
import com.acme.payroll.v1.PreviewPayRunRequest;
import com.acme.payroll.v1.PreviewPayRunResponse;
import com.acme.payroll.v1.RecalculateTaxWithholdingsRequest;
import com.acme.payroll.v1.RecalculateTaxWithholdingsResponse;
import com.acme.payroll.v1.ResolvePayRunsRequest;
import com.acme.payroll.v1.ResolvePayRunsResponse;
import com.acme.payroll.v1.ScheduleTaxWithholdingReportRequest;
import com.acme.payroll.v1.ScheduleTaxWithholdingReportResponse;
import com.acme.payroll.v1.ShareTaxWithholdingReportRequest;
import com.acme.payroll.v1.ShareTaxWithholdingReportResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingCalculationServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingRouteAction;
import com.acme.personalization.v1.ProfileServiceGrpc;
import com.acme.personalization.v1.SearchProfilesRequest;
import com.acme.personalization.v1.SearchProfilesResponse;
import com.acme.procurement.v1.AdvancePurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.AdvancePurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.ListPendingPurchaseRequisitionsRequest;
import com.acme.procurement.v1.ListPendingPurchaseRequisitionsResponse;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionApprovalServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionWorkflowServiceGrpc;
import com.acme.procurement.v1.SubmitPurchaseRequisitionRequest;
import com.acme.procurement.v1.SubmitPurchaseRequisitionResponse;
import com.acme.risk.v1.RetryRiskScenarioWorkflowRequest;
import com.acme.risk.v1.RetryRiskScenarioWorkflowResponse;
import com.acme.risk.v1.RiskScenarioWorkflowServiceGrpc;
import com.google.protobuf.Any;
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
 * Builds and schedules reports about tax withholdings. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class TaxWithholdingReportingApplication {
    private static final Logger log = LoggerFactory.getLogger(TaxWithholdingReportingApplication.class);

    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking;

    private final VarianceImportServiceGrpc.VarianceImportServiceStub budgetingVarianceImportServiceAsync;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final CoverageApprovalServiceGrpc.CoverageApprovalServiceBlockingStub insuranceCoverageApprovalServiceBlocking;

    private final UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking;

    private final UnderwritingSyncServiceGrpc.UnderwritingSyncServiceStub insuranceUnderwritingSyncServiceAsync;

    private final TaxWithholdingReportingRequestMapper mapper;

    private final DeductionCalculationServiceGrpc.DeductionCalculationServiceBlockingStub payrollDeductionCalculationServiceBlocking;

    private final DeductionValidationServiceGrpc.DeductionValidationServiceBlockingStub payrollDeductionValidationServiceBlocking;

    private final PayRunQueryServiceGrpc.PayRunQueryServiceBlockingStub payrollPayRunQueryServiceBlocking;

    private final PayRunServiceGrpc.PayRunServiceBlockingStub payrollPayRunServiceBlocking;

    private final PayRunValidationServiceGrpc.PayRunValidationServiceBlockingStub payrollPayRunValidationServiceBlocking;

    private final PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking;

    private final TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceBlockingStub payrollTaxWithholdingCalculationServiceBlocking;

    private final ProfileServiceGrpc.ProfileServiceBlockingStub personalizationProfileServiceBlocking;

    private final PurchaseRequisitionApprovalServiceGrpc.PurchaseRequisitionApprovalServiceBlockingStub procurementPurchaseRequisitionApprovalServiceBlocking;

    private final PurchaseRequisitionWorkflowServiceGrpc.PurchaseRequisitionWorkflowServiceBlockingStub procurementPurchaseRequisitionWorkflowServiceBlocking;

    private final PayrollRepository repository;

    public TaxWithholdingReportingApplication(
            AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking,
            VarianceImportServiceGrpc.VarianceImportServiceStub budgetingVarianceImportServiceAsync,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            CoverageApprovalServiceGrpc.CoverageApprovalServiceBlockingStub insuranceCoverageApprovalServiceBlocking,
            UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking,
            UnderwritingSyncServiceGrpc.UnderwritingSyncServiceStub insuranceUnderwritingSyncServiceAsync,
            TaxWithholdingReportingRequestMapper mapper,
            DeductionCalculationServiceGrpc.DeductionCalculationServiceBlockingStub payrollDeductionCalculationServiceBlocking,
            DeductionValidationServiceGrpc.DeductionValidationServiceBlockingStub payrollDeductionValidationServiceBlocking,
            PayRunQueryServiceGrpc.PayRunQueryServiceBlockingStub payrollPayRunQueryServiceBlocking,
            PayRunServiceGrpc.PayRunServiceBlockingStub payrollPayRunServiceBlocking,
            PayRunValidationServiceGrpc.PayRunValidationServiceBlockingStub payrollPayRunValidationServiceBlocking,
            PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking,
            TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceBlockingStub payrollTaxWithholdingCalculationServiceBlocking,
            ProfileServiceGrpc.ProfileServiceBlockingStub personalizationProfileServiceBlocking,
            PurchaseRequisitionApprovalServiceGrpc.PurchaseRequisitionApprovalServiceBlockingStub procurementPurchaseRequisitionApprovalServiceBlocking,
            PurchaseRequisitionWorkflowServiceGrpc.PurchaseRequisitionWorkflowServiceBlockingStub procurementPurchaseRequisitionWorkflowServiceBlocking,
            PayrollRepository repository) {
        this.auditAuditTrailHistoryServiceBlocking = auditAuditTrailHistoryServiceBlocking;
        this.budgetingVarianceImportServiceAsync = budgetingVarianceImportServiceAsync;
        this.channels = channels;
        this.events = events;
        this.insuranceCoverageApprovalServiceBlocking = insuranceCoverageApprovalServiceBlocking;
        this.insuranceUnderwritingReportingServiceBlocking = insuranceUnderwritingReportingServiceBlocking;
        this.insuranceUnderwritingSyncServiceAsync = insuranceUnderwritingSyncServiceAsync;
        this.mapper = mapper;
        this.payrollDeductionCalculationServiceBlocking = payrollDeductionCalculationServiceBlocking;
        this.payrollDeductionValidationServiceBlocking = payrollDeductionValidationServiceBlocking;
        this.payrollPayRunQueryServiceBlocking = payrollPayRunQueryServiceBlocking;
        this.payrollPayRunServiceBlocking = payrollPayRunServiceBlocking;
        this.payrollPayRunValidationServiceBlocking = payrollPayRunValidationServiceBlocking;
        this.payrollPayStubCalculationServiceBlocking = payrollPayStubCalculationServiceBlocking;
        this.payrollTaxWithholdingCalculationServiceBlocking = payrollTaxWithholdingCalculationServiceBlocking;
        this.personalizationProfileServiceBlocking = personalizationProfileServiceBlocking;
        this.procurementPurchaseRequisitionApprovalServiceBlocking = procurementPurchaseRequisitionApprovalServiceBlocking;
        this.procurementPurchaseRequisitionWorkflowServiceBlocking = procurementPurchaseRequisitionWorkflowServiceBlocking;
        this.repository = repository;
    }

    public record ScheduleTaxWithholdingReportStage1Requested(EstimateTaxWithholdingRequest request, CompletableFuture<EstimateTaxWithholdingResponse> completion, io.grpc.Context context) {}

    /**
     * Generates a tax withholding.
     */
    public GenerateTaxWithholdingReportResponse generateTaxWithholdingReport(GenerateTaxWithholdingReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GenerateTaxWithholdingReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "TaxWithholding", request.getTaxWithholdingId(), "GenerateTaxWithholdingReport");

        var outbound0 = RejectCoverageRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(insuranceCoverageApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectCoverage(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = mapper.generateTaxWithholdingReportStage1(request, reply0);
        var reply1 = payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimatePayStub(outbound1);

        var outbound2 = SearchProfilesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchProfilesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getCoverage().getAudit().getUpdatedAt())
                        .putAllLabels(request.getDetails().getFiltersMap())
                        .setRegion(reply1.getPayStub().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = personalizationProfileServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchProfiles(outbound2);

        var builder3 = ListPendingPurchaseRequisitionsRequest.newBuilder();
        builder3.setContext(request.getContext());
        builder3.setFilter(ListPendingPurchaseRequisitionsRequest.Filter.newBuilder()
                .setRegion(reply1.getPayStub().getBillingAddress().getRegion())
                .build());
        var outbound3 = builder3.build();
        var reply3 = procurementPurchaseRequisitionApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingPurchaseRequisitions(outbound3);

        var outbound4 = ImportVariancesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(ImportVariancesRequest.StreamHeader.newBuilder()
                        .setFormat(request.getDetails().getFormat())
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(ImportVariancesResponse.getDefaultInstance());
        var input4 = budgetingVarianceImportServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).importVariances(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();

        var outbound5 = StreamUnderwritingChangesRequest.newBuilder()
                .setChange(Underwriting.newBuilder()
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
        var collector5 = new StreamCollector<>(StreamUnderwritingChangesResponse.getDefaultInstance());
        var input5 = insuranceUnderwritingSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamUnderwritingChanges(collector5);
        input5.onNext(outbound5);
        input5.onCompleted();
        var reply5 = collector5.await();
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "GenerateTaxWithholdingReport", request.getTaxWithholdingId());
        return GenerateTaxWithholdingReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getOutcome().getMessage())
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a tax withholding.
     */
    public FetchTaxWithholdingReportResponse fetchTaxWithholdingReport(FetchTaxWithholdingReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FetchTaxWithholdingReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "TaxWithholding", request.getTaxWithholdingId(), "FetchTaxWithholdingReport");

        var outbound0 = AdvancePurchaseRequisitionWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AdvancePurchaseRequisitionWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<AdvancePurchaseRequisitionWorkflowRequest, AdvancePurchaseRequisitionWorkflowResponse>() {
            @Override
            public AdvancePurchaseRequisitionWorkflowResponse apply(AdvancePurchaseRequisitionWorkflowRequest input) {
                return procurementPurchaseRequisitionWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advancePurchaseRequisitionWorkflow(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        return FetchTaxWithholdingReportResponse.newBuilder()
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPurchaseRequisition().getAudit().getCreatedAt())
                                .build())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Schedules a tax withholding.
     */
    public ScheduleTaxWithholdingReportResponse scheduleTaxWithholdingReport(ScheduleTaxWithholdingReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleTaxWithholdingReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "TaxWithholding", request.getTaxWithholdingId(), "ScheduleTaxWithholdingReport");

        var outbound0 = SubmitPurchaseRequisitionRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SubmitPurchaseRequisitionRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        final class Stage0Adapter {
            SubmitPurchaseRequisitionResponse fetch(SubmitPurchaseRequisitionRequest input) {
                return procurementPurchaseRequisitionApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitPurchaseRequisition(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = EstimateTaxWithholdingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateTaxWithholdingRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getPurchaseRequisition().getBillingAddress().getCountryCode())
                                .setRecipient(reply0.getPurchaseRequisition().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getPurchaseRequisition().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<EstimateTaxWithholdingResponse>();
        events.publishEvent(new ScheduleTaxWithholdingReportStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = RetryRiskScenarioWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RetryRiskScenarioWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(reply1.getTaxWithholding().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.risk.v1.RiskScenarioWorkflowService");
        var selected2 = RiskScenarioWorkflowServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.retryRiskScenarioWorkflow(outbound2);

        var outbound3 = PreviewPayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewPayRunRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply1.getTaxWithholding().getOwner().getEmail())
                                .build())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPurchaseRequisition().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getPurchaseRequisition().getAudit().getUpdatedAt())
                                .build())
                        .setBank(PayRunBankSettlement.newBuilder()
                                .setBic(switch (reply0.getPurchaseRequisition().getSettlementCase()) {
                                    case BANK -> reply0.getPurchaseRequisition().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply3 = payrollPayRunValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewPayRun(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleTaxWithholdingReport", request.getTaxWithholdingId());
        return ScheduleTaxWithholdingReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .setMessage(reply0.getOutcome().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists tax withholdings a page at a time.
     */
    public ListTaxWithholdingReportsResponse listTaxWithholdingReports(ListTaxWithholdingReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListTaxWithholdingReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "TaxWithholding", request.getContext().getTraceId(), "ListTaxWithholdingReports");

        var outbound0 = AnnotateAuditTrailVersionRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setDueAt(request.getFilter().getCreatedAfter())
                        .build())
                .setContext(request.getContext())
                .setDetails(AnnotateAuditTrailVersionRequest.Details.newBuilder()
                        .setAuthorEmail(request.getContext().getActor().getEmail())
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        RpcPort<AnnotateAuditTrailVersionRequest, AnnotateAuditTrailVersionResponse> port0 = RpcPort.adapt(auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::annotateAuditTrailVersion);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListTaxWithholdingReports", request.getPage().getPageToken());
        return ListTaxWithholdingReportsResponse.newBuilder()
                .addTaxWithholdings(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getAuditTrail().getAudit().getCreatedAt())
                                .setUpdatedBy(reply0.getAuditTrail().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
    }

    /**
     * Shares a tax withholding.
     */
    public ShareTaxWithholdingReportResponse shareTaxWithholdingReport(ShareTaxWithholdingReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ShareTaxWithholdingReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "TaxWithholding", request.getTaxWithholdingId(), "ShareTaxWithholdingReport");

        var outbound0 = NormalizeDeductionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeDeductionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .setPreferredChannel(request.getDetails().getRecipient().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> payrollDeductionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeDeduction(value));

        var outbound1 = ListPayRunsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPayRunsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getDeduction().getAudit().getCreatedAt())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> payrollPayRunServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPayRuns(value));

        var outbound2 = mapper.shareTaxWithholdingReportStage2(request, reply0);
        var reply2 = payrollTaxWithholdingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateTaxWithholdings(outbound2);

        var outbound3 = ShareUnderwritingReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareUnderwritingReportRequest.Details.newBuilder()
                        .setChannel(request.getDetails().getChannel())
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply2.getTaxWithholding().getOwner().getEmail())
                                .setName(request.getDetails().getRecipient().getName())
                                .setPhone(request.getDetails().getRecipient().getPhone())
                                .build())
                        .build())
                .build();
        var command3 = new DispatchCommand<>("acme.insurance.v1.UnderwritingReportingService/ShareUnderwritingReport", outbound3);
        var reply3 = insuranceUnderwritingReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareUnderwritingReport(command3.payload());
        log.atDebug().addKeyValue("auditValue", request.getDetails().getTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ShareTaxWithholdingReport", request.getDetails().getTemplateId());
        return ShareTaxWithholdingReportResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getDeduction().getAudit().getCreatedAt())
                                .setEtag(reply2.getTaxWithholding().getAudit().getEtag())
                                .build())
                        .setName(reply2.getTaxWithholding().getName())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Deletes a tax withholding.
     */
    public DeleteTaxWithholdingReportResponse deleteTaxWithholdingReport(DeleteTaxWithholdingReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteTaxWithholdingReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "TaxWithholding", request.getTaxWithholdingId(), "DeleteTaxWithholdingReport");

        var outbound0 = mapper.deleteTaxWithholdingReportStage0(request);
        var reply0 = payrollPayRunQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolvePayRuns(outbound0);

        var outbound1 = BeanCopies.copy(ExplainDeductionCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getEtag())
                                .build())
                        .setBlock(DeductionBlockAction.newBuilder()
                                .setReason(request.getReason())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build(), ExplainDeductionCalculationRequest.class);
        var reply1 = payrollDeductionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainDeductionCalculation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteTaxWithholdingReport", request.getTaxWithholdingId());
        return DeleteTaxWithholdingReportResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }
}
