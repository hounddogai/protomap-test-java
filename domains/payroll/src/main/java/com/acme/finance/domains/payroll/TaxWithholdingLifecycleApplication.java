package com.acme.finance.domains.payroll;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.ListAuditEventsRequest;
import com.acme.audit.v1.ListAuditEventsResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetLifecycleServiceGrpc;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastLine;
import com.acme.budgeting.v1.PullForecastsRequest;
import com.acme.budgeting.v1.PullForecastsResponse;
import com.acme.budgeting.v1.ScheduleBudgetExpiryRequest;
import com.acme.budgeting.v1.ScheduleBudgetExpiryResponse;
import com.acme.catalog.v1.Product;
import com.acme.catalog.v1.ProductCalculationServiceGrpc;
import com.acme.catalog.v1.RecalculateProductsRequest;
import com.acme.catalog.v1.RecalculateProductsResponse;
import com.acme.certificates.v1.CertificateCalculationServiceGrpc;
import com.acme.certificates.v1.EstimateCertificateRequest;
import com.acme.certificates.v1.EstimateCertificateResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.documents.v1.ListDocumentsRequest;
import com.acme.documents.v1.ListDocumentsResponse;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportApprovalServiceGrpc;
import com.acme.expenses.v1.ExpenseReportBankSettlement;
import com.acme.expenses.v1.RecallExpenseReportRequest;
import com.acme.expenses.v1.RecallExpenseReportResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.forms.v1.FormQueryServiceGrpc;
import com.acme.forms.v1.SummarizeFormsRequest;
import com.acme.forms.v1.SummarizeFormsResponse;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.RejectClaimRequest;
import com.acme.insurance.v1.RejectClaimResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.insurance.v1.ValidateUnderwritingImportRequest;
import com.acme.insurance.v1.ValidateUnderwritingImportResponse;
import com.acme.payroll.v1.ActivateTaxWithholdingRequest;
import com.acme.payroll.v1.ActivateTaxWithholdingResponse;
import com.acme.payroll.v1.ComparePayStubOptionsRequest;
import com.acme.payroll.v1.ComparePayStubOptionsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionLifecycleServiceGrpc;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.DeductionRouteAction;
import com.acme.payroll.v1.EstimatePayRunRequest;
import com.acme.payroll.v1.EstimatePayRunResponse;
import com.acme.payroll.v1.ExpireTaxWithholdingRequest;
import com.acme.payroll.v1.ExpireTaxWithholdingResponse;
import com.acme.payroll.v1.ExplainTaxWithholdingDecisionRequest;
import com.acme.payroll.v1.ExplainTaxWithholdingDecisionResponse;
import com.acme.payroll.v1.GetTaxWithholdingLifecycleRequest;
import com.acme.payroll.v1.GetTaxWithholdingLifecycleResponse;
import com.acme.payroll.v1.ListPayRunsRequest;
import com.acme.payroll.v1.ListPayRunsResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.ReinstateTaxWithholdingRequest;
import com.acme.payroll.v1.ReinstateTaxWithholdingResponse;
import com.acme.payroll.v1.RenewTaxWithholdingRequest;
import com.acme.payroll.v1.RenewTaxWithholdingResponse;
import com.acme.payroll.v1.ScheduleDeductionExpiryRequest;
import com.acme.payroll.v1.ScheduleDeductionExpiryResponse;
import com.acme.payroll.v1.ScheduleTaxWithholdingExpiryRequest;
import com.acme.payroll.v1.ScheduleTaxWithholdingExpiryResponse;
import com.acme.payroll.v1.ScheduleTaxWithholdingReportRequest;
import com.acme.payroll.v1.ScheduleTaxWithholdingReportResponse;
import com.acme.payroll.v1.SuspendTaxWithholdingRequest;
import com.acme.payroll.v1.SuspendTaxWithholdingResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingReportingServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingValidationServiceGrpc;
import com.acme.payroll.v1.TerminateTaxWithholdingRequest;
import com.acme.payroll.v1.TerminateTaxWithholdingResponse;
import com.acme.risk.v1.ScheduleStressTestExpiryRequest;
import com.acme.risk.v1.ScheduleStressTestExpiryResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestLifecycleServiceGrpc;
import com.acme.risk.v1.StressTestLine;
import com.acme.risk.v1.StressTestLineTax;
import com.acme.seating.v1.BrowseSeatMapsRequest;
import com.acme.seating.v1.BrowseSeatMapsResponse;
import com.acme.seating.v1.SeatMapQueryServiceGrpc;
import com.acme.suppliers.v1.ListSupplierContractWorkflowsRequest;
import com.acme.suppliers.v1.ListSupplierContractWorkflowsResponse;
import com.acme.suppliers.v1.SignalSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.SignalSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.SupplierContractWorkflowServiceGrpc;
import com.acme.venues.v1.CloneVenueRequest;
import com.acme.venues.v1.CloneVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
 * Moves a tax withholding between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class TaxWithholdingLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(TaxWithholdingLifecycleApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking;

    private final BudgetLifecycleServiceGrpc.BudgetLifecycleServiceBlockingStub budgetingBudgetLifecycleServiceBlocking;

    private final ProductCalculationServiceGrpc.ProductCalculationServiceFutureStub catalogProductCalculationServiceFuture;

    private final CertificateCalculationServiceGrpc.CertificateCalculationServiceBlockingStub certificatesCertificateCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking;

    private final ExpenseReportApprovalServiceGrpc.ExpenseReportApprovalServiceBlockingStub expensesExpenseReportApprovalServiceBlocking;

    private final ClaimApprovalServiceGrpc.ClaimApprovalServiceBlockingStub insuranceClaimApprovalServiceBlocking;

    private final UnderwritingImportServiceGrpc.UnderwritingImportServiceBlockingStub insuranceUnderwritingImportServiceBlocking;

    private final TaxWithholdingLifecycleRequestMapper mapper;

    private final DeductionLifecycleServiceGrpc.DeductionLifecycleServiceBlockingStub payrollDeductionLifecycleServiceBlocking;

    private final PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking;

    private final PayRunServiceGrpc.PayRunServiceFutureStub payrollPayRunServiceFuture;

    private final TaxWithholdingReportingServiceGrpc.TaxWithholdingReportingServiceBlockingStub payrollTaxWithholdingReportingServiceBlocking;

    private final TaxWithholdingValidationServiceGrpc.TaxWithholdingValidationServiceBlockingStub payrollTaxWithholdingValidationServiceBlocking;

    private final PayrollRepository repository;

    private final StressTestLifecycleServiceGrpc.StressTestLifecycleServiceBlockingStub riskStressTestLifecycleServiceBlocking;

    private final Executor rpcExecutor;

    private final SeatMapQueryServiceGrpc.SeatMapQueryServiceBlockingStub seatingSeatMapQueryServiceBlocking;

    private final SupplierContractWorkflowServiceGrpc.SupplierContractWorkflowServiceBlockingStub suppliersSupplierContractWorkflowServiceBlocking;

    private final VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking;

    public TaxWithholdingLifecycleApplication(
            AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking,
            BudgetLifecycleServiceGrpc.BudgetLifecycleServiceBlockingStub budgetingBudgetLifecycleServiceBlocking,
            ProductCalculationServiceGrpc.ProductCalculationServiceFutureStub catalogProductCalculationServiceFuture,
            CertificateCalculationServiceGrpc.CertificateCalculationServiceBlockingStub certificatesCertificateCalculationServiceBlocking,
            ChannelRegistry channels,
            DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking,
            ExpenseReportApprovalServiceGrpc.ExpenseReportApprovalServiceBlockingStub expensesExpenseReportApprovalServiceBlocking,
            ClaimApprovalServiceGrpc.ClaimApprovalServiceBlockingStub insuranceClaimApprovalServiceBlocking,
            UnderwritingImportServiceGrpc.UnderwritingImportServiceBlockingStub insuranceUnderwritingImportServiceBlocking,
            TaxWithholdingLifecycleRequestMapper mapper,
            DeductionLifecycleServiceGrpc.DeductionLifecycleServiceBlockingStub payrollDeductionLifecycleServiceBlocking,
            PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking,
            PayRunServiceGrpc.PayRunServiceFutureStub payrollPayRunServiceFuture,
            TaxWithholdingReportingServiceGrpc.TaxWithholdingReportingServiceBlockingStub payrollTaxWithholdingReportingServiceBlocking,
            TaxWithholdingValidationServiceGrpc.TaxWithholdingValidationServiceBlockingStub payrollTaxWithholdingValidationServiceBlocking,
            PayrollRepository repository,
            StressTestLifecycleServiceGrpc.StressTestLifecycleServiceBlockingStub riskStressTestLifecycleServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SeatMapQueryServiceGrpc.SeatMapQueryServiceBlockingStub seatingSeatMapQueryServiceBlocking,
            SupplierContractWorkflowServiceGrpc.SupplierContractWorkflowServiceBlockingStub suppliersSupplierContractWorkflowServiceBlocking,
            VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking) {
        this.auditAuditEventServiceBlocking = auditAuditEventServiceBlocking;
        this.budgetingBudgetLifecycleServiceBlocking = budgetingBudgetLifecycleServiceBlocking;
        this.catalogProductCalculationServiceFuture = catalogProductCalculationServiceFuture;
        this.certificatesCertificateCalculationServiceBlocking = certificatesCertificateCalculationServiceBlocking;
        this.channels = channels;
        this.documentsDocumentServiceBlocking = documentsDocumentServiceBlocking;
        this.expensesExpenseReportApprovalServiceBlocking = expensesExpenseReportApprovalServiceBlocking;
        this.insuranceClaimApprovalServiceBlocking = insuranceClaimApprovalServiceBlocking;
        this.insuranceUnderwritingImportServiceBlocking = insuranceUnderwritingImportServiceBlocking;
        this.mapper = mapper;
        this.payrollDeductionLifecycleServiceBlocking = payrollDeductionLifecycleServiceBlocking;
        this.payrollPayRunCalculationServiceBlocking = payrollPayRunCalculationServiceBlocking;
        this.payrollPayRunServiceFuture = payrollPayRunServiceFuture;
        this.payrollTaxWithholdingReportingServiceBlocking = payrollTaxWithholdingReportingServiceBlocking;
        this.payrollTaxWithholdingValidationServiceBlocking = payrollTaxWithholdingValidationServiceBlocking;
        this.repository = repository;
        this.riskStressTestLifecycleServiceBlocking = riskStressTestLifecycleServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.seatingSeatMapQueryServiceBlocking = seatingSeatMapQueryServiceBlocking;
        this.suppliersSupplierContractWorkflowServiceBlocking = suppliersSupplierContractWorkflowServiceBlocking;
        this.venuesVenueServiceBlocking = venuesVenueServiceBlocking;
    }

    /**
     * Activates a tax withholding.
     */
    public ActivateTaxWithholdingResponse activateTaxWithholding(ActivateTaxWithholdingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateTaxWithholding")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "TaxWithholding", request.getTaxWithholdingId(), "ActivateTaxWithholding");

        var outbound0 = EstimatePayRunRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getDetails().getEffectiveAt())
                                .build())
                        .setCounterparty(DeductionRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var adapter0 = new Function<EstimatePayRunRequest, EstimatePayRunResponse>() {
            @Override
            public EstimatePayRunResponse apply(EstimatePayRunRequest input) {
                return payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimatePayRun(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = RecallExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecallExpenseReportRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(reply0.getPayRun().getCounterparty().getDisplayName())
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setBank(ExpenseReportBankSettlement.newBuilder()
                                .setIban(switch (reply0.getPayRun().getSettlementCase()) {
                                    case BANK -> reply0.getPayRun().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = expensesExpenseReportApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallExpenseReport(outbound1);

        var outbound2 = mapper.activateTaxWithholdingStage2(request);
        var reply2 = payrollTaxWithholdingReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleTaxWithholdingReport(outbound2);

        var outbound3 = ListAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getPayRun().getAudit().getCreatedAt())
                        .setRegion(reply1.getExpenseReport().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply3 = RpcSupport.retryRead(() -> auditAuditEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditEvents(outbound3));
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateTaxWithholding", request.getDetails().getReasonCode());
        return ActivateTaxWithholdingResponse.newBuilder()
                .setOutcome(ActivateTaxWithholdingResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPayRun().getAudit().getCreatedAt())
                                .build())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Suspends a tax withholding.
     */
    public SuspendTaxWithholdingResponse suspendTaxWithholding(SuspendTaxWithholdingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendTaxWithholding")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "TaxWithholding", request.getTaxWithholdingId(), "SuspendTaxWithholding");

        var outbound0 = BrowseSeatMapsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseSeatMapsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        Function<BrowseSeatMapsRequest, BrowseSeatMapsResponse> operation0 = seatingSeatMapQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::browseSeatMaps;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = ExplainTaxWithholdingDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainTaxWithholdingDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build();
        Function<ExplainTaxWithholdingDecisionRequest, ExplainTaxWithholdingDecisionResponse> operation1 = payrollTaxWithholdingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainTaxWithholdingDecision;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = PullForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PullForecastsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getTaxWithholding().getAudit().getCreatedBy())
                                .setUpdatedBy(reply1.getTaxWithholding().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(BudgetRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .addLines(ForecastLine.newBuilder()
                                .setAmount(reply1.getTaxWithholding().getDiscount().getAmount())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.budgeting.v1.ForecastSyncService");
        var serviceName2 = "acme.budgeting.v1" + "." + "ForecastSyncService";
        var methodName2 = "Pul" + "lForecasts";
        var descriptor2 = MethodDescriptor.<PullForecastsRequest, PullForecastsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(PullForecastsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(PullForecastsResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = RecalculateProductsRequest.newBuilder()
                .setContext(request.getContext())
                .setProduct(Product.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setLocality(reply2.getForecast().getBillingAddress().getLocality())
                                .setRecipient(reply2.getForecast().getBillingAddress().getRecipient())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getTaxWithholding().getAudit().getCreatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply1.getTaxWithholding().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var pending3 = catalogProductCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateProducts(outbound3);
        var reply3 = RpcSupport.await(pending3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendTaxWithholding", request.getDetails().getReasonCode());
        return SuspendTaxWithholdingResponse.newBuilder()
                .setOutcome(SuspendTaxWithholdingResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setEffective(reply1.getTaxWithholding().getEffective())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply2.getForecast().getCounterparty().getDisplayName())
                                .build())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Reinstates a tax withholding.
     */
    public ReinstateTaxWithholdingResponse reinstateTaxWithholding(ReinstateTaxWithholdingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateTaxWithholding")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "TaxWithholding", request.getTaxWithholdingId(), "ReinstateTaxWithholding");

        var outbound0 = ComparePayStubOptionsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .setCounterparty(DeductionRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var channel0 = channels.lookup("acme.payroll.v1.PayStubCalculationService");
        var selected0 = PayStubCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.comparePayStubOptions(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateTaxWithholding", request.getDetails().getReasonCode());
        return ReinstateTaxWithholdingResponse.newBuilder()
                .setOutcome(ReinstateTaxWithholdingResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Terminates a tax withholding.
     */
    public TerminateTaxWithholdingResponse terminateTaxWithholding(TerminateTaxWithholdingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateTaxWithholding")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "TaxWithholding", request.getTaxWithholdingId(), "TerminateTaxWithholding");

        var outbound0 = ScheduleBudgetExpiryRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ScheduleBudgetExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(budgetingBudgetLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleBudgetExpiry(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = ListDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListDocumentsRequest.Filter.newBuilder()
                        .putAllLabels(reply0.getBudget().getLabelsMap())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> documentsDocumentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listDocuments(value)).orElseThrow();

        var outbound2 = mapper.terminateTaxWithholdingStage2(request, reply0);
        var reply2 = suppliersSupplierContractWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listSupplierContractWorkflows(outbound2);

        var outbound3 = SummarizeFormsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeFormsRequest.Details.newBuilder()
                        .putAllFilters(reply0.getResult().getDetailsMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var channel3 = channels.lookup("acme.forms.v1.FormQueryService");
        var selected3 = FormQueryServiceGrpc.newBlockingStub(channel3)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply3 = selected3.summarizeForms(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateTaxWithholding", request.getDetails().getReasonCode());
        return TerminateTaxWithholdingResponse.newBuilder()
                .setOutcome(TerminateTaxWithholdingResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Renews a tax withholding.
     */
    public RenewTaxWithholdingResponse renewTaxWithholding(RenewTaxWithholdingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewTaxWithholding")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "TaxWithholding", request.getTaxWithholdingId(), "RenewTaxWithholding");

        var outbound0 = ValidateUnderwritingImportRequest.newBuilder()
                .setContext(request.getContext())
                .setUnderwriting(Underwriting.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = insuranceUnderwritingImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateUnderwritingImport(outbound0);

        var outbound1 = SignalSupplierContractWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SignalSupplierContractWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPreferredChannel(reply0.getUnderwriting().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.suppliers.v1.SupplierContractWorkflowService");
        var selected1 = SupplierContractWorkflowServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.signalSupplierContractWorkflow(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewTaxWithholding", request.getDetails().getReasonCode());
        return RenewTaxWithholdingResponse.newBuilder()
                .setOutcome(RenewTaxWithholdingResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Expires a tax withholding.
     */
    public ExpireTaxWithholdingResponse expireTaxWithholding(ExpireTaxWithholdingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireTaxWithholding")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "TaxWithholding", request.getTaxWithholdingId(), "ExpireTaxWithholding");

        var outbound0 = CloneVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setVenue(Venue.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> venuesVenueServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneVenue(outbound0), rpcExecutor);

        var outbound1 = ExplainTaxWithholdingDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainTaxWithholdingDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> payrollTaxWithholdingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainTaxWithholdingDecision(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = EstimateCertificateRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateCertificateRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(reply0.getVenue().getAddress().getLocality())
                                .build())
                        .build())
                .build();
        var adapter2 = new Function<EstimateCertificateRequest, EstimateCertificateResponse>() {
            @Override
            public EstimateCertificateResponse apply(EstimateCertificateRequest input) {
                return certificatesCertificateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateCertificate(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);

        var builder3 = RejectClaimRequest.newBuilder();
        builder3.setClaim(Claim.newBuilder()
                .setAssignee(UserRef.newBuilder()
                        .setUserId(reply0.getVenue().getOwner().getUserId())
                        .build())
                .build());
        builder3.setContext(request.getContext());
        builder3.setDetails(RejectClaimRequest.Details.newBuilder()
                .setApprover(UserRef.newBuilder()
                        .setEmail(reply0.getVenue().getOwner().getEmail())
                        .build())
                .build());
        var outbound3 = builder3.build();
        var reply3 = insuranceClaimApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectClaim(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireTaxWithholding", request.getDetails().getReasonCode());
        return ExpireTaxWithholdingResponse.newBuilder()
                .setOutcome(ExpireTaxWithholdingResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getVenue().getAudit().getEtag())
                                .setUpdatedBy(reply0.getVenue().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply3.getClaim().getCustomer().getDisplayName())
                                .build())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Returns a tax withholding.
     */
    public GetTaxWithholdingLifecycleResponse getTaxWithholdingLifecycle(GetTaxWithholdingLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetTaxWithholdingLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "TaxWithholding", request.getTaxWithholdingId(), "GetTaxWithholdingLifecycle");
        return GetTaxWithholdingLifecycleResponse.newBuilder()
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Schedules a tax withholding.
     */
    public ScheduleTaxWithholdingExpiryResponse scheduleTaxWithholdingExpiry(ScheduleTaxWithholdingExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleTaxWithholdingExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveTaxWithholding(request.getContext().getTenantId(), request.getTaxWithholding());

        var outbound0 = ScheduleStressTestExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setStressTest(StressTest.newBuilder()
                        .addLines(StressTestLine.newBuilder()
                                .setTax(StressTestLineTax.newBuilder()
                                        .setAmount(request.getTaxWithholding().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> riskStressTestLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleStressTestExpiry(value));

        var outbound1 = ScheduleDeductionExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getTaxWithholding().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getStressTest().getCounterparty().getEmail())
                                .build())
                        .setRoute(DeductionRouteAction.newBuilder()
                                .setTarget(switch (request.getTaxWithholding().getActionCase()) {
                                    case ROUTE -> request.getTaxWithholding().getRoute().getTarget();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = payrollDeductionLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleDeductionExpiry(outbound1);

        var outbound2 = ListPayRunsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPayRunsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getStressTest().getAudit().getUpdatedAt())
                        .putAllLabels(reply1.getDeduction().getParametersMap())
                        .setRegion(reply0.getStressTest().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending2 = payrollPayRunServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).listPayRuns(outbound2);
        var reply2 = RpcSupport.await(pending2);
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholding().getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleTaxWithholdingExpiry", request.getTaxWithholding().getTaxWithholdingId());
        return ScheduleTaxWithholdingExpiryResponse.newBuilder()
                .setOutcome(ScheduleTaxWithholdingExpiryResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getStressTest().getAudit().getEtag())
                                .build())
                        .setEffective(reply1.getDeduction().getEffective())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getStressTest().getCounterparty().getDisplayName())
                                .build())
                        .setStatus(request.getTaxWithholding().getStatus())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }
}
