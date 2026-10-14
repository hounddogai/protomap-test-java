package com.acme.finance.domains.expenses;

import com.acme.budgeting.v1.ExplainVarianceDecisionRequest;
import com.acme.budgeting.v1.ExplainVarianceDecisionResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastCardSettlement;
import com.acme.budgeting.v1.ForecastValidationServiceGrpc;
import com.acme.budgeting.v1.PreviewForecastRequest;
import com.acme.budgeting.v1.PreviewForecastResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceComment;
import com.acme.budgeting.v1.VarianceValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCaseQueryServiceGrpc;
import com.acme.compliance.v1.ResolveComplianceCasesRequest;
import com.acme.compliance.v1.ResolveComplianceCasesResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.ExportDocumentsRequest;
import com.acme.documents.v1.ExportDocumentsResponse;
import com.acme.expenses.v1.CalculateExpenseReportRequest;
import com.acme.expenses.v1.CalculateExpenseReportResponse;
import com.acme.expenses.v1.CheckExpenseReportEligibilityRequest;
import com.acme.expenses.v1.CheckExpenseReportEligibilityResponse;
import com.acme.expenses.v1.EstimateReceiptRequest;
import com.acme.expenses.v1.EstimateReceiptResponse;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportAttachment;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ExpenseReportCardSettlement;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExplainExpenseReportCalculationRequest;
import com.acme.expenses.v1.ExplainExpenseReportCalculationResponse;
import com.acme.expenses.v1.ExplainExpenseReportDecisionRequest;
import com.acme.expenses.v1.ExplainExpenseReportDecisionResponse;
import com.acme.expenses.v1.GetExpenseReportRatesRequest;
import com.acme.expenses.v1.GetExpenseReportRatesResponse;
import com.acme.expenses.v1.GetExpenseReportRulesRequest;
import com.acme.expenses.v1.GetExpenseReportRulesResponse;
import com.acme.expenses.v1.LintExpenseReportsRequest;
import com.acme.expenses.v1.LintExpenseReportsResponse;
import com.acme.expenses.v1.PreviewExpenseReportRequest;
import com.acme.expenses.v1.PreviewExpenseReportResponse;
import com.acme.expenses.v1.QuoteExpenseReportRequest;
import com.acme.expenses.v1.QuoteExpenseReportResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptCalculationServiceGrpc;
import com.acme.expenses.v1.ScoreExpenseReportRequest;
import com.acme.expenses.v1.ScoreExpenseReportResponse;
import com.acme.expenses.v1.ValidateExpenseReportRequest;
import com.acme.expenses.v1.ValidateExpenseReportResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CheckCoverageEligibilityRequest;
import com.acme.insurance.v1.CheckCoverageEligibilityResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.licensing.v1.QuoteSoftwareLicenseRequest;
import com.acme.licensing.v1.QuoteSoftwareLicenseResponse;
import com.acme.licensing.v1.SoftwareLicense;
import com.acme.licensing.v1.SoftwareLicenseCalculationServiceGrpc;
import com.acme.licensing.v1.SoftwareLicenseCardFunding;
import com.acme.licensing.v1.SoftwareLicenseLimit;
import com.acme.procurement.v1.EstimatePurchaseOrderRequest;
import com.acme.procurement.v1.EstimatePurchaseOrderResponse;
import com.acme.procurement.v1.ExplainPurchaseRequisitionDecisionRequest;
import com.acme.procurement.v1.ExplainPurchaseRequisitionDecisionResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderAttachment;
import com.acme.procurement.v1.PurchaseOrderCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionAttachment;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.procurement.v1.ValidatePurchaseRequisitionRequest;
import com.acme.procurement.v1.ValidatePurchaseRequisitionResponse;
import java.util.ArrayList;
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
 * Checks expense reports against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ExpenseReportValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(ExpenseReportValidationApplication.class);

    private final ForecastValidationServiceGrpc.ForecastValidationServiceBlockingStub budgetingForecastValidationServiceBlocking;

    private final VarianceValidationServiceGrpc.VarianceValidationServiceBlockingStub budgetingVarianceValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ComplianceCaseQueryServiceGrpc.ComplianceCaseQueryServiceBlockingStub complianceComplianceCaseQueryServiceBlocking;

    private final DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking;

    private final ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceBlockingStub expensesExpenseReportCalculationServiceBlocking;

    private final ReceiptCalculationServiceGrpc.ReceiptCalculationServiceBlockingStub expensesReceiptCalculationServiceBlocking;

    private final CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking;

    private final SoftwareLicenseCalculationServiceGrpc.SoftwareLicenseCalculationServiceBlockingStub licensingSoftwareLicenseCalculationServiceBlocking;

    private final ExpenseReportValidationRequestMapper mapper;

    private final PurchaseOrderCalculationServiceGrpc.PurchaseOrderCalculationServiceBlockingStub procurementPurchaseOrderCalculationServiceBlocking;

    private final PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking;

    private final ExpensesRepository repository;

    private final Executor rpcExecutor;

    public ExpenseReportValidationApplication(
            ForecastValidationServiceGrpc.ForecastValidationServiceBlockingStub budgetingForecastValidationServiceBlocking,
            VarianceValidationServiceGrpc.VarianceValidationServiceBlockingStub budgetingVarianceValidationServiceBlocking,
            ChannelRegistry channels,
            ComplianceCaseQueryServiceGrpc.ComplianceCaseQueryServiceBlockingStub complianceComplianceCaseQueryServiceBlocking,
            DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking,
            ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceBlockingStub expensesExpenseReportCalculationServiceBlocking,
            ReceiptCalculationServiceGrpc.ReceiptCalculationServiceBlockingStub expensesReceiptCalculationServiceBlocking,
            CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking,
            SoftwareLicenseCalculationServiceGrpc.SoftwareLicenseCalculationServiceBlockingStub licensingSoftwareLicenseCalculationServiceBlocking,
            ExpenseReportValidationRequestMapper mapper,
            PurchaseOrderCalculationServiceGrpc.PurchaseOrderCalculationServiceBlockingStub procurementPurchaseOrderCalculationServiceBlocking,
            PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking,
            ExpensesRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.budgetingForecastValidationServiceBlocking = budgetingForecastValidationServiceBlocking;
        this.budgetingVarianceValidationServiceBlocking = budgetingVarianceValidationServiceBlocking;
        this.channels = channels;
        this.complianceComplianceCaseQueryServiceBlocking = complianceComplianceCaseQueryServiceBlocking;
        this.documentsDocumentQueryServiceBlocking = documentsDocumentQueryServiceBlocking;
        this.expensesExpenseReportCalculationServiceBlocking = expensesExpenseReportCalculationServiceBlocking;
        this.expensesReceiptCalculationServiceBlocking = expensesReceiptCalculationServiceBlocking;
        this.insuranceCoverageValidationServiceBlocking = insuranceCoverageValidationServiceBlocking;
        this.licensingSoftwareLicenseCalculationServiceBlocking = licensingSoftwareLicenseCalculationServiceBlocking;
        this.mapper = mapper;
        this.procurementPurchaseOrderCalculationServiceBlocking = procurementPurchaseOrderCalculationServiceBlocking;
        this.procurementPurchaseRequisitionValidationServiceBlocking = procurementPurchaseRequisitionValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Validates an expense report.
     */
    public ValidateExpenseReportResponse validateExpenseReport(ValidateExpenseReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateExpenseReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var results0 = new ArrayList<CalculateExpenseReportResponse>();
        for (var element : request.getExpenseReport().getAttachmentsList()) {
            CalculateExpenseReportRequest itemRequest = CalculateExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .addAttachments(ExpenseReportAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setContentType(element.getContentType())
                                .setFileName(element.getFileName())
                                .build())
                        .build())
                .build();
            results0.add(expensesExpenseReportCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateExpenseReport(itemRequest));
        }
        var reply0 = results0.isEmpty() ? CalculateExpenseReportResponse.getDefaultInstance()
                : results0.getLast();

        var futures1 = request.getExpenseReport().getAttachmentsList().stream()
                .map(element -> EstimatePurchaseOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .addAttachments(PurchaseOrderAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> procurementPurchaseOrderCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimatePurchaseOrder(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? EstimatePurchaseOrderResponse.getDefaultInstance()
                : results1.getLast();

        List<ValidatePurchaseRequisitionResponse> results2 = request.getExpenseReport().getAttachmentsList().stream()
                .map(element -> ValidatePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .addAttachments(PurchaseRequisitionAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setSizeBytes(element.getSizeBytes())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(procurementPurchaseRequisitionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePurchaseRequisition(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? ValidatePurchaseRequisitionResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = CheckCoverageEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getExpenseReport().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        Function<CheckCoverageEligibilityRequest, CheckCoverageEligibilityResponse> operation3 = insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::checkCoverageEligibility;
        var reply3 = operation3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateExpenseReport", request.getExpenseReport().getExpenseReportId());
        return ValidateExpenseReportResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setExpenseReportId(request.getExpenseReportId())
                        .setIssuedAt(request.getExpenseReport().getIssuedAt())
                        .addLines(ExpenseReportLine.newBuilder()
                                .setAmount(reply3.getCoverage().getDiscount().getAmount())
                                .build())
                        .build())
                .build();
    }

    /**
     * Previews an expense report.
     */
    public PreviewExpenseReportResponse previewExpenseReport(PreviewExpenseReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewExpenseReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = ResolveComplianceCasesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply0 = complianceComplianceCaseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveComplianceCases(outbound0);

        var builder1 = QuoteExpenseReportRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setExpenseReport(ExpenseReport.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedAt(request.getExpenseReport().getAudit().getCreatedAt())
                        .build())
                .setCard(ExpenseReportCardSettlement.newBuilder()
                        .setHolderName(request.getExpenseReport().getCard().getHolderName())
                        .build())
                .setCounterparty(CustomerRef.newBuilder()
                        .setEmail(request.getDetails().getContact().getEmail())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = expensesExpenseReportCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteExpenseReport(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewExpenseReport", request.getExpenseReport().getExpenseReportId());
        return PreviewExpenseReportResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply1.getExpenseReport().getCounterparty().getEmail())
                                .build())
                        .setExpenseReportId(request.getExpenseReportId())
                        .setNotes(reply1.getExpenseReport().getNotes())
                        .setNumber(request.getExpenseReport().getNumber())
                        .build())
                .setOutcome(PreviewExpenseReportResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Checks an expense report.
     */
    public CheckExpenseReportEligibilityResponse checkExpenseReportEligibility(CheckExpenseReportEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckExpenseReportEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = PreviewForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setForecast(Forecast.newBuilder()
                        .setCard(ForecastCardSettlement.newBuilder()
                                .setNetwork(switch (request.getExpenseReport().getSettlementCase()) {
                                    case CARD -> request.getExpenseReport().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = budgetingForecastValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewForecast(outbound0);

        var outbound1 = (ExplainExpenseReportCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getExpenseReport().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = expensesExpenseReportCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainExpenseReportCalculation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckExpenseReportEligibility", request.getExpenseReport().getExpenseReportId());
        return CheckExpenseReportEligibilityResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getForecast().getAudit().getUpdatedBy())
                                .build())
                        .setExpenseReportId(request.getExpenseReportId())
                        .setIssuedAt(reply0.getForecast().getIssuedAt())
                        .setNotes(request.getExpenseReport().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Explains an expense report.
     */
    public ExplainExpenseReportDecisionResponse explainExpenseReportDecision(ExplainExpenseReportDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainExpenseReportDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = EstimateReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateReceiptRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getExpenseReport().getBillingAddress().getLinesList())
                                .setPostalCode(request.getExpenseReport().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getExpenseReport().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getExpenseReport().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> expensesReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateReceipt(value)).orElseThrow();

        var outbound1 = GetExpenseReportRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReportId(request.getExpenseReport().getExpenseReportId())
                .build();
        final class Stage1Adapter {
            GetExpenseReportRatesResponse fetch(GetExpenseReportRatesRequest input) {
                return expensesExpenseReportCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getExpenseReportRates(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainExpenseReportDecision", request.getExpenseReport().getNumber());
        return ExplainExpenseReportDecisionResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setBillingAddress(reply0.getReceipt().getBillingAddress())
                        .setExpenseReportId(request.getExpenseReportId())
                        .putAllMetadata(reply1.getExpenseReport().getMetadataMap())
                        .setStatus(request.getExpenseReport().getStatus())
                        .setTotal(reply0.getReceipt().getTotal())
                        .build())
                .build();
    }

    /**
     * Scores an expense report.
     */
    public ScoreExpenseReportResponse scoreExpenseReport(ScoreExpenseReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreExpenseReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var futures0 = request.getExpenseReport().getLinesList().stream()
                .map(element -> QuoteSoftwareLicenseRequest.newBuilder()
                .setContext(request.getContext())
                .setSoftwareLicense(SoftwareLicense.newBuilder()
                        .setCard(SoftwareLicenseCardFunding.newBuilder()
                                .setToken(request.getExpenseReport().getCard().getToken())
                                .build())
                        .addLimits(SoftwareLicenseLimit.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> licensingSoftwareLicenseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteSoftwareLicense(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? QuoteSoftwareLicenseResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ExplainVarianceDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainVarianceDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getExpenseReport().getCounterparty().getEmail())
                                .setName(reply0.getSoftwareLicense().getContact().getName())
                                .build())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getExpenseReport().getAudit().getUpdatedAt())
                                .build())
                        .addComments(VarianceComment.newBuilder()
                                .setCreatedAt(reply0.getSoftwareLicense().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> budgetingVarianceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainVarianceDecision(value));

        var outbound2 = mapper.scoreExpenseReportStage2(request, reply1);
        var reply2 = expensesExpenseReportCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainExpenseReportCalculation(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreExpenseReport", request.getExpenseReport().getExpenseReportId());
        return ScoreExpenseReportResponse.newBuilder()
                .setBreakdown(ScoreExpenseReportResponse.Breakdown.newBuilder()
                        .setTotal(request.getExpenseReport().getTotal())
                        .build())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply2.getExpenseReport().getCounterparty().getDisplayName())
                                .build())
                        .setExpenseReportId(request.getExpenseReportId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lints an expense report.
     */
    public LintExpenseReportsResponse lintExpenseReports(LintExpenseReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintExpenseReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var builder0 = QuoteExpenseReportRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setExpenseReport(ExpenseReport.newBuilder()
                .setBillingAddress(PostalAddress.newBuilder()
                        .setRecipient(request.getExpenseReport().getBillingAddress().getRecipient())
                        .build())
                .setCounterparty(CustomerRef.newBuilder()
                        .setCustomerId(request.getExpenseReport().getCounterparty().getCustomerId())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = expensesExpenseReportCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteExpenseReport(outbound0);

        var outbound1 = ExportDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportDocumentsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getExpenseReport().getDueAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(reply0.getExpenseReport().getCounterparty().getCustomerId())
                        .setRegion(reply0.getExpenseReport().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = documentsDocumentQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportDocuments(outbound1);
        var reply1 = ExportDocumentsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintExpenseReports", request.getExpenseReport().getExpenseReportId());
        return LintExpenseReportsResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getExpenseReport().getBillingAddress().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setCustomerId(reply0.getExpenseReport().getCounterparty().getCustomerId())
                                .build())
                        .setExpenseReportId(request.getExpenseReportId())
                        .setNumber(request.getExpenseReport().getNumber())
                        .build())
                .build();
    }

    /**
     * Returns an expense report.
     */
    public GetExpenseReportRulesResponse getExpenseReportRules(GetExpenseReportRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetExpenseReportRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpenseReport", request.getExpenseReportId(), "GetExpenseReportRules");

        var outbound0 = ExplainPurchaseRequisitionDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPurchaseRequisitionDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.procurement.v1.PurchaseRequisitionValidationService");
        var selected0 = PurchaseRequisitionValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.explainPurchaseRequisitionDecision(outbound0);
        return GetExpenseReportRulesResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPurchaseRequisition().getAudit().getCreatedAt())
                                .build())
                        .setExpenseReportId(request.getExpenseReportId())
                        .build())
                .build();
    }
}
