package com.acme.finance.domains.expenses;

import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.ShareRetentionPolicyExportRequest;
import com.acme.audit.v1.ShareRetentionPolicyExportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.LintComplianceCasesRequest;
import com.acme.compliance.v1.LintComplianceCasesResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.PreviewDocumentRequest;
import com.acme.documents.v1.PreviewDocumentResponse;
import com.acme.documents.v1.ValidateDocumentRequest;
import com.acme.documents.v1.ValidateDocumentResponse;
import com.acme.expenses.v1.BrowseExpenseReportsRequest;
import com.acme.expenses.v1.BrowseExpenseReportsResponse;
import com.acme.expenses.v1.CountExpenseReportsRequest;
import com.acme.expenses.v1.CountExpenseReportsResponse;
import com.acme.expenses.v1.EstimateExpensePolicyRequest;
import com.acme.expenses.v1.EstimateExpensePolicyResponse;
import com.acme.expenses.v1.ExpensePolicyCalculationServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportCardSettlement;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExpenseReportLineTax;
import com.acme.expenses.v1.ExpenseReportValidationServiceGrpc;
import com.acme.expenses.v1.ExplainMileageClaimCalculationRequest;
import com.acme.expenses.v1.ExplainMileageClaimCalculationResponse;
import com.acme.expenses.v1.ExportExpenseReportsRequest;
import com.acme.expenses.v1.ExportExpenseReportsResponse;
import com.acme.expenses.v1.FindExpenseReportsRequest;
import com.acme.expenses.v1.FindExpenseReportsResponse;
import com.acme.expenses.v1.LookupExpenseReportRequest;
import com.acme.expenses.v1.LookupExpenseReportResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimCalculationServiceGrpc;
import com.acme.expenses.v1.ResolveExpenseReportsRequest;
import com.acme.expenses.v1.ResolveExpenseReportsResponse;
import com.acme.expenses.v1.ValidateExpenseReportRequest;
import com.acme.expenses.v1.ValidateExpenseReportResponse;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.procurement.v1.NormalizePurchaseRequisitionRequest;
import com.acme.procurement.v1.NormalizePurchaseRequisitionResponse;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionLine;
import com.acme.procurement.v1.PurchaseRequisitionLineTax;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about expense reports, for screens and reports. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class ExpenseReportQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(ExpenseReportQueryApplication.class);

    private final RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking;

    private final ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking;

    private final DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking;

    private final DocumentValidationServiceGrpc.DocumentValidationServiceFutureStub documentsDocumentValidationServiceFuture;

    private final ExpensePolicyCalculationServiceGrpc.ExpensePolicyCalculationServiceBlockingStub expensesExpensePolicyCalculationServiceBlocking;

    private final ExpenseReportValidationServiceGrpc.ExpenseReportValidationServiceBlockingStub expensesExpenseReportValidationServiceBlocking;

    private final MileageClaimCalculationServiceGrpc.MileageClaimCalculationServiceBlockingStub expensesMileageClaimCalculationServiceBlocking;

    private final PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking;

    private final ExpensesRepository repository;

    public ExpenseReportQueryApplication(
            RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking,
            ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking,
            DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking,
            DocumentValidationServiceGrpc.DocumentValidationServiceFutureStub documentsDocumentValidationServiceFuture,
            ExpensePolicyCalculationServiceGrpc.ExpensePolicyCalculationServiceBlockingStub expensesExpensePolicyCalculationServiceBlocking,
            ExpenseReportValidationServiceGrpc.ExpenseReportValidationServiceBlockingStub expensesExpenseReportValidationServiceBlocking,
            MileageClaimCalculationServiceGrpc.MileageClaimCalculationServiceBlockingStub expensesMileageClaimCalculationServiceBlocking,
            PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking,
            ExpensesRepository repository) {
        this.auditRetentionPolicyExportServiceBlocking = auditRetentionPolicyExportServiceBlocking;
        this.complianceComplianceCaseValidationServiceBlocking = complianceComplianceCaseValidationServiceBlocking;
        this.documentsDocumentValidationServiceBlocking = documentsDocumentValidationServiceBlocking;
        this.documentsDocumentValidationServiceFuture = documentsDocumentValidationServiceFuture;
        this.expensesExpensePolicyCalculationServiceBlocking = expensesExpensePolicyCalculationServiceBlocking;
        this.expensesExpenseReportValidationServiceBlocking = expensesExpenseReportValidationServiceBlocking;
        this.expensesMileageClaimCalculationServiceBlocking = expensesMileageClaimCalculationServiceBlocking;
        this.procurementPurchaseRequisitionValidationServiceBlocking = procurementPurchaseRequisitionValidationServiceBlocking;
        this.repository = repository;
    }

    /**
     * Returns an expense report.
     */
    public LookupExpenseReportResponse lookupExpenseReport(LookupExpenseReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupExpenseReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpenseReport", request.getExpenseReportId(), "LookupExpenseReport");
        return LookupExpenseReportResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setExpenseReportId(request.getExpenseReportId())
                        .build())
                .build();
    }

    /**
     * Lists expense reports a page at a time.
     */
    public BrowseExpenseReportsResponse browseExpenseReports(BrowseExpenseReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseExpenseReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpenseReport", request.getContext().getTraceId(), "BrowseExpenseReports");

        var outbound0 = LintComplianceCasesRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage0Adapter {
            LintComplianceCasesResponse fetch(LintComplianceCasesRequest input) {
                return complianceComplianceCaseValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintComplianceCases(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseExpenseReports", request.getPage().getPageToken());
        return BrowseExpenseReportsResponse.newBuilder()
                .addExpenseReports(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getComplianceCase().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists expense reports a page at a time.
     */
    public FindExpenseReportsResponse findExpenseReports(FindExpenseReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindExpenseReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpenseReport", request.getContext().getTraceId(), "FindExpenseReports");

        var outbound0 = PreviewDocumentRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewDocumentRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setDocument(Document.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setIssuedAt(request.getFilter().getCreatedAfter())
                        .build())
                .build();
        var pending0 = documentsDocumentValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).previewDocument(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindExpenseReports", request.getPage().getPageToken());
        return FindExpenseReportsResponse.newBuilder()
                .addExpenseReports(ExpenseReport.newBuilder()
                        .putAllMetadata(reply0.getDocument().getMetadataMap())
                        .build())
                .build();
    }

    /**
     * Counts expense reports by status.
     */
    public CountExpenseReportsResponse countExpenseReports(CountExpenseReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountExpenseReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpenseReport", request.getContext().getTraceId(), "CountExpenseReports");

        var outbound0 = RpcSupport.unpack(Any.pack(ExplainMileageClaimCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainMileageClaimCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setMileageClaim(MileageClaim.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build()), ExplainMileageClaimCalculationRequest.class);
        var reply0 = expensesMileageClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainMileageClaimCalculation(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountExpenseReports", request.getPage().getPageToken());
        return CountExpenseReportsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns several expense reports at once.
     */
    public ResolveExpenseReportsResponse resolveExpenseReports(ResolveExpenseReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveExpenseReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpenseReport", request.getContext().getTraceId(), "ResolveExpenseReports");

        var outbound0 = ShareRetentionPolicyExportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareRetentionPolicyExportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = auditRetentionPolicyExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareRetentionPolicyExport(outbound0);
        return ResolveExpenseReportsResponse.newBuilder()
                .addExpenseReports(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getRetentionPolicy().getAudit().getEtag())
                                .build())
                        .build())
                .build();
    }

    /**
     * Streams every matching expense report.
     */
    public ExportExpenseReportsResponse exportExpenseReports(ExportExpenseReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportExpenseReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpenseReport", request.getContext().getTraceId(), "ExportExpenseReports");

        var outbound0 = ValidateDocumentRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateDocumentRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setDocument(Document.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> documentsDocumentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateDocument(value));

        var outbound1 = EstimateExpensePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateExpensePolicyRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(reply0.getDocument().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        Function<EstimateExpensePolicyRequest, EstimateExpensePolicyResponse> operation1 = expensesExpensePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::estimateExpensePolicy;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = (NormalizePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .addLines(PurchaseRequisitionLine.newBuilder()
                                .setTax(PurchaseRequisitionLineTax.newBuilder()
                                        .setAmount(reply1.getExpensePolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = procurementPurchaseRequisitionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizePurchaseRequisition(outbound2);

        var outbound3 = ValidateExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getDocument().getBillingAddress().getRegion())
                                .build())
                        .setCard(ExpenseReportCardSettlement.newBuilder()
                                .setNetwork(switch (reply0.getDocument().getSettlementCase()) {
                                    case CARD -> reply0.getDocument().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .addLines(ExpenseReportLine.newBuilder()
                                .setTax(ExpenseReportLineTax.newBuilder()
                                        .setAmount(reply1.getExpensePolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply3 = expensesExpenseReportValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateExpenseReport(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportExpenseReports", request.getFilter().getQuery());
        return ExportExpenseReportsResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setBillingAddress(reply0.getDocument().getBillingAddress())
                        .addLines(ExpenseReportLine.newBuilder()
                                .setTax(ExpenseReportLineTax.newBuilder()
                                        .setAmount(reply1.getExpensePolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
