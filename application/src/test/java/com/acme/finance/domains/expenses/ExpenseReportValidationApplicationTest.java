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
import com.acme.expenses.v1.ExpensePolicy;
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
import com.acme.expenses.v1.MileageClaim;
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
import com.acme.finance.testing.ProbeSupport;
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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class ExpenseReportValidationApplicationTest {
    @Test
    void validateExpenseReportPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var application = new ExpenseReportValidationApplication(
                ForecastValidationServiceGrpc.newBlockingStub(channel),
                VarianceValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ComplianceCaseQueryServiceGrpc.newBlockingStub(channel),
                DocumentQueryServiceGrpc.newBlockingStub(channel),
                ExpenseReportCalculationServiceGrpc.newBlockingStub(channel),
                ReceiptCalculationServiceGrpc.newBlockingStub(channel),
                CoverageValidationServiceGrpc.newBlockingStub(channel),
                SoftwareLicenseCalculationServiceGrpc.newBlockingStub(channel),
                new ExpenseReportValidationRequestMapperImpl(),
                PurchaseOrderCalculationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionValidationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(ValidateExpenseReportRequest.getDefaultInstance());
        var response = application.validateExpenseReport(request);
        var sent0 = channel.request("acme.expenses.v1.ExpenseReportCalculationService/CalculateExpenseReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "attachments", "checksum"),
                ProbeSupport.value(sent0, "expense_report", "attachments", "checksum"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "attachments", "content_type"),
                ProbeSupport.value(sent0, "expense_report", "attachments", "content_type"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "attachments", "file_name"),
                ProbeSupport.value(sent0, "expense_report", "attachments", "file_name"));
        var sent1 = channel.request("acme.procurement.v1.PurchaseOrderCalculationService/EstimatePurchaseOrder");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "attachments", "checksum"),
                ProbeSupport.value(sent1, "purchase_order", "attachments", "checksum"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "attachments", "uri"),
                ProbeSupport.value(sent1, "purchase_order", "attachments", "uri"));
        var sent2 = channel.request("acme.procurement.v1.PurchaseRequisitionValidationService/ValidatePurchaseRequisition");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "attachments", "uri"),
                ProbeSupport.value(sent2, "purchase_requisition", "attachments", "uri"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "attachments", "size_bytes"),
                ProbeSupport.value(sent2, "purchase_requisition", "attachments", "size_bytes"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "attachments", "checksum"),
                ProbeSupport.value(sent2, "purchase_requisition", "attachments", "checksum"));
        var sent3 = channel.request("acme.insurance.v1.CoverageValidationService/CheckCoverageEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "counterparty", "email"),
                ProbeSupport.value(sent3, "coverage", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageValidationService/CheckCoverageEligibility"), "coverage", "discount", "amount"),
                ProbeSupport.value(response, "expense_report", "lines", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report_id"),
                ProbeSupport.value(response, "expense_report", "expense_report_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "issued_at"),
                ProbeSupport.value(response, "expense_report", "issued_at"));
    }
}
