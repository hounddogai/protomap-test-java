package com.acme.finance.domains.expenses;

import com.acme.assets.v1.DepreciationQueryServiceGrpc;
import com.acme.assets.v1.SummarizeDepreciationsRequest;
import com.acme.assets.v1.SummarizeDepreciationsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.CheckRenewalEligibilityRequest;
import com.acme.contracts.v1.CheckRenewalEligibilityResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.documents.v1.CreateDocumentRequest;
import com.acme.documents.v1.CreateDocumentResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentLine;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.documents.v1.FindDocumentsRequest;
import com.acme.documents.v1.FindDocumentsResponse;
import com.acme.expenses.v1.CheckExpensePolicyEligibilityRequest;
import com.acme.expenses.v1.CheckExpensePolicyEligibilityResponse;
import com.acme.expenses.v1.CheckReceiptEligibilityRequest;
import com.acme.expenses.v1.CheckReceiptEligibilityResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportCardSettlement;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExpenseReportLineTax;
import com.acme.expenses.v1.ExpenseReportValidationServiceGrpc;
import com.acme.expenses.v1.ExplainExpensePolicyDecisionRequest;
import com.acme.expenses.v1.ExplainExpensePolicyDecisionResponse;
import com.acme.expenses.v1.GetExpensePolicyRulesRequest;
import com.acme.expenses.v1.GetExpensePolicyRulesResponse;
import com.acme.expenses.v1.LintExpensePoliciesRequest;
import com.acme.expenses.v1.LintExpensePoliciesResponse;
import com.acme.expenses.v1.LintExpenseReportsRequest;
import com.acme.expenses.v1.LintExpenseReportsResponse;
import com.acme.expenses.v1.LintMileageClaimsRequest;
import com.acme.expenses.v1.LintMileageClaimsResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimLine;
import com.acme.expenses.v1.MileageClaimLineTax;
import com.acme.expenses.v1.MileageClaimRef;
import com.acme.expenses.v1.MileageClaimValidationServiceGrpc;
import com.acme.expenses.v1.NormalizeExpensePolicyRequest;
import com.acme.expenses.v1.NormalizeExpensePolicyResponse;
import com.acme.expenses.v1.NormalizeReceiptRequest;
import com.acme.expenses.v1.NormalizeReceiptResponse;
import com.acme.expenses.v1.PreviewExpensePolicyRequest;
import com.acme.expenses.v1.PreviewExpensePolicyResponse;
import com.acme.expenses.v1.PreviewExpenseReportRequest;
import com.acme.expenses.v1.PreviewExpenseReportResponse;
import com.acme.expenses.v1.RecalculateReceiptsRequest;
import com.acme.expenses.v1.RecalculateReceiptsResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptBankSettlement;
import com.acme.expenses.v1.ReceiptCalculationServiceGrpc;
import com.acme.expenses.v1.ReceiptLine;
import com.acme.expenses.v1.ReceiptLineTax;
import com.acme.expenses.v1.ReceiptValidationServiceGrpc;
import com.acme.expenses.v1.ScoreExpensePolicyRequest;
import com.acme.expenses.v1.ScoreExpensePolicyResponse;
import com.acme.expenses.v1.ScoreMileageClaimRequest;
import com.acme.expenses.v1.ScoreMileageClaimResponse;
import com.acme.expenses.v1.SimulateReceiptRequest;
import com.acme.expenses.v1.SimulateReceiptResponse;
import com.acme.expenses.v1.ValidateExpensePolicyRequest;
import com.acme.expenses.v1.ValidateExpensePolicyResponse;
import com.acme.expenses.v1.ValidateExpenseReportRequest;
import com.acme.expenses.v1.ValidateExpenseReportResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.BatchGetUsersRequest;
import com.acme.identity.v1.BatchGetUsersResponse;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchCardSettlement;
import com.acme.ledger.v1.PostingBatchHistoryServiceGrpc;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.PreviewPostingBatchRequest;
import com.acme.ledger.v1.PreviewPostingBatchResponse;
import com.acme.ledger.v1.RevertPostingBatchRequest;
import com.acme.ledger.v1.RevertPostingBatchResponse;
import com.acme.procurement.v1.CompareRfqOptionsRequest;
import com.acme.procurement.v1.CompareRfqOptionsResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.google.protobuf.Any;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class ExpensePolicyValidationApplicationTest {
    @Test
    void previewExpensePolicyPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var application = new ExpensePolicyValidationApplication(
                DepreciationQueryServiceGrpc.newBlockingStub(channel),
                RenewalValidationServiceGrpc.newBlockingStub(channel),
                DocumentQueryServiceGrpc.newBlockingStub(channel),
                DocumentServiceGrpc.newFutureStub(channel),
                ExpenseReportValidationServiceGrpc.newBlockingStub(channel),
                MileageClaimValidationServiceGrpc.newBlockingStub(channel),
                ReceiptCalculationServiceGrpc.newBlockingStub(channel),
                ReceiptValidationServiceGrpc.newBlockingStub(channel),
                UserServiceGrpc.newBlockingStub(channel),
                PostingBatchHistoryServiceGrpc.newBlockingStub(channel),
                PostingBatchValidationServiceGrpc.newBlockingStub(channel),
                RfqCalculationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(PreviewExpensePolicyRequest.getDefaultInstance());
        var response = application.previewExpensePolicy(request);
        var sent0 = channel.request("acme.expenses.v1.MileageClaimValidationService/ScoreMileageClaim");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy", "audit", "updated_by"),
                ProbeSupport.value(sent0, "mileage_claim", "audit", "updated_by"));
        var sent1 = channel.request("acme.procurement.v1.RfqCalculationService/CompareRfqOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "phone"),
                ProbeSupport.value(sent1, "rfq", "requester", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.MileageClaimValidationService/ScoreMileageClaim"), "mileage_claim", "billing_address", "region"),
                ProbeSupport.value(sent1, "details", "destination", "region"));
        var sent2 = channel.request("acme.expenses.v1.ReceiptCalculationService/RecalculateReceipts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.MileageClaimValidationService/ScoreMileageClaim"), "mileage_claim", "audit", "created_at"),
                ProbeSupport.value(sent2, "receipt", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy", "owner", "display_name"),
                ProbeSupport.value(sent2, "receipt", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqCalculationService/CompareRfqOptions"), "rfq", "audit", "etag"),
                ProbeSupport.value(sent2, "receipt", "audit", "etag"));
        var sent3 = channel.request("acme.ledger.v1.PostingBatchHistoryService/RevertPostingBatch");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.MileageClaimValidationService/ScoreMileageClaim"), "mileage_claim", "audit", "updated_by"),
                ProbeSupport.value(sent3, "posting_batch", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.MileageClaimValidationService/ScoreMileageClaim"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.MileageClaimValidationService/ScoreMileageClaim"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqCalculationService/CompareRfqOptions"), "rfq", "audit", "created_at"),
                ProbeSupport.value(response, "expense_policy", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ReceiptCalculationService/RecalculateReceipts"), "receipt", "counterparty", "display_name"),
                ProbeSupport.value(response, "expense_policy", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchHistoryService/RevertPostingBatch"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy_id"),
                ProbeSupport.value(response, "expense_policy", "expense_policy_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy", "description"),
                ProbeSupport.value(response, "expense_policy", "description"));
    }
}
