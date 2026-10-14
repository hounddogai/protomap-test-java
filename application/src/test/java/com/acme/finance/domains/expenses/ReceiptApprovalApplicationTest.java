package com.acme.finance.domains.expenses;

import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.CheckAccessPolicyEligibilityRequest;
import com.acme.authorization.v1.CheckAccessPolicyEligibilityResponse;
import com.acme.banking.v1.ResumeStatementWorkflowRequest;
import com.acme.banking.v1.ResumeStatementWorkflowResponse;
import com.acme.banking.v1.StandingOrderRef;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.StatementCardSettlement;
import com.acme.banking.v1.StatementWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CountCustomersRequest;
import com.acme.customers.v1.CountCustomersResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.ExportCustomersRequest;
import com.acme.customers.v1.ExportCustomersResponse;
import com.acme.customers.v1.LintCustomerSegmentsRequest;
import com.acme.customers.v1.LintCustomerSegmentsResponse;
import com.acme.customers.v1.ResolveCustomersRequest;
import com.acme.customers.v1.ResolveCustomersResponse;
import com.acme.expenses.v1.ApproveExpenseReportRequest;
import com.acme.expenses.v1.ApproveExpenseReportResponse;
import com.acme.expenses.v1.ApproveReceiptRequest;
import com.acme.expenses.v1.ApproveReceiptResponse;
import com.acme.expenses.v1.CheckReceiptEligibilityRequest;
import com.acme.expenses.v1.CheckReceiptEligibilityResponse;
import com.acme.expenses.v1.CommentReceiptApprovalRequest;
import com.acme.expenses.v1.CommentReceiptApprovalResponse;
import com.acme.expenses.v1.DelegateReceiptApprovalRequest;
import com.acme.expenses.v1.DelegateReceiptApprovalResponse;
import com.acme.expenses.v1.EscalateReceiptRequest;
import com.acme.expenses.v1.EscalateReceiptResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyLifecycleServiceGrpc;
import com.acme.expenses.v1.ExpensePolicySyncServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportApprovalServiceGrpc;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExpenseReportLineTax;
import com.acme.expenses.v1.ExpenseReportQueryServiceGrpc;
import com.acme.expenses.v1.GetReceiptApprovalStatusRequest;
import com.acme.expenses.v1.GetReceiptApprovalStatusResponse;
import com.acme.expenses.v1.LintReceiptsRequest;
import com.acme.expenses.v1.LintReceiptsResponse;
import com.acme.expenses.v1.ListExpensePolicyConflictsRequest;
import com.acme.expenses.v1.ListExpensePolicyConflictsResponse;
import com.acme.expenses.v1.ListPendingReceiptsRequest;
import com.acme.expenses.v1.ListPendingReceiptsResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimRef;
import com.acme.expenses.v1.PreviewReceiptRequest;
import com.acme.expenses.v1.PreviewReceiptResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptAttachment;
import com.acme.expenses.v1.ReceiptLine;
import com.acme.expenses.v1.ReceiptValidationServiceGrpc;
import com.acme.expenses.v1.ReconcileExpensePoliciesRequest;
import com.acme.expenses.v1.ReconcileExpensePoliciesResponse;
import com.acme.expenses.v1.RejectReceiptRequest;
import com.acme.expenses.v1.RejectReceiptResponse;
import com.acme.expenses.v1.ResolveExpenseReportsRequest;
import com.acme.expenses.v1.ResolveExpenseReportsResponse;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryRequest;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryResponse;
import com.acme.expenses.v1.SubmitReceiptRequest;
import com.acme.expenses.v1.SubmitReceiptResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.keymanagement.v1.BrowseCryptoKeysRequest;
import com.acme.keymanagement.v1.BrowseCryptoKeysResponse;
import com.acme.keymanagement.v1.CryptoKeyQueryServiceGrpc;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.lending.v1.LoanBankFunding;
import com.acme.lending.v1.SubmitLoanRequest;
import com.acme.lending.v1.SubmitLoanResponse;
import com.acme.sponsorships.v1.ListSponsorsRequest;
import com.acme.sponsorships.v1.ListSponsorsResponse;
import com.acme.sponsorships.v1.SponsorServiceGrpc;
import com.google.protobuf.Any;
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

class ReceiptApprovalApplicationTest {
    @Test
    void submitReceiptPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var application = new ReceiptApprovalApplication(
                AccessPolicyValidationServiceGrpc.newBlockingStub(channel),
                StatementWorkflowServiceGrpc.newBlockingStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                CustomerSegmentValidationServiceGrpc.newBlockingStub(channel),
                ExpensePolicyLifecycleServiceGrpc.newBlockingStub(channel),
                ExpensePolicySyncServiceGrpc.newBlockingStub(channel),
                ExpenseReportApprovalServiceGrpc.newBlockingStub(channel),
                ExpenseReportQueryServiceGrpc.newBlockingStub(channel),
                ReceiptValidationServiceGrpc.newBlockingStub(channel),
                CryptoKeyQueryServiceGrpc.newBlockingStub(channel),
                LoanApprovalServiceGrpc.newBlockingStub(channel),
                new ReceiptApprovalRequestMapperImpl(),
                repository,
                Runnable::run,
                SponsorServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SubmitReceiptRequest.getDefaultInstance());
        var response = application.submitReceipt(request);
        var sent0 = channel.request("acme.expenses.v1.ExpenseReportQueryService/ResolveExpenseReports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        var sent1 = channel.request("acme.sponsorships.v1.SponsorService/ListSponsors");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "receipt", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        var sent2 = channel.request("acme.customers.v1.CustomerSegmentValidationService/LintCustomerSegments");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "user_id"),
                ProbeSupport.value(sent2, "customer_segment", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "receipt", "audit", "updated_by"),
                ProbeSupport.value(sent2, "customer_segment", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent2, "customer_segment", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "receipt", "audit", "etag"),
                ProbeSupport.value(sent2, "customer_segment", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportQueryService/ResolveExpenseReports"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportQueryService/ResolveExpenseReports"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.sponsorships.v1.SponsorService/ListSponsors"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerSegmentValidationService/LintCustomerSegments"), "customer_segment", "audit", "updated_by"),
                ProbeSupport.value(response, "receipt", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "receipt_id"),
                ProbeSupport.value(response, "receipt", "receipt_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "receipt", "notes"),
                ProbeSupport.value(response, "receipt", "notes"));
    }
}
