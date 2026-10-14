package com.acme.finance.domains.lending;

import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BankAccountWorkflowServiceGrpc;
import com.acme.banking.v1.StartBankAccountWorkflowRequest;
import com.acme.banking.v1.StartBankAccountWorkflowResponse;
import com.acme.banking.v1.StartStatementWorkflowRequest;
import com.acme.banking.v1.StartStatementWorkflowResponse;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.StatementWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.community.v1.CloneForumRequest;
import com.acme.community.v1.CloneForumResponse;
import com.acme.community.v1.Forum;
import com.acme.community.v1.ForumServiceGrpc;
import com.acme.concessions.v1.ConcessionStandServiceGrpc;
import com.acme.concessions.v1.SearchConcessionStandsRequest;
import com.acme.concessions.v1.SearchConcessionStandsResponse;
import com.acme.customers.v1.CheckHouseholdEligibilityRequest;
import com.acme.customers.v1.CheckHouseholdEligibilityResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.EstimateCustomerRequest;
import com.acme.customers.v1.EstimateCustomerResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.QuoteCustomerSegmentRequest;
import com.acme.customers.v1.QuoteCustomerSegmentResponse;
import com.acme.customers.v1.ResolveHouseholdsRequest;
import com.acme.customers.v1.ResolveHouseholdsResponse;
import com.acme.customers.v1.SuggestHouseholdsRequest;
import com.acme.customers.v1.SuggestHouseholdsResponse;
import com.acme.customers.v1.ValidateCustomerRequest;
import com.acme.customers.v1.ValidateCustomerResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageLifecycleServiceGrpc;
import com.acme.insurance.v1.ScheduleCoverageExpiryRequest;
import com.acme.insurance.v1.ScheduleCoverageExpiryResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.ledger.v1.RevertJournalEntryRequest;
import com.acme.ledger.v1.RevertJournalEntryResponse;
import com.acme.lending.v1.ApproveCollateralRequest;
import com.acme.lending.v1.ApproveCollateralResponse;
import com.acme.lending.v1.ApproveRepaymentRequest;
import com.acme.lending.v1.ApproveRepaymentResponse;
import com.acme.lending.v1.CancelLoanWorkflowRequest;
import com.acme.lending.v1.CancelLoanWorkflowResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralApprovalServiceGrpc;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.CommentRepaymentApprovalRequest;
import com.acme.lending.v1.CommentRepaymentApprovalResponse;
import com.acme.lending.v1.EscalateRepaymentRequest;
import com.acme.lending.v1.EscalateRepaymentResponse;
import com.acme.lending.v1.GetRepaymentApprovalStatusRequest;
import com.acme.lending.v1.GetRepaymentApprovalStatusResponse;
import com.acme.lending.v1.ListPendingRepaymentsRequest;
import com.acme.lending.v1.ListPendingRepaymentsResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationApprovalServiceGrpc;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanApplicationWorkflowServiceGrpc;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.lending.v1.LoanWorkflowServiceGrpc;
import com.acme.lending.v1.PauseLoanWorkflowRequest;
import com.acme.lending.v1.PauseLoanWorkflowResponse;
import com.acme.lending.v1.PreviewCollateralRequest;
import com.acme.lending.v1.PreviewCollateralResponse;
import com.acme.lending.v1.RecallLoanApplicationRequest;
import com.acme.lending.v1.RecallLoanApplicationResponse;
import com.acme.lending.v1.RecallRepaymentRequest;
import com.acme.lending.v1.RecallRepaymentResponse;
import com.acme.lending.v1.RejectLoanRequest;
import com.acme.lending.v1.RejectLoanResponse;
import com.acme.lending.v1.RejectRepaymentRequest;
import com.acme.lending.v1.RejectRepaymentResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.StartLoanApplicationWorkflowRequest;
import com.acme.lending.v1.StartLoanApplicationWorkflowResponse;
import com.acme.lending.v1.SubmitRepaymentRequest;
import com.acme.lending.v1.SubmitRepaymentResponse;
import com.acme.licensing.v1.EstimateSoftwareLicenseRequest;
import com.acme.licensing.v1.EstimateSoftwareLicenseResponse;
import com.acme.licensing.v1.SoftwareLicense;
import com.acme.licensing.v1.SoftwareLicenseCalculationServiceGrpc;
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

class RepaymentApprovalApplicationTest {
    @Test
    void submitRepaymentPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LendingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollateral(String tenant, Collateral value) {}
            public void saveLoan(String tenant, Loan value) {}
            public void saveLoanApplication(String tenant, LoanApplication value) {}
            public void saveRepayment(String tenant, Repayment value) {}
        };
        var application = new RepaymentApprovalApplication(
                BankAccountWorkflowServiceGrpc.newBlockingStub(channel),
                StatementWorkflowServiceGrpc.newBlockingStub(channel),
                ForumServiceGrpc.newBlockingStub(channel),
                ConcessionStandServiceGrpc.newBlockingStub(channel),
                CustomerCalculationServiceGrpc.newBlockingStub(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                CustomerValidationServiceGrpc.newBlockingStub(channel),
                HouseholdQueryServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                CoverageLifecycleServiceGrpc.newBlockingStub(channel),
                JournalEntryHistoryServiceGrpc.newBlockingStub(channel),
                CollateralApprovalServiceGrpc.newBlockingStub(channel),
                CollateralValidationServiceGrpc.newBlockingStub(channel),
                LoanApplicationApprovalServiceGrpc.newBlockingStub(channel),
                LoanApplicationWorkflowServiceGrpc.newBlockingStub(channel),
                LoanApprovalServiceGrpc.newBlockingStub(channel),
                LoanWorkflowServiceGrpc.newBlockingStub(channel),
                SoftwareLicenseCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(SubmitRepaymentRequest.getDefaultInstance());
        var response = application.submitRepayment(request);
        var sent0 = channel.request("acme.banking.v1.BankAccountWorkflowService/StartBankAccountWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent0, "bank_account", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "display_name"),
                ProbeSupport.value(sent0, "bank_account", "holder", "display_name"));
        var sent1 = channel.request("acme.lending.v1.CollateralValidationService/PreviewCollateral");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.BankAccountWorkflowService/StartBankAccountWorkflow"), "bank_account", "contact", "preferred_channel"),
                ProbeSupport.value(sent1, "details", "contact", "preferred_channel"));
        var sent2 = channel.request("acme.customers.v1.CustomerCalculationService/EstimateCustomer");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "repayment", "participants", "contact", "name"),
                ProbeSupport.value(sent2, "customer", "contact", "name"));
        var sent3 = channel.request("acme.banking.v1.StatementWorkflowService/StartStatementWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/PreviewCollateral"), "collateral", "audit", "created_at"),
                ProbeSupport.value(sent3, "statement", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerCalculationService/EstimateCustomer"), "customer", "contact", "preferred_channel"),
                ProbeSupport.value(sent3, "details", "notify", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerCalculationService/EstimateCustomer"), "customer", "contact", "phone"),
                ProbeSupport.value(sent3, "details", "notify", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.BankAccountWorkflowService/StartBankAccountWorkflow"), "bank_account", "contact", "email"),
                ProbeSupport.value(response, "repayment", "participants", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/PreviewCollateral"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StatementWorkflowService/StartStatementWorkflow"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StatementWorkflowService/StartStatementWorkflow"), "outcome", "next_steps"),
                ProbeSupport.value(response, "outcome", "next_steps"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "repayment_id"),
                ProbeSupport.value(response, "repayment", "repayment_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "repayment", "status"),
                ProbeSupport.value(response, "repayment", "status"));
    }
}
