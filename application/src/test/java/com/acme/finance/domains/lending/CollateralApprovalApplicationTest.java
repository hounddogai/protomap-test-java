package com.acme.finance.domains.lending;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailLine;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.NormalizeAuditTrailRequest;
import com.acme.audit.v1.NormalizeAuditTrailResponse;
import com.acme.audit.v1.PreviewAuditTrailRequest;
import com.acme.audit.v1.PreviewAuditTrailResponse;
import com.acme.banking.v1.PauseStandingOrderWorkflowRequest;
import com.acme.banking.v1.PauseStandingOrderWorkflowResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderWorkflowServiceGrpc;
import com.acme.benefits.v1.BenefitPlanQueryServiceGrpc;
import com.acme.benefits.v1.SuggestBenefitPlansRequest;
import com.acme.benefits.v1.SuggestBenefitPlansResponse;
import com.acme.collections.v1.ListPendingPaymentPromisesRequest;
import com.acme.collections.v1.ListPendingPaymentPromisesResponse;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CountCustomersRequest;
import com.acme.customers.v1.CountCustomersResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.EstimateCustomerRequest;
import com.acme.customers.v1.EstimateCustomerResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ApproveCoverageRequest;
import com.acme.insurance.v1.ApproveCoverageResponse;
import com.acme.insurance.v1.CoverageApprovalServiceGrpc;
import com.acme.insurance.v1.EscalatePolicyRequest;
import com.acme.insurance.v1.EscalatePolicyResponse;
import com.acme.insurance.v1.ListPendingCoveragesRequest;
import com.acme.insurance.v1.ListPendingCoveragesResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.RejectCoverageRequest;
import com.acme.insurance.v1.RejectCoverageResponse;
import com.acme.labeling.v1.ExplainLabelingTaskCalculationRequest;
import com.acme.labeling.v1.ExplainLabelingTaskCalculationResponse;
import com.acme.labeling.v1.LabelingTask;
import com.acme.labeling.v1.LabelingTaskCalculationServiceGrpc;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.StreamJournalEntryNoticesRequest;
import com.acme.ledger.v1.StreamJournalEntryNoticesResponse;
import com.acme.lending.v1.ApproveCollateralRequest;
import com.acme.lending.v1.ApproveCollateralResponse;
import com.acme.lending.v1.CancelLoanApplicationWorkflowRequest;
import com.acme.lending.v1.CancelLoanApplicationWorkflowResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralComponent;
import com.acme.lending.v1.CollateralLifecycleServiceGrpc;
import com.acme.lending.v1.CommentCollateralApprovalRequest;
import com.acme.lending.v1.CommentCollateralApprovalResponse;
import com.acme.lending.v1.CountRepaymentTotalsRequest;
import com.acme.lending.v1.CountRepaymentTotalsResponse;
import com.acme.lending.v1.DelegateCollateralApprovalRequest;
import com.acme.lending.v1.DelegateCollateralApprovalResponse;
import com.acme.lending.v1.DelegateLoanApplicationApprovalRequest;
import com.acme.lending.v1.DelegateLoanApplicationApprovalResponse;
import com.acme.lending.v1.EscalateCollateralRequest;
import com.acme.lending.v1.EscalateCollateralResponse;
import com.acme.lending.v1.GetCollateralApprovalStatusRequest;
import com.acme.lending.v1.GetCollateralApprovalStatusResponse;
import com.acme.lending.v1.ListPendingCollateralsRequest;
import com.acme.lending.v1.ListPendingCollateralsResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationApprovalServiceGrpc;
import com.acme.lending.v1.LoanApplicationLine;
import com.acme.lending.v1.LoanApplicationLineTax;
import com.acme.lending.v1.LoanApplicationWorkflowServiceGrpc;
import com.acme.lending.v1.LoanBankFunding;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.NormalizeRepaymentRequest;
import com.acme.lending.v1.NormalizeRepaymentResponse;
import com.acme.lending.v1.RecallCollateralRequest;
import com.acme.lending.v1.RecallCollateralResponse;
import com.acme.lending.v1.RejectCollateralRequest;
import com.acme.lending.v1.RejectCollateralResponse;
import com.acme.lending.v1.RenewCollateralRequest;
import com.acme.lending.v1.RenewCollateralResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.lending.v1.RepaymentValidationServiceGrpc;
import com.acme.lending.v1.SimulateLoanRequest;
import com.acme.lending.v1.SimulateLoanResponse;
import com.acme.lending.v1.SubmitCollateralRequest;
import com.acme.lending.v1.SubmitCollateralResponse;
import com.acme.search.v1.BrowseSearchIndexesRequest;
import com.acme.search.v1.BrowseSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.google.protobuf.Any;
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

class CollateralApprovalApplicationTest {
    @Test
    void submitCollateralPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LendingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollateral(String tenant, Collateral value) {}
            public void saveLoan(String tenant, Loan value) {}
            public void saveLoanApplication(String tenant, LoanApplication value) {}
            public void saveRepayment(String tenant, Repayment value) {}
        };
        var application = new CollateralApprovalApplication(
                AuditTrailValidationServiceGrpc.newBlockingStub(channel),
                StandingOrderWorkflowServiceGrpc.newBlockingStub(channel),
                BenefitPlanQueryServiceGrpc.newBlockingStub(channel),
                PaymentPromiseApprovalServiceGrpc.newBlockingStub(channel),
                CustomerCalculationServiceGrpc.newBlockingStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                CoverageApprovalServiceGrpc.newBlockingStub(channel),
                PolicyApprovalServiceGrpc.newBlockingStub(channel),
                LabelingTaskCalculationServiceGrpc.newBlockingStub(channel),
                JournalEntryNotificationServiceGrpc.newBlockingStub(channel),
                CollateralLifecycleServiceGrpc.newBlockingStub(channel),
                LoanApplicationApprovalServiceGrpc.newBlockingStub(channel),
                LoanApplicationWorkflowServiceGrpc.newBlockingStub(channel),
                LoanCalculationServiceGrpc.newBlockingStub(channel),
                RepaymentReportingServiceGrpc.newBlockingStub(channel),
                RepaymentValidationServiceGrpc.newBlockingStub(channel),
                new CollateralApprovalRequestMapperImpl(),
                repository,
                Runnable::run,
                SearchIndexQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SubmitCollateralRequest.getDefaultInstance());
        var response = application.submitCollateral(request);
        var sent0 = channel.request("acme.insurance.v1.CoverageApprovalService/RejectCoverage");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent0, "details", "approver", "email"));
        var sent1 = channel.request("acme.lending.v1.RepaymentReportingService/CountRepaymentTotals");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collateral", "address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        var sent2 = channel.request("acme.lending.v1.LoanApplicationWorkflowService/CancelLoanApplicationWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageApprovalService/RejectCoverage"), "coverage", "discount", "amount"),
                ProbeSupport.value(sent2, "loan_application", "lines", "tax", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collateral", "address", "lines"),
                ProbeSupport.value(sent2, "loan_application", "billing_address", "lines"));
        var sent3 = channel.request("acme.lending.v1.CollateralLifecycleService/RenewCollateral");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collateral", "collateral_id"),
                ProbeSupport.value(sent3, "collateral_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationWorkflowService/CancelLoanApplicationWorkflow"), "loan_application", "counterparty", "email"),
                ProbeSupport.value(sent3, "details", "notify_contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageApprovalService/RejectCoverage"), "coverage", "owner", "display_name"),
                ProbeSupport.value(sent3, "details", "new_owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent3, "details", "new_owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(sent3, "details", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageApprovalService/RejectCoverage"), "coverage", "owner", "user_id"),
                ProbeSupport.value(response, "collateral", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentReportingService/CountRepaymentTotals"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentReportingService/CountRepaymentTotals"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationWorkflowService/CancelLoanApplicationWorkflow"), "loan_application", "audit", "created_at"),
                ProbeSupport.value(response, "collateral", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationWorkflowService/CancelLoanApplicationWorkflow"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralLifecycleService/RenewCollateral"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collateral_id"),
                ProbeSupport.value(response, "collateral", "collateral_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collateral", "name"),
                ProbeSupport.value(response, "collateral", "name"));
    }
}
