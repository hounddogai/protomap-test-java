package com.acme.finance.domains.lending;

import com.acme.banking.v1.ApproveMandateRequest;
import com.acme.banking.v1.ApproveMandateResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.banking.v1.MandateCardSettlement;
import com.acme.banking.v1.SubmitMandateRequest;
import com.acme.banking.v1.SubmitMandateResponse;
import com.acme.collections.v1.CommentPaymentPromiseApprovalRequest;
import com.acme.collections.v1.CommentPaymentPromiseApprovalResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.BrowseCustomersRequest;
import com.acme.customers.v1.BrowseCustomersResponse;
import com.acme.customers.v1.CalculateHouseholdRequest;
import com.acme.customers.v1.CalculateHouseholdResponse;
import com.acme.customers.v1.CheckHouseholdEligibilityRequest;
import com.acme.customers.v1.CheckHouseholdEligibilityResponse;
import com.acme.customers.v1.CreateCustomerRequest;
import com.acme.customers.v1.CreateCustomerResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegmentDiscountAction;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.NormalizeHouseholdRequest;
import com.acme.customers.v1.NormalizeHouseholdResponse;
import com.acme.customers.v1.RecalculateHouseholdsRequest;
import com.acme.customers.v1.RecalculateHouseholdsResponse;
import com.acme.customers.v1.SimulateCustomerSegmentRequest;
import com.acme.customers.v1.SimulateCustomerSegmentResponse;
import com.acme.customers.v1.SimulateHouseholdRequest;
import com.acme.customers.v1.SimulateHouseholdResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.CommentClaimApprovalRequest;
import com.acme.insurance.v1.CommentClaimApprovalResponse;
import com.acme.lending.v1.ApproveLoanRequest;
import com.acme.lending.v1.ApproveLoanResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CommentLoanApprovalRequest;
import com.acme.lending.v1.CommentLoanApprovalResponse;
import com.acme.lending.v1.CountLoansRequest;
import com.acme.lending.v1.CountLoansResponse;
import com.acme.lending.v1.DelegateLoanApplicationApprovalRequest;
import com.acme.lending.v1.DelegateLoanApplicationApprovalResponse;
import com.acme.lending.v1.EscalateLoanRequest;
import com.acme.lending.v1.EscalateLoanResponse;
import com.acme.lending.v1.GetLoanApprovalStatusRequest;
import com.acme.lending.v1.GetLoanApprovalStatusResponse;
import com.acme.lending.v1.LintRepaymentsRequest;
import com.acme.lending.v1.LintRepaymentsResponse;
import com.acme.lending.v1.ListPendingLoanApplicationsRequest;
import com.acme.lending.v1.ListPendingLoanApplicationsResponse;
import com.acme.lending.v1.ListPendingLoansRequest;
import com.acme.lending.v1.ListPendingLoansResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationApprovalServiceGrpc;
import com.acme.lending.v1.LoanApplicationCardSettlement;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanLifecycleServiceGrpc;
import com.acme.lending.v1.LoanQueryServiceGrpc;
import com.acme.lending.v1.RecallLoanRequest;
import com.acme.lending.v1.RecallLoanResponse;
import com.acme.lending.v1.ReinstateLoanRequest;
import com.acme.lending.v1.ReinstateLoanResponse;
import com.acme.lending.v1.RejectLoanRequest;
import com.acme.lending.v1.RejectLoanResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentValidationServiceGrpc;
import com.acme.lending.v1.SubmitLoanRequest;
import com.acme.lending.v1.SubmitLoanResponse;
import com.acme.messaging.v1.Conversation;
import com.acme.messaging.v1.ConversationCalculationServiceGrpc;
import com.acme.messaging.v1.SimulateConversationRequest;
import com.acme.messaging.v1.SimulateConversationResponse;
import com.google.protobuf.Any;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class LoanApprovalApplicationTest {
    @Test
    void commentLoanApprovalPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LendingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollateral(String tenant, Collateral value) {}
            public void saveLoan(String tenant, Loan value) {}
            public void saveLoanApplication(String tenant, LoanApplication value) {}
            public void saveRepayment(String tenant, Repayment value) {}
        };
        var application = new LoanApprovalApplication(
                MandateApprovalServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                PaymentPromiseApprovalServiceGrpc.newBlockingStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                ClaimApprovalServiceGrpc.newBlockingStub(channel),
                LoanApplicationApprovalServiceGrpc.newBlockingStub(channel),
                LoanLifecycleServiceGrpc.newBlockingStub(channel),
                LoanQueryServiceGrpc.newBlockingStub(channel),
                RepaymentValidationServiceGrpc.newBlockingStub(channel),
                new LoanApprovalRequestMapperImpl(),
                ConversationCalculationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(CommentLoanApprovalRequest.getDefaultInstance());
        var response = application.commentLoanApproval(request);
        var sent0 = channel.request("acme.messaging.v1.ConversationCalculationService/SimulateConversation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "audit", "created_at"),
                ProbeSupport.value(sent0, "conversation", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "user_id"),
                ProbeSupport.value(sent0, "conversation", "assignee", "user_id"));
        var sent1 = channel.request("acme.customers.v1.HouseholdValidationService/NormalizeHousehold");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "holder", "display_name"),
                ProbeSupport.value(sent1, "household", "children", "children", "manager", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.messaging.v1.ConversationCalculationService/SimulateConversation"), "conversation", "audit", "created_at"),
                ProbeSupport.value(sent1, "household", "children", "children", "audit", "created_at"));
        var sent2 = channel.request("acme.customers.v1.HouseholdCalculationService/CalculateHousehold");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdValidationService/NormalizeHousehold"), "household", "manager", "email"),
                ProbeSupport.value(sent2, "household", "children", "children", "manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "audit", "etag"),
                ProbeSupport.value(sent2, "household", "children", "children", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.messaging.v1.ConversationCalculationService/SimulateConversation"), "conversation", "assignee", "display_name"),
                ProbeSupport.value(sent2, "household", "children", "children", "manager", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.messaging.v1.ConversationCalculationService/SimulateConversation"), "conversation", "audit", "created_by"),
                ProbeSupport.value(sent2, "household", "children", "children", "audit", "created_by"));
        var sent3 = channel.request("acme.banking.v1.MandateApprovalService/SubmitMandate");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.messaging.v1.ConversationCalculationService/SimulateConversation"), "conversation", "audit", "created_by"),
                ProbeSupport.value(sent3, "mandate", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdCalculationService/CalculateHousehold"), "household", "manager", "user_id"),
                ProbeSupport.value(sent3, "details", "approver", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdCalculationService/CalculateHousehold"), "household", "manager", "display_name"),
                ProbeSupport.value(sent3, "mandate", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "card", "last4"),
                ProbeSupport.value(sent3, "mandate", "card", "last4"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.messaging.v1.ConversationCalculationService/SimulateConversation"), "conversation", "assignee", "display_name"),
                ProbeSupport.value(response, "loan", "holder", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.messaging.v1.ConversationCalculationService/SimulateConversation"), "conversation", "requester", "preferred_channel"),
                ProbeSupport.value(response, "loan", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdValidationService/NormalizeHousehold"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdCalculationService/CalculateHousehold"), "household", "audit", "etag"),
                ProbeSupport.value(response, "loan", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateApprovalService/SubmitMandate"), "mandate", "audit", "created_at"),
                ProbeSupport.value(response, "loan", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateApprovalService/SubmitMandate"), "outcome", "next_steps"),
                ProbeSupport.value(response, "outcome", "next_steps"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_id"),
                ProbeSupport.value(response, "loan", "loan_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "labels"),
                ProbeSupport.value(response, "loan", "labels"));
    }
}
