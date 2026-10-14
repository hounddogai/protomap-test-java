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
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.CommentClaimApprovalRequest;
import com.acme.insurance.v1.CommentClaimApprovalResponse;
import com.acme.lending.v1.ApproveLoanRequest;
import com.acme.lending.v1.ApproveLoanResponse;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class LoanApprovalRequestMapperImpl implements LoanApprovalRequestMapper {
    @Override
    public RecalculateHouseholdsRequest rejectLoanStage0(RejectLoanRequest request) {
        return RecalculateHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedBy(request.getLoan().getAudit().getCreatedBy())
                                                .setUpdatedBy(request.getLoan().getAudit().getUpdatedBy())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getLoan().getContact().getEmail())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                        .build())
                                .build())
                        .build())
                .build();
    }

    @Override
    public LintRepaymentsRequest listPendingLoansStage0(ListPendingLoansRequest request) {
        return LintRepaymentsRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
    }
}
