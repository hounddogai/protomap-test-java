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
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class CollateralApprovalRequestMapperImpl implements CollateralApprovalRequestMapper {
    @Override
    public SimulateLoanRequest rejectCollateralStage2(RejectCollateralRequest request, NormalizeAuditTrailResponse reply1) {
        return SimulateLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setBank(LoanBankFunding.newBuilder()
                                .setIban(reply1.getAuditTrail().getBank().getIban())
                                .build())
                        .build())
                .build();
    }

    @Override
    public PauseStandingOrderWorkflowRequest getCollateralApprovalStatusStage0(GetCollateralApprovalStatusRequest request) {
        return PauseStandingOrderWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getContext().getActor().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
    }
}
