package com.acme.finance.domains.lending;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.boxoffice.v1.BoxOfficeSaleCalculationServiceGrpc;
import com.acme.boxoffice.v1.RecalculateBoxOfficeSalesRequest;
import com.acme.boxoffice.v1.RecalculateBoxOfficeSalesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.BatchCreateContractsRequest;
import com.acme.contracts.v1.BatchCreateContractsResponse;
import com.acme.contracts.v1.ContractServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.EscalatePremiumRequest;
import com.acme.insurance.v1.EscalatePremiumResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumApprovalServiceGrpc;
import com.acme.insurance.v1.ShareCoverageReportRequest;
import com.acme.insurance.v1.ShareCoverageReportResponse;
import com.acme.lending.v1.AdvanceCollateralWorkflowRequest;
import com.acme.lending.v1.AdvanceCollateralWorkflowResponse;
import com.acme.lending.v1.ApproveLoanApplicationRequest;
import com.acme.lending.v1.ApproveLoanApplicationResponse;
import com.acme.lending.v1.CancelCollateralWorkflowRequest;
import com.acme.lending.v1.CancelCollateralWorkflowResponse;
import com.acme.lending.v1.CancelLoanWorkflowRequest;
import com.acme.lending.v1.CancelLoanWorkflowResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralApprovalServiceGrpc;
import com.acme.lending.v1.CollateralDimensions;
import com.acme.lending.v1.CommentCollateralApprovalRequest;
import com.acme.lending.v1.CommentCollateralApprovalResponse;
import com.acme.lending.v1.CommentRepaymentApprovalRequest;
import com.acme.lending.v1.CommentRepaymentApprovalResponse;
import com.acme.lending.v1.ListCollateralWorkflowsRequest;
import com.acme.lending.v1.ListCollateralWorkflowsResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationApprovalServiceGrpc;
import com.acme.lending.v1.LoanApplicationCardSettlement;
import com.acme.lending.v1.LoanCardFunding;
import com.acme.lending.v1.LoanWorkflowServiceGrpc;
import com.acme.lending.v1.PauseCollateralWorkflowRequest;
import com.acme.lending.v1.PauseCollateralWorkflowResponse;
import com.acme.lending.v1.QuoteRepaymentRequest;
import com.acme.lending.v1.QuoteRepaymentResponse;
import com.acme.lending.v1.RejectLoanApplicationRequest;
import com.acme.lending.v1.RejectLoanApplicationResponse;
import com.acme.lending.v1.RejectRepaymentRequest;
import com.acme.lending.v1.RejectRepaymentResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentApprovalServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.ResumeCollateralWorkflowRequest;
import com.acme.lending.v1.ResumeCollateralWorkflowResponse;
import com.acme.lending.v1.SignalCollateralWorkflowRequest;
import com.acme.lending.v1.SignalCollateralWorkflowResponse;
import com.acme.lending.v1.StartCollateralWorkflowRequest;
import com.acme.lending.v1.StartCollateralWorkflowResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.metrics.v1.ScoreMetricSeriesRequest;
import com.acme.metrics.v1.ScoreMetricSeriesResponse;
import com.acme.payments.v1.CalculatePaymentRequest;
import com.acme.payments.v1.CalculatePaymentResponse;
import com.acme.payments.v1.Payment;
import com.acme.payments.v1.PaymentCalculationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class CollateralWorkflowApplicationTest {
    @Test
    void advanceCollateralWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LendingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollateral(String tenant, Collateral value) {}
            public void saveLoan(String tenant, Loan value) {}
            public void saveLoanApplication(String tenant, LoanApplication value) {}
            public void saveRepayment(String tenant, Repayment value) {}
        };
        var listener = new CollateralWorkflowApplicationEvents(PaymentCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof CollateralWorkflowApplication.ResumeCollateralWorkflowStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new CollateralWorkflowApplication(
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BoxOfficeSaleCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ContractServiceGrpc.newStub(channel),
                events,
                PremiumApprovalServiceGrpc.newBlockingStub(channel),
                LoanApplicationApprovalServiceGrpc.newBlockingStub(channel),
                LoanWorkflowServiceGrpc.newBlockingStub(channel),
                RepaymentApprovalServiceGrpc.newBlockingStub(channel),
                MetricSeriesValidationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(AdvanceCollateralWorkflowRequest.getDefaultInstance());
        var response = application.advanceCollateralWorkflow(request);
        var sent0 = channel.request("acme.insurance.v1.PremiumApprovalService/EscalatePremium");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collateral", "address", "region"),
                ProbeSupport.value(sent0, "premium", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collateral", "address", "recipient"),
                ProbeSupport.value(sent0, "premium", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collateral", "audit", "created_at"),
                ProbeSupport.value(sent0, "premium", "audit", "created_at"));
        var sent1 = channel.request("acme.lending.v1.LoanWorkflowService/CancelLoanWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "phone"),
                ProbeSupport.value(sent1, "details", "notify", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumApprovalService/EscalatePremium"), "premium", "card", "holder_name"),
                ProbeSupport.value(sent1, "loan", "card", "holder_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumApprovalService/EscalatePremium"), "premium", "audit", "created_at"),
                ProbeSupport.value(sent1, "loan", "audit", "created_at"));
        var sent2 = channel.request("acme.lending.v1.LoanApplicationApprovalService/ApproveLoanApplication");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanWorkflowService/CancelLoanWorkflow"), "loan", "card", "token"),
                ProbeSupport.value(sent2, "loan_application", "card", "token"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanWorkflowService/CancelLoanWorkflow"), "loan", "audit", "etag"),
                ProbeSupport.value(sent2, "loan_application", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanWorkflowService/CancelLoanWorkflow"), "loan", "holder", "display_name"),
                ProbeSupport.value(sent2, "loan_application", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumApprovalService/EscalatePremium"), "outcome", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumApprovalService/EscalatePremium"), "premium", "counterparty", "email"),
                ProbeSupport.value(response, "collateral", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationApprovalService/ApproveLoanApplication"), "loan_application", "audit", "etag"),
                ProbeSupport.value(response, "collateral", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collateral_id"),
                ProbeSupport.value(response, "collateral", "collateral_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collateral", "status"),
                ProbeSupport.value(response, "collateral", "status"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new CollateralWorkflowApplicationEvents(PaymentCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CalculatePaymentResponse>();
        listener.on(new CollateralWorkflowApplication.ResumeCollateralWorkflowStage1Requested(ProbeSupport.sample(CalculatePaymentRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
