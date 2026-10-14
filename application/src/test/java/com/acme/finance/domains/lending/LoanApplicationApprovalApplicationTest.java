package com.acme.finance.domains.lending;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerDecisionRequest;
import com.acme.customers.v1.ExplainCustomerDecisionResponse;
import com.acme.customers.v1.FindHouseholdsRequest;
import com.acme.customers.v1.FindHouseholdsResponse;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.QuoteConsentRecordRequest;
import com.acme.customers.v1.QuoteConsentRecordResponse;
import com.acme.customers.v1.ResolveHouseholdsRequest;
import com.acme.customers.v1.ResolveHouseholdsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageWorkflowServiceGrpc;
import com.acme.insurance.v1.ResumeCoverageWorkflowRequest;
import com.acme.insurance.v1.ResumeCoverageWorkflowResponse;
import com.acme.lending.v1.ApproveLoanApplicationRequest;
import com.acme.lending.v1.ApproveLoanApplicationResponse;
import com.acme.lending.v1.CalculateCollateralRequest;
import com.acme.lending.v1.CalculateCollateralResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.CommentLoanApplicationApprovalRequest;
import com.acme.lending.v1.CommentLoanApplicationApprovalResponse;
import com.acme.lending.v1.DelegateLoanApplicationApprovalRequest;
import com.acme.lending.v1.DelegateLoanApplicationApprovalResponse;
import com.acme.lending.v1.ExplainLoanApplicationCalculationRequest;
import com.acme.lending.v1.ExplainLoanApplicationCalculationResponse;
import com.acme.lending.v1.GenerateRepaymentReportRequest;
import com.acme.lending.v1.GenerateRepaymentReportResponse;
import com.acme.lending.v1.GetLoanApplicationApprovalStatusRequest;
import com.acme.lending.v1.GetLoanApplicationApprovalStatusResponse;
import com.acme.lending.v1.ListPendingLoanApplicationsRequest;
import com.acme.lending.v1.ListPendingLoanApplicationsResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.LoanApplicationCardSettlement;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.NormalizeLoanApplicationRequest;
import com.acme.lending.v1.NormalizeLoanApplicationResponse;
import com.acme.lending.v1.RecallLoanApplicationRequest;
import com.acme.lending.v1.RecallLoanApplicationResponse;
import com.acme.lending.v1.RejectLoanApplicationRequest;
import com.acme.lending.v1.RejectLoanApplicationResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.lending.v1.StreamRepaymentMetricsRequest;
import com.acme.lending.v1.StreamRepaymentMetricsResponse;
import com.acme.lending.v1.SubmitLoanApplicationRequest;
import com.acme.lending.v1.SubmitLoanApplicationResponse;
import com.acme.lending.v1.ValidateLoanApplicationRequest;
import com.acme.lending.v1.ValidateLoanApplicationResponse;
import com.acme.notifications.v1.ExportNotificationsRequest;
import com.acme.notifications.v1.ExportNotificationsResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
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

class LoanApplicationApprovalApplicationTest {
    @Test
    void delegateLoanApplicationApprovalPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LendingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollateral(String tenant, Collateral value) {}
            public void saveLoan(String tenant, Loan value) {}
            public void saveLoanApplication(String tenant, LoanApplication value) {}
            public void saveRepayment(String tenant, Repayment value) {}
        };
        var application = new LoanApplicationApprovalApplication(
                ChannelRegistry.fixed(channel),
                ConsentRecordCalculationServiceGrpc.newBlockingStub(channel),
                CustomerValidationServiceGrpc.newBlockingStub(channel),
                HouseholdQueryServiceGrpc.newBlockingStub(channel),
                CoverageWorkflowServiceGrpc.newBlockingStub(channel),
                LoanApplicationCalculationServiceGrpc.newBlockingStub(channel),
                LoanApplicationValidationServiceGrpc.newBlockingStub(channel),
                RepaymentReportingServiceGrpc.newBlockingStub(channel),
                new LoanApplicationApprovalRequestMapperImpl(),
                NotificationQueryServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(DelegateLoanApplicationApprovalRequest.getDefaultInstance());
        var response = application.delegateLoanApplicationApproval(request);
        var sent0 = channel.request("acme.lending.v1.RepaymentReportingService/GenerateRepaymentReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "lines", "attributes"),
                ProbeSupport.value(sent0, "details", "filters"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        var sent1 = channel.request("acme.lending.v1.LoanApplicationCalculationService/ExplainLoanApplicationCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent1, "loan_application", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "billing_address", "recipient"),
                ProbeSupport.value(sent1, "loan_application", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "billing_address", "postal_code"),
                ProbeSupport.value(sent1, "loan_application", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "loan_application", "counterparty", "display_name"));
        var sent2 = channel.request("acme.insurance.v1.CoverageWorkflowService/ResumeCoverageWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "audit", "etag"),
                ProbeSupport.value(sent2, "coverage", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "counterparty", "display_name"),
                ProbeSupport.value(sent2, "coverage", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "counterparty", "email"),
                ProbeSupport.value(sent2, "details", "notify", "email"));
        var sent3 = channel.request("acme.notifications.v1.NotificationQueryService/ExportNotifications");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationCalculationService/ExplainLoanApplicationCalculation"), "loan_application", "billing_address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationCalculationService/ExplainLoanApplicationCalculation"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationCalculationService/ExplainLoanApplicationCalculation"), "loan_application", "notes"),
                ProbeSupport.value(response, "loan_application", "notes"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageWorkflowService/ResumeCoverageWorkflow"), "outcome", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageWorkflowService/ResumeCoverageWorkflow"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application_id"),
                ProbeSupport.value(response, "loan_application", "loan_application_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "number"),
                ProbeSupport.value(response, "loan_application", "number"));
    }
}
