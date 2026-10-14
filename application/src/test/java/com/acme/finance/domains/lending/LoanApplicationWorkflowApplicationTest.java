package com.acme.finance.domains.lending;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CreateCustomerRequest;
import com.acme.customers.v1.CreateCustomerResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerPreferences;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.LookupCustomerRequest;
import com.acme.customers.v1.LookupCustomerResponse;
import com.acme.customers.v1.QuoteHouseholdRequest;
import com.acme.customers.v1.QuoteHouseholdResponse;
import com.acme.customers.v1.RecalculateHouseholdsRequest;
import com.acme.customers.v1.RecalculateHouseholdsResponse;
import com.acme.customers.v1.ScoreHouseholdRequest;
import com.acme.customers.v1.ScoreHouseholdResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.PeekUserRequest;
import com.acme.identity.v1.PeekUserResponse;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ApprovePremiumRequest;
import com.acme.insurance.v1.ApprovePremiumResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumApprovalServiceGrpc;
import com.acme.insurance.v1.PremiumBankSettlement;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.ResolvePremiumConflictRequest;
import com.acme.insurance.v1.ResolvePremiumConflictResponse;
import com.acme.learning.v1.Course;
import com.acme.learning.v1.CourseServiceGrpc;
import com.acme.learning.v1.CreateCourseRequest;
import com.acme.learning.v1.CreateCourseResponse;
import com.acme.lending.v1.AdvanceLoanApplicationWorkflowRequest;
import com.acme.lending.v1.AdvanceLoanApplicationWorkflowResponse;
import com.acme.lending.v1.CancelLoanApplicationWorkflowRequest;
import com.acme.lending.v1.CancelLoanApplicationWorkflowResponse;
import com.acme.lending.v1.CheckLoanApplicationEligibilityRequest;
import com.acme.lending.v1.CheckLoanApplicationEligibilityResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.CollateralComponent;
import com.acme.lending.v1.EstimateCollateralRequest;
import com.acme.lending.v1.EstimateCollateralResponse;
import com.acme.lending.v1.InspectLoanApplicationWorkflowRequest;
import com.acme.lending.v1.InspectLoanApplicationWorkflowResponse;
import com.acme.lending.v1.ListPendingLoanApplicationsRequest;
import com.acme.lending.v1.ListPendingLoanApplicationsResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationApprovalServiceGrpc;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.lending.v1.LoanLifecycleServiceGrpc;
import com.acme.lending.v1.LoanLimit;
import com.acme.lending.v1.PauseLoanApplicationWorkflowRequest;
import com.acme.lending.v1.PauseLoanApplicationWorkflowResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.ResumeLoanApplicationWorkflowRequest;
import com.acme.lending.v1.ResumeLoanApplicationWorkflowResponse;
import com.acme.lending.v1.RetryLoanApplicationWorkflowRequest;
import com.acme.lending.v1.RetryLoanApplicationWorkflowResponse;
import com.acme.lending.v1.ScheduleLoanExpiryRequest;
import com.acme.lending.v1.ScheduleLoanExpiryResponse;
import com.acme.lending.v1.SignalLoanApplicationWorkflowRequest;
import com.acme.lending.v1.SignalLoanApplicationWorkflowResponse;
import com.acme.lending.v1.StartLoanApplicationWorkflowRequest;
import com.acme.lending.v1.StartLoanApplicationWorkflowResponse;
import com.acme.lending.v1.SubmitLoanRequest;
import com.acme.lending.v1.SubmitLoanResponse;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterQueryServiceGrpc;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.risk.v1.SummarizeRiskRegistersRequest;
import com.acme.risk.v1.SummarizeRiskRegistersResponse;
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

class LoanApplicationWorkflowApplicationTest {
    @Test
    void startLoanApplicationWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LendingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollateral(String tenant, Collateral value) {}
            public void saveLoan(String tenant, Loan value) {}
            public void saveLoanApplication(String tenant, LoanApplication value) {}
            public void saveRepayment(String tenant, Repayment value) {}
        };
        var application = new LoanApplicationWorkflowApplication(
                RetentionPolicySyncServiceGrpc.newStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                CustomerServiceGrpc.newBlockingStub(channel),
                HouseholdCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                UserQueryServiceGrpc.newBlockingStub(channel),
                PremiumApprovalServiceGrpc.newBlockingStub(channel),
                PremiumSyncServiceGrpc.newBlockingStub(channel),
                CourseServiceGrpc.newBlockingStub(channel),
                CollateralCalculationServiceGrpc.newBlockingStub(channel),
                LoanApplicationApprovalServiceGrpc.newBlockingStub(channel),
                LoanApplicationValidationServiceGrpc.newBlockingStub(channel),
                LoanApprovalServiceGrpc.newBlockingStub(channel),
                LoanLifecycleServiceGrpc.newBlockingStub(channel),
                new LoanApplicationWorkflowRequestMapperImpl(),
                repository,
                RiskRegisterQueryServiceGrpc.newFutureStub(channel),
                RiskRegisterServiceGrpc.newStub(channel),
                Runnable::run);
        var request = ProbeSupport.sample(StartLoanApplicationWorkflowRequest.getDefaultInstance());
        var response = application.startLoanApplicationWorkflow(request);
        var sent0 = channel.request("acme.lending.v1.CollateralCalculationService/EstimateCollateral");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "audit", "updated_by"),
                ProbeSupport.value(sent0, "collateral", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "billing_address", "region"),
                ProbeSupport.value(sent0, "collateral", "address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "counterparty", "email"),
                ProbeSupport.value(sent0, "collateral", "owner", "email"));
        var sent1 = channel.request("acme.insurance.v1.PremiumApprovalService/ApprovePremium");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralCalculationService/EstimateCollateral"), "collateral", "address", "locality"),
                ProbeSupport.value(sent1, "premium", "billing_address", "locality"));
        var sent2 = channel.request("acme.risk.v1.RiskRegisterQueryService/SummarizeRiskRegisters");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent2, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent2, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralCalculationService/EstimateCollateral"), "result", "details"),
                ProbeSupport.value(sent2, "details", "filters"));
        var sent3 = channel.request("acme.lending.v1.LoanApplicationApprovalService/ListPendingLoanApplications");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralCalculationService/EstimateCollateral"), "collateral", "address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralCalculationService/EstimateCollateral"), "collateral", "audit", "updated_at"),
                ProbeSupport.value(sent3, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralCalculationService/EstimateCollateral"), "collateral", "components", "component_id"),
                ProbeSupport.value(sent3, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralCalculationService/EstimateCollateral"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralCalculationService/EstimateCollateral"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumApprovalService/ApprovePremium"), "premium", "billing_address"),
                ProbeSupport.value(response, "loan_application", "billing_address"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterQueryService/SummarizeRiskRegisters"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationApprovalService/ListPendingLoanApplications"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application_id"),
                ProbeSupport.value(response, "loan_application", "loan_application_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "due_at"),
                ProbeSupport.value(response, "loan_application", "due_at"));
    }
}
