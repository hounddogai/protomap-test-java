package com.acme.finance.domains.lending;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.NotifyAuditEventWatchersRequest;
import com.acme.audit.v1.NotifyAuditEventWatchersResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.CountAccessPoliciesRequest;
import com.acme.authorization.v1.CountAccessPoliciesResponse;
import com.acme.banking.v1.ExportStatementsRequest;
import com.acme.banking.v1.ExportStatementsResponse;
import com.acme.banking.v1.StatementQueryServiceGrpc;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.collections.v1.UpdateCollectionCaseRequest;
import com.acme.collections.v1.UpdateCollectionCaseResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ExplainClaimCalculationRequest;
import com.acme.insurance.v1.ExplainClaimCalculationResponse;
import com.acme.lending.v1.CalculateCollateralRequest;
import com.acme.lending.v1.CalculateCollateralResponse;
import com.acme.lending.v1.CalculateLoanRequest;
import com.acme.lending.v1.CalculateLoanResponse;
import com.acme.lending.v1.CheckLoanApplicationEligibilityRequest;
import com.acme.lending.v1.CheckLoanApplicationEligibilityResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralComponent;
import com.acme.lending.v1.CompareCollateralOptionsRequest;
import com.acme.lending.v1.CompareCollateralOptionsResponse;
import com.acme.lending.v1.EstimateCollateralRequest;
import com.acme.lending.v1.EstimateCollateralResponse;
import com.acme.lending.v1.ExplainLoanApplicationDecisionRequest;
import com.acme.lending.v1.ExplainLoanApplicationDecisionResponse;
import com.acme.lending.v1.GetCollateralRatesRequest;
import com.acme.lending.v1.GetCollateralRatesResponse;
import com.acme.lending.v1.LintLoanApplicationsRequest;
import com.acme.lending.v1.LintLoanApplicationsResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.LoanApplicationLine;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.QuoteCollateralRequest;
import com.acme.lending.v1.QuoteCollateralResponse;
import com.acme.lending.v1.QuoteLoanApplicationRequest;
import com.acme.lending.v1.QuoteLoanApplicationResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.SimulateCollateralRequest;
import com.acme.lending.v1.SimulateCollateralResponse;
import com.google.protobuf.Any;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class CollateralCalculationApplicationTest {
    @Test
    void compareCollateralOptionsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LendingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollateral(String tenant, Collateral value) {}
            public void saveLoan(String tenant, Loan value) {}
            public void saveLoanApplication(String tenant, LoanApplication value) {}
            public void saveRepayment(String tenant, Repayment value) {}
        };
        var application = new CollateralCalculationApplication(
                AuditEventNotificationServiceGrpc.newBlockingStub(channel),
                AuditEventServiceGrpc.newStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                StatementQueryServiceGrpc.newBlockingStub(channel),
                CollectionCaseServiceGrpc.newBlockingStub(channel),
                ClaimCalculationServiceGrpc.newBlockingStub(channel),
                LoanApplicationCalculationServiceGrpc.newBlockingStub(channel),
                LoanApplicationValidationServiceGrpc.newBlockingStub(channel),
                LoanCalculationServiceGrpc.newBlockingStub(channel),
                new CollateralCalculationRequestMapperImpl(),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(CompareCollateralOptionsRequest.getDefaultInstance());
        var response = application.compareCollateralOptions(request);
        var sent0 = channel.request("acme.audit.v1.AuditEventNotificationService/NotifyAuditEventWatchers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collateral", "owner", "email"),
                ProbeSupport.value(sent0, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        var sent1 = channel.request("acme.lending.v1.LoanApplicationValidationService/CheckLoanApplicationEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collateral", "address", "locality"),
                ProbeSupport.value(sent1, "loan_application", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collateral", "audit", "updated_at"),
                ProbeSupport.value(sent1, "loan_application", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "recipient"),
                ProbeSupport.value(sent1, "loan_application", "billing_address", "recipient"));
        var sent2 = channel.request("acme.insurance.v1.ClaimCalculationService/ExplainClaimCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventNotificationService/NotifyAuditEventWatchers"), "audit_event", "subject", "display_name"),
                ProbeSupport.value(sent2, "claim", "customer", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collateral", "owner", "user_id"),
                ProbeSupport.value(sent2, "claim", "assignee", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventNotificationService/NotifyAuditEventWatchers"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationValidationService/CheckLoanApplicationEligibility"), "loan_application", "total"),
                ProbeSupport.value(response, "breakdown", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationValidationService/CheckLoanApplicationEligibility"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimCalculationService/ExplainClaimCalculation"), "claim", "customer", "display_name"),
                ProbeSupport.value(response, "collateral", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collateral_id"),
                ProbeSupport.value(response, "collateral", "collateral_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "currency_code"),
                ProbeSupport.value(response, "breakdown", "currency_code"));
    }
}
