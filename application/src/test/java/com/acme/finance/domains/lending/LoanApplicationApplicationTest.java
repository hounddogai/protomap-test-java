package com.acme.finance.domains.lending;

import com.acme.audit.v1.ReconcileRetentionPoliciesRequest;
import com.acme.audit.v1.ReconcileRetentionPoliciesResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ClaimQueryServiceGrpc;
import com.acme.insurance.v1.ResolveClaimsRequest;
import com.acme.insurance.v1.ResolveClaimsResponse;
import com.acme.lending.v1.BatchGetLoanApplicationsRequest;
import com.acme.lending.v1.BatchGetLoanApplicationsResponse;
import com.acme.lending.v1.CheckCollateralEligibilityRequest;
import com.acme.lending.v1.CheckCollateralEligibilityResponse;
import com.acme.lending.v1.CloneLoanApplicationRequest;
import com.acme.lending.v1.CloneLoanApplicationResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.CreateLoanApplicationRequest;
import com.acme.lending.v1.CreateLoanApplicationResponse;
import com.acme.lending.v1.DeleteLoanApplicationRequest;
import com.acme.lending.v1.DeleteLoanApplicationResponse;
import com.acme.lending.v1.ExplainLoanCalculationRequest;
import com.acme.lending.v1.ExplainLoanCalculationResponse;
import com.acme.lending.v1.GetLoanApplicationRequest;
import com.acme.lending.v1.GetLoanApplicationResponse;
import com.acme.lending.v1.GetLoanApplicationRulesRequest;
import com.acme.lending.v1.GetLoanApplicationRulesResponse;
import com.acme.lending.v1.LintCollateralsRequest;
import com.acme.lending.v1.LintCollateralsResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.SearchLoanApplicationsRequest;
import com.acme.lending.v1.SearchLoanApplicationsResponse;
import com.acme.notifications.v1.ExplainNotificationDecisionRequest;
import com.acme.notifications.v1.ExplainNotificationDecisionResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationSla;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class LoanApplicationApplicationTest {
    @Test
    void createLoanApplicationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LendingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollateral(String tenant, Collateral value) {}
            public void saveLoan(String tenant, Loan value) {}
            public void saveLoanApplication(String tenant, LoanApplication value) {}
            public void saveRepayment(String tenant, Repayment value) {}
        };
        var application = new LoanApplicationApplication(
                RetentionPolicySyncServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ClaimQueryServiceGrpc.newBlockingStub(channel),
                CollateralValidationServiceGrpc.newBlockingStub(channel),
                LoanCalculationServiceGrpc.newBlockingStub(channel),
                NotificationValidationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(CreateLoanApplicationRequest.getDefaultInstance());
        var response = application.createLoanApplication(request);
        var sent0 = channel.request("acme.lending.v1.CollateralValidationService/LintCollaterals");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "billing_address", "country_code"),
                ProbeSupport.value(sent0, "collateral", "address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "audit", "updated_by"),
                ProbeSupport.value(sent0, "collateral", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "audit", "etag"),
                ProbeSupport.value(sent0, "collateral", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "billing_address", "postal_code"),
                ProbeSupport.value(sent0, "collateral", "address", "postal_code"));
        var sent1 = channel.request("acme.insurance.v1.ClaimQueryService/ResolveClaims");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/LintCollaterals"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimQueryService/ResolveClaims"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "total"),
                ProbeSupport.value(response, "loan_application", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "counterparty"),
                ProbeSupport.value(response, "loan_application", "counterparty"));
    }
}
