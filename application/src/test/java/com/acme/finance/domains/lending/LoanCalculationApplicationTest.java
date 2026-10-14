package com.acme.finance.domains.lending;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.banking.v1.BatchGetBankAccountsRequest;
import com.acme.banking.v1.BatchGetBankAccountsResponse;
import com.acme.banking.v1.LintStandingOrdersRequest;
import com.acme.banking.v1.LintStandingOrdersResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.insurance.v1.CheckPolicyEligibilityRequest;
import com.acme.insurance.v1.CheckPolicyEligibilityResponse;
import com.acme.insurance.v1.ExplainPolicyDecisionRequest;
import com.acme.insurance.v1.ExplainPolicyDecisionResponse;
import com.acme.insurance.v1.NormalizePolicyRequest;
import com.acme.insurance.v1.NormalizePolicyResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.PolicyLimit;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.ListJournalEntryVersionsRequest;
import com.acme.ledger.v1.ListJournalEntryVersionsResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.StreamPostingBatchNoticesRequest;
import com.acme.ledger.v1.StreamPostingBatchNoticesResponse;
import com.acme.lending.v1.CalculateLoanApplicationRequest;
import com.acme.lending.v1.CalculateLoanApplicationResponse;
import com.acme.lending.v1.CalculateLoanRequest;
import com.acme.lending.v1.CalculateLoanResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CompareLoanApplicationOptionsRequest;
import com.acme.lending.v1.CompareLoanApplicationOptionsResponse;
import com.acme.lending.v1.CompareLoanOptionsRequest;
import com.acme.lending.v1.CompareLoanOptionsResponse;
import com.acme.lending.v1.EstimateLoanApplicationRequest;
import com.acme.lending.v1.EstimateLoanApplicationResponse;
import com.acme.lending.v1.EstimateLoanRequest;
import com.acme.lending.v1.EstimateLoanResponse;
import com.acme.lending.v1.ExplainLoanCalculationRequest;
import com.acme.lending.v1.ExplainLoanCalculationResponse;
import com.acme.lending.v1.GetLoanRatesRequest;
import com.acme.lending.v1.GetLoanRatesResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationBankSettlement;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.LoanApplicationCardSettlement;
import com.acme.lending.v1.LoanApplicationLine;
import com.acme.lending.v1.LoanApplicationLineTax;
import com.acme.lending.v1.QuoteLoanRequest;
import com.acme.lending.v1.QuoteLoanResponse;
import com.acme.lending.v1.RecalculateLoanApplicationsRequest;
import com.acme.lending.v1.RecalculateLoanApplicationsResponse;
import com.acme.lending.v1.RecalculateLoansRequest;
import com.acme.lending.v1.RecalculateLoansResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.SimulateLoanApplicationRequest;
import com.acme.lending.v1.SimulateLoanApplicationResponse;
import com.acme.lending.v1.SimulateLoanRequest;
import com.acme.lending.v1.SimulateLoanResponse;
import com.acme.workplaces.v1.BuildingQueryServiceGrpc;
import com.acme.workplaces.v1.ExportBuildingsRequest;
import com.acme.workplaces.v1.ExportBuildingsResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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

class LoanCalculationApplicationTest {
    @Test
    void compareLoanOptionsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LendingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollateral(String tenant, Collateral value) {}
            public void saveLoan(String tenant, Loan value) {}
            public void saveLoanApplication(String tenant, LoanApplication value) {}
            public void saveRepayment(String tenant, Repayment value) {}
        };
        var application = new LoanCalculationApplication(
                AuditEventServiceGrpc.newStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                StandingOrderValidationServiceGrpc.newFutureStub(channel),
                ChannelRegistry.fixed(channel),
                PolicyValidationServiceGrpc.newBlockingStub(channel),
                JournalEntryHistoryServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                LoanApplicationCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                BuildingQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(CompareLoanOptionsRequest.getDefaultInstance());
        var response = application.compareLoanOptions(request);
        var sent0 = channel.request("acme.lending.v1.LoanApplicationCalculationService/RecalculateLoanApplications");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "audit", "created_by"),
                ProbeSupport.value(sent0, "loan_application", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "country_code"),
                ProbeSupport.value(sent0, "loan_application", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "card", "token"),
                ProbeSupport.value(sent0, "loan_application", "card", "token"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "lines"),
                ProbeSupport.value(sent0, "details", "destination", "lines"));
        var sent1 = channel.request("acme.banking.v1.StandingOrderValidationService/LintStandingOrders");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "postal_code"),
                ProbeSupport.value(sent1, "standing_order", "location", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationCalculationService/RecalculateLoanApplications"), "loan_application", "audit", "created_by"),
                ProbeSupport.value(sent1, "standing_order", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationCalculationService/RecalculateLoanApplications"), "loan_application", "audit", "updated_by"),
                ProbeSupport.value(sent1, "standing_order", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "contact", "email"),
                ProbeSupport.value(sent1, "standing_order", "participants", "contact", "email"));
        var sent2 = channel.request("acme.banking.v1.BankAccountService/BatchGetBankAccounts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        var sent3 = channel.request("acme.lending.v1.LoanApplicationCalculationService/EstimateLoanApplication");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "limits", "amount"),
                ProbeSupport.value(sent3, "loan_application", "lines", "tax", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationCalculationService/RecalculateLoanApplications"), "loan_application", "audit", "updated_by"),
                ProbeSupport.value(response, "loan", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderValidationService/LintStandingOrders"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.BankAccountService/BatchGetBankAccounts"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_id"),
                ProbeSupport.value(response, "loan", "loan_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "status"),
                ProbeSupport.value(response, "loan", "status"));
    }
}
