package com.acme.finance.domains.lending;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.CheckAccessPolicyEligibilityRequest;
import com.acme.authorization.v1.CheckAccessPolicyEligibilityResponse;
import com.acme.authorization.v1.ScoreAccessPolicyRequest;
import com.acme.authorization.v1.ScoreAccessPolicyResponse;
import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.BrowseBankAccountsRequest;
import com.acme.banking.v1.BrowseBankAccountsResponse;
import com.acme.banking.v1.CreateBankAccountRequest;
import com.acme.banking.v1.CreateBankAccountResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.ValidateMandateRequest;
import com.acme.banking.v1.ValidateMandateResponse;
import com.acme.chat.v1.ChatRoom;
import com.acme.chat.v1.ChatRoomServiceGrpc;
import com.acme.chat.v1.CreateChatRoomRequest;
import com.acme.chat.v1.CreateChatRoomResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.ExplainCoverageCalculationRequest;
import com.acme.insurance.v1.ExplainCoverageCalculationResponse;
import com.acme.lending.v1.CalculateCollateralRequest;
import com.acme.lending.v1.CalculateCollateralResponse;
import com.acme.lending.v1.CalculateRepaymentRequest;
import com.acme.lending.v1.CalculateRepaymentResponse;
import com.acme.lending.v1.CheckLoanEligibilityRequest;
import com.acme.lending.v1.CheckLoanEligibilityResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.CollateralComponent;
import com.acme.lending.v1.CompareCollateralOptionsRequest;
import com.acme.lending.v1.CompareCollateralOptionsResponse;
import com.acme.lending.v1.CompareRepaymentOptionsRequest;
import com.acme.lending.v1.CompareRepaymentOptionsResponse;
import com.acme.lending.v1.EstimateCollateralRequest;
import com.acme.lending.v1.EstimateCollateralResponse;
import com.acme.lending.v1.EstimateRepaymentRequest;
import com.acme.lending.v1.EstimateRepaymentResponse;
import com.acme.lending.v1.ExplainLoanDecisionRequest;
import com.acme.lending.v1.ExplainLoanDecisionResponse;
import com.acme.lending.v1.ExplainRepaymentCalculationRequest;
import com.acme.lending.v1.ExplainRepaymentCalculationResponse;
import com.acme.lending.v1.GetRepaymentRatesRequest;
import com.acme.lending.v1.GetRepaymentRatesResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.LoanValidationServiceGrpc;
import com.acme.lending.v1.QuoteCollateralRequest;
import com.acme.lending.v1.QuoteCollateralResponse;
import com.acme.lending.v1.QuoteRepaymentRequest;
import com.acme.lending.v1.QuoteRepaymentResponse;
import com.acme.lending.v1.RecalculateRepaymentsRequest;
import com.acme.lending.v1.RecalculateRepaymentsResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.SimulateCollateralRequest;
import com.acme.lending.v1.SimulateCollateralResponse;
import com.acme.lending.v1.SimulateRepaymentRequest;
import com.acme.lending.v1.SimulateRepaymentResponse;
import com.acme.lending.v1.ValidateLoanApplicationRequest;
import com.acme.lending.v1.ValidateLoanApplicationResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.notifications.v1.ResolveNotificationsRequest;
import com.acme.notifications.v1.ResolveNotificationsResponse;
import com.acme.notifications.v1.SummarizeNotificationsRequest;
import com.acme.notifications.v1.SummarizeNotificationsResponse;
import com.acme.personalization.v1.CreateProfileRequest;
import com.acme.personalization.v1.CreateProfileResponse;
import com.acme.personalization.v1.Profile;
import com.acme.personalization.v1.ProfileServiceGrpc;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class RepaymentCalculationApplicationTest {
    @Test
    void simulateRepaymentPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LendingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollateral(String tenant, Collateral value) {}
            public void saveLoan(String tenant, Loan value) {}
            public void saveLoanApplication(String tenant, LoanApplication value) {}
            public void saveRepayment(String tenant, Repayment value) {}
        };
        var listener = new RepaymentCalculationApplicationEvents(BankAccountServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof RepaymentCalculationApplication.ExplainRepaymentCalculationStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new RepaymentCalculationApplication(
                AuditEventServiceGrpc.newStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                AccessPolicyValidationServiceGrpc.newBlockingStub(channel),
                BankAccountQueryServiceGrpc.newBlockingStub(channel),
                MandateValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ChatRoomServiceGrpc.newBlockingStub(channel),
                events,
                CollateralCalculationServiceGrpc.newBlockingStub(channel),
                LoanValidationServiceGrpc.newBlockingStub(channel),
                NotificationQueryServiceGrpc.newBlockingStub(channel),
                ProfileServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(SimulateRepaymentRequest.getDefaultInstance());
        var response = application.simulateRepayment(request);
        var sent0 = channel.request("acme.lending.v1.LoanValidationService/CheckLoanEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "repayment", "organizer", "display_name"),
                ProbeSupport.value(sent0, "loan", "holder", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "repayment", "audit", "updated_at"),
                ProbeSupport.value(sent0, "loan", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "repayment", "organizer", "email"),
                ProbeSupport.value(sent0, "loan", "contact", "email"));
        var sent1 = channel.request("acme.lending.v1.CollateralCalculationService/CompareCollateralOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "repayment", "organizer", "email"),
                ProbeSupport.value(sent1, "collateral", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "locality"),
                ProbeSupport.value(sent1, "details", "destination", "locality"));
        var sent2 = channel.request("acme.notifications.v1.NotificationQueryService/ResolveNotifications");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        var sent3 = channel.request("acme.personalization.v1.ProfileService/CreateProfile");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "repayment", "participants", "contact", "email"),
                ProbeSupport.value(sent3, "profile", "relationship_manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanValidationService/CheckLoanEligibility"), "loan", "contact", "preferred_channel"),
                ProbeSupport.value(response, "repayment", "participants", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralCalculationService/CompareCollateralOptions"), "collateral", "address", "recipient"),
                ProbeSupport.value(response, "repayment", "location", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationQueryService/ResolveNotifications"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationQueryService/ResolveNotifications"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "repayment_id"),
                ProbeSupport.value(response, "repayment", "repayment_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "repayment", "title"),
                ProbeSupport.value(response, "repayment", "title"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new RepaymentCalculationApplicationEvents(BankAccountServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CreateBankAccountResponse>();
        listener.on(new RepaymentCalculationApplication.ExplainRepaymentCalculationStage2Requested(ProbeSupport.sample(CreateBankAccountRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
