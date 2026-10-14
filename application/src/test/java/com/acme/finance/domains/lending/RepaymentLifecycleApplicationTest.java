package com.acme.finance.domains.lending;

import com.acme.audit.v1.ResolveRetentionPolicyConflictRequest;
import com.acme.audit.v1.ResolveRetentionPolicyConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CheckPreferenceEligibilityRequest;
import com.acme.customers.v1.CheckPreferenceEligibilityResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.LintCustomersRequest;
import com.acme.customers.v1.LintCustomersResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.ActivateRepaymentRequest;
import com.acme.lending.v1.ActivateRepaymentResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.EstimateRepaymentRequest;
import com.acme.lending.v1.EstimateRepaymentResponse;
import com.acme.lending.v1.ExpireRepaymentRequest;
import com.acme.lending.v1.ExpireRepaymentResponse;
import com.acme.lending.v1.ExplainLoanCalculationRequest;
import com.acme.lending.v1.ExplainLoanCalculationResponse;
import com.acme.lending.v1.GetRepaymentLifecycleRequest;
import com.acme.lending.v1.GetRepaymentLifecycleResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.LoanLifecycleServiceGrpc;
import com.acme.lending.v1.PreviewLoanApplicationRequest;
import com.acme.lending.v1.PreviewLoanApplicationResponse;
import com.acme.lending.v1.RenewRepaymentRequest;
import com.acme.lending.v1.RenewRepaymentResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentCalculationServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.ScheduleLoanExpiryRequest;
import com.acme.lending.v1.ScheduleLoanExpiryResponse;
import com.acme.lending.v1.ScheduleRepaymentExpiryRequest;
import com.acme.lending.v1.ScheduleRepaymentExpiryResponse;
import com.acme.lending.v1.SimulateRepaymentRequest;
import com.acme.lending.v1.SimulateRepaymentResponse;
import com.acme.lending.v1.SuspendRepaymentRequest;
import com.acme.lending.v1.SuspendRepaymentResponse;
import com.acme.lending.v1.TerminateRepaymentRequest;
import com.acme.lending.v1.TerminateRepaymentResponse;
import com.acme.search.v1.ResolveSearchIndexesRequest;
import com.acme.search.v1.ResolveSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.venues.v1.PreviewVenueRequest;
import com.acme.venues.v1.PreviewVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class RepaymentLifecycleApplicationTest {
    @Test
    void suspendRepaymentPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LendingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollateral(String tenant, Collateral value) {}
            public void saveLoan(String tenant, Loan value) {}
            public void saveLoanApplication(String tenant, LoanApplication value) {}
            public void saveRepayment(String tenant, Repayment value) {}
        };
        var listener = new RepaymentLifecycleApplicationEvents(SearchIndexQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof RepaymentLifecycleApplication.RenewRepaymentStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new RepaymentLifecycleApplication(
                RetentionPolicySyncServiceGrpc.newBlockingStub(channel),
                CustomerValidationServiceGrpc.newBlockingStub(channel),
                PreferenceValidationServiceGrpc.newFutureStub(channel),
                events,
                LoanApplicationValidationServiceGrpc.newBlockingStub(channel),
                LoanCalculationServiceGrpc.newBlockingStub(channel),
                LoanLifecycleServiceGrpc.newBlockingStub(channel),
                RepaymentCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                VenueValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SuspendRepaymentRequest.getDefaultInstance());
        var response = application.suspendRepayment(request);
        var sent0 = channel.request("acme.lending.v1.LoanCalculationService/ExplainLoanCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "name"),
                ProbeSupport.value(sent0, "loan", "contact", "name"));
        var sent1 = channel.request("acme.lending.v1.RepaymentCalculationService/EstimateRepayment");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanCalculationService/ExplainLoanCalculation"), "loan", "contact", "preferred_channel"),
                ProbeSupport.value(sent1, "repayment", "participants", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanCalculationService/ExplainLoanCalculation"), "loan", "contact", "phone"),
                ProbeSupport.value(sent1, "repayment", "participants", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanCalculationService/ExplainLoanCalculation"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanCalculationService/ExplainLoanCalculation"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentCalculationService/EstimateRepayment"), "repayment", "location", "locality"),
                ProbeSupport.value(response, "repayment", "location", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentCalculationService/EstimateRepayment"), "repayment", "status"),
                ProbeSupport.value(response, "repayment", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "repayment_id"),
                ProbeSupport.value(response, "repayment", "repayment_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new RepaymentLifecycleApplicationEvents(SearchIndexQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ResolveSearchIndexesResponse>();
        listener.on(new RepaymentLifecycleApplication.RenewRepaymentStage1Requested(ProbeSupport.sample(ResolveSearchIndexesRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
