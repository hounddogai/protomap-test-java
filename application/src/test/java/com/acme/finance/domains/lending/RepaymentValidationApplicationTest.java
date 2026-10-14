package com.acme.finance.domains.lending;

import com.acme.audit.v1.PullRetentionPoliciesRequest;
import com.acme.audit.v1.PullRetentionPoliciesResponse;
import com.acme.audit.v1.ResolveRetentionPolicyConflictRequest;
import com.acme.audit.v1.ResolveRetentionPolicyConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.SendRetentionPolicyTestNoticeRequest;
import com.acme.audit.v1.SendRetentionPolicyTestNoticeResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.ExportAccessPoliciesRequest;
import com.acme.authorization.v1.ExportAccessPoliciesResponse;
import com.acme.banking.v1.ExplainStandingOrderDecisionRequest;
import com.acme.banking.v1.ExplainStandingOrderDecisionResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.employees.v1.EmployeeRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.FindUsersRequest;
import com.acme.identity.v1.FindUsersResponse;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculatePolicyRequest;
import com.acme.insurance.v1.CalculatePolicyResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.itsm.v1.Incident;
import com.acme.itsm.v1.IncidentCalculationServiceGrpc;
import com.acme.itsm.v1.RecalculateIncidentsRequest;
import com.acme.itsm.v1.RecalculateIncidentsResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodWeeklyRule;
import com.acme.ledger.v1.RecordFiscalPeriodVersionRequest;
import com.acme.ledger.v1.RecordFiscalPeriodVersionResponse;
import com.acme.lending.v1.CheckRepaymentEligibilityRequest;
import com.acme.lending.v1.CheckRepaymentEligibilityResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.CompareCollateralOptionsRequest;
import com.acme.lending.v1.CompareCollateralOptionsResponse;
import com.acme.lending.v1.ExplainRepaymentDecisionRequest;
import com.acme.lending.v1.ExplainRepaymentDecisionResponse;
import com.acme.lending.v1.GetRepaymentRulesRequest;
import com.acme.lending.v1.GetRepaymentRulesResponse;
import com.acme.lending.v1.LintRepaymentsRequest;
import com.acme.lending.v1.LintRepaymentsResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.LoanValidationServiceGrpc;
import com.acme.lending.v1.NormalizeRepaymentRequest;
import com.acme.lending.v1.NormalizeRepaymentResponse;
import com.acme.lending.v1.PreviewLoanApplicationRequest;
import com.acme.lending.v1.PreviewLoanApplicationResponse;
import com.acme.lending.v1.PreviewLoanRequest;
import com.acme.lending.v1.PreviewLoanResponse;
import com.acme.lending.v1.PreviewRepaymentRequest;
import com.acme.lending.v1.PreviewRepaymentResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.ScoreCollateralRequest;
import com.acme.lending.v1.ScoreCollateralResponse;
import com.acme.lending.v1.ScoreRepaymentRequest;
import com.acme.lending.v1.ScoreRepaymentResponse;
import com.acme.lending.v1.SimulateCollateralRequest;
import com.acme.lending.v1.SimulateCollateralResponse;
import com.acme.lending.v1.SimulateLoanApplicationRequest;
import com.acme.lending.v1.SimulateLoanApplicationResponse;
import com.acme.lending.v1.SimulateLoanRequest;
import com.acme.lending.v1.SimulateLoanResponse;
import com.acme.lending.v1.ValidateRepaymentRequest;
import com.acme.lending.v1.ValidateRepaymentResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class RepaymentValidationApplicationTest {
    @Test
    void previewRepaymentPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LendingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollateral(String tenant, Collateral value) {}
            public void saveLoan(String tenant, Loan value) {}
            public void saveLoanApplication(String tenant, LoanApplication value) {}
            public void saveRepayment(String tenant, Repayment value) {}
        };
        var listener = new RepaymentValidationApplicationEvents(CollateralCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof RepaymentValidationApplication.GetRepaymentRulesStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new RepaymentValidationApplication(
                RetentionPolicySyncServiceGrpc.newBlockingStub(channel),
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                StandingOrderValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                UserQueryServiceGrpc.newBlockingStub(channel),
                PolicyCalculationServiceGrpc.newBlockingStub(channel),
                IncidentCalculationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodHistoryServiceGrpc.newBlockingStub(channel),
                CollateralCalculationServiceGrpc.newBlockingStub(channel),
                CollateralValidationServiceGrpc.newBlockingStub(channel),
                LoanApplicationCalculationServiceGrpc.newBlockingStub(channel),
                LoanApplicationValidationServiceGrpc.newBlockingStub(channel),
                LoanValidationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(PreviewRepaymentRequest.getDefaultInstance());
        var response = application.previewRepayment(request);
        var sent0 = channel.request("acme.authorization.v1.AccessPolicyQueryService/ExportAccessPolicies");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "repayment", "location", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        var sent1 = channel.request("acme.lending.v1.LoanValidationService/PreviewLoan");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "repayment", "organizer", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "phone"),
                ProbeSupport.value(sent1, "loan", "contact", "phone"));
        var sent2 = channel.request("acme.itsm.v1.IncidentCalculationService/RecalculateIncidents");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "repayment", "participants", "contact", "email"),
                ProbeSupport.value(sent2, "incident", "customer", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "repayment", "participants", "contact", "name"),
                ProbeSupport.value(sent2, "incident", "requester", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "repayment", "participants", "contact", "phone"),
                ProbeSupport.value(sent2, "incident", "requester", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "repayment", "participants", "contact", "preferred_channel"),
                ProbeSupport.value(sent2, "incident", "requester", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanValidationService/PreviewLoan"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "repayment_id"),
                ProbeSupport.value(response, "repayment", "repayment_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "repayment", "window"),
                ProbeSupport.value(response, "repayment", "window"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new RepaymentValidationApplicationEvents(CollateralCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<SimulateCollateralResponse>();
        listener.on(new RepaymentValidationApplication.GetRepaymentRulesStage0Requested(ProbeSupport.sample(SimulateCollateralRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
