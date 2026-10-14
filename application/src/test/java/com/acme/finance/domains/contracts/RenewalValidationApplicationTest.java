package com.acme.finance.domains.contracts;

import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.CloneAccessPolicyRequest;
import com.acme.authorization.v1.CloneAccessPolicyResponse;
import com.acme.budgeting.v1.QuoteVarianceRequest;
import com.acme.budgeting.v1.QuoteVarianceResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceCalculationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.CheckRenewalEligibilityRequest;
import com.acme.contracts.v1.CheckRenewalEligibilityResponse;
import com.acme.contracts.v1.CompareObligationOptionsRequest;
import com.acme.contracts.v1.CompareObligationOptionsResponse;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.EstimateObligationRequest;
import com.acme.contracts.v1.EstimateObligationResponse;
import com.acme.contracts.v1.ExplainObligationCalculationRequest;
import com.acme.contracts.v1.ExplainObligationCalculationResponse;
import com.acme.contracts.v1.ExplainRenewalDecisionRequest;
import com.acme.contracts.v1.ExplainRenewalDecisionResponse;
import com.acme.contracts.v1.GetRenewalRulesRequest;
import com.acme.contracts.v1.GetRenewalRulesResponse;
import com.acme.contracts.v1.NormalizeRenewalRequest;
import com.acme.contracts.v1.NormalizeRenewalResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationCalculationServiceGrpc;
import com.acme.contracts.v1.ObligationComment;
import com.acme.contracts.v1.ObligationSla;
import com.acme.contracts.v1.PreviewRenewalRequest;
import com.acme.contracts.v1.PreviewRenewalResponse;
import com.acme.contracts.v1.QuoteObligationRequest;
import com.acme.contracts.v1.QuoteObligationResponse;
import com.acme.contracts.v1.RecalculateObligationsRequest;
import com.acme.contracts.v1.RecalculateObligationsResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.ScoreRenewalRequest;
import com.acme.contracts.v1.ScoreRenewalResponse;
import com.acme.contracts.v1.SimulateObligationRequest;
import com.acme.contracts.v1.SimulateObligationResponse;
import com.acme.contracts.v1.ValidateRenewalRequest;
import com.acme.contracts.v1.ValidateRenewalResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.featurestore.v1.CountFeatureViewsRequest;
import com.acme.featurestore.v1.CountFeatureViewsResponse;
import com.acme.featurestore.v1.FeatureViewQueryServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.ListUsersRequest;
import com.acme.identity.v1.ListUsersResponse;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.SimulateLoanRequest;
import com.acme.lending.v1.SimulateLoanResponse;
import com.acme.notifications.v1.CloneNotificationRequest;
import com.acme.notifications.v1.CloneNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.risk.v1.CountRiskScenariosRequest;
import com.acme.risk.v1.CountRiskScenariosResponse;
import com.acme.risk.v1.RiskScenarioQueryServiceGrpc;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class RenewalValidationApplicationTest {
    @Test
    void previewRenewalPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ContractsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveContract(String tenant, Contract value) {}
            public void saveContractClause(String tenant, ContractClause value) {}
            public void saveObligation(String tenant, Obligation value) {}
            public void saveRenewal(String tenant, Renewal value) {}
        };
        var listener = new RenewalValidationApplicationEvents(VarianceCalculationServiceGrpc.newStub(channel), UserServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof RenewalValidationApplication.ValidateRenewalStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof RenewalValidationApplication.PreviewRenewalStage3Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new RenewalValidationApplication(
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ObligationCalculationServiceGrpc.newBlockingStub(channel),
                events,
                FeatureViewQueryServiceGrpc.newBlockingStub(channel),
                LoanCalculationServiceGrpc.newBlockingStub(channel),
                NotificationServiceGrpc.newBlockingStub(channel),
                repository,
                RiskScenarioQueryServiceGrpc.newBlockingStub(channel),
                Runnable::run);
        var request = ProbeSupport.sample(PreviewRenewalRequest.getDefaultInstance());
        var response = application.previewRenewal(request);
        var sent0 = channel.request("acme.contracts.v1.ObligationCalculationService/EstimateObligation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "renewal", "audit", "updated_by"),
                ProbeSupport.value(sent0, "obligation", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "renewal", "organizer", "email"),
                ProbeSupport.value(sent0, "obligation", "assignee", "email"));
        var sent1 = channel.request("acme.notifications.v1.NotificationService/CloneNotification");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationCalculationService/EstimateObligation"), "obligation", "audit", "created_at"),
                ProbeSupport.value(sent1, "notification", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationCalculationService/EstimateObligation"), "obligation", "requester", "preferred_channel"),
                ProbeSupport.value(sent1, "notification", "requester", "preferred_channel"));
        var sent2 = channel.request("acme.contracts.v1.ObligationCalculationService/ExplainObligationCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationCalculationService/EstimateObligation"), "obligation", "requester", "preferred_channel"),
                ProbeSupport.value(sent2, "details", "contact", "preferred_channel"));
        var sent3 = channel.request("acme.identity.v1.UserService/ListUsers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "renewal", "location", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationCalculationService/EstimateObligation"), "obligation", "audit", "created_at"),
                ProbeSupport.value(sent3, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationService/CloneNotification"), "notification", "sla", "policy_id"),
                ProbeSupport.value(sent3, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationCalculationService/EstimateObligation"), "obligation", "assignee", "display_name"),
                ProbeSupport.value(response, "renewal", "organizer", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationService/CloneNotification"), "notification", "audit", "created_by"),
                ProbeSupport.value(response, "renewal", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationService/CloneNotification"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationCalculationService/ExplainObligationCalculation"), "outcome", "reference_id"),
                ProbeSupport.value(response, "outcome", "reference_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserService/ListUsers"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "renewal_id"),
                ProbeSupport.value(response, "renewal", "renewal_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "renewal", "window"),
                ProbeSupport.value(response, "renewal", "window"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new RenewalValidationApplicationEvents(VarianceCalculationServiceGrpc.newStub(channel), UserServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<QuoteVarianceResponse>();
        listener.on(new RenewalValidationApplication.ValidateRenewalStage0Requested(ProbeSupport.sample(QuoteVarianceRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<ListUsersResponse>();
        listener.on(new RenewalValidationApplication.PreviewRenewalStage3Requested(ProbeSupport.sample(ListUsersRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
