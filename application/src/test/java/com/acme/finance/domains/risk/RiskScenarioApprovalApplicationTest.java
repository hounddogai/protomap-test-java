package com.acme.finance.domains.risk;

import com.acme.appointments.v1.AppointmentServiceGrpc;
import com.acme.appointments.v1.ListAppointmentsRequest;
import com.acme.appointments.v1.ListAppointmentsResponse;
import com.acme.banking.v1.ResumeStatementWorkflowRequest;
import com.acme.banking.v1.ResumeStatementWorkflowResponse;
import com.acme.banking.v1.RetryStandingOrderWorkflowRequest;
import com.acme.banking.v1.RetryStandingOrderWorkflowResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderRef;
import com.acme.banking.v1.StandingOrderWorkflowServiceGrpc;
import com.acme.banking.v1.StartStandingOrderWorkflowRequest;
import com.acme.banking.v1.StartStandingOrderWorkflowResponse;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.StatementWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CountHouseholdsRequest;
import com.acme.customers.v1.CountHouseholdsResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.LintCustomersRequest;
import com.acme.customers.v1.LintCustomersResponse;
import com.acme.featureflags.v1.BatchCreateFeatureFlagsRequest;
import com.acme.featureflags.v1.BatchCreateFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.identity.v1.WatchUsersRequest;
import com.acme.identity.v1.WatchUsersResponse;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.insurance.v1.SharePremiumReportRequest;
import com.acme.insurance.v1.SharePremiumReportResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationTemplateRef;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.ValidateNotificationRequest;
import com.acme.notifications.v1.ValidateNotificationResponse;
import com.acme.risk.v1.ApproveRiskScenarioRequest;
import com.acme.risk.v1.ApproveRiskScenarioResponse;
import com.acme.risk.v1.CheckStressTestEligibilityRequest;
import com.acme.risk.v1.CheckStressTestEligibilityResponse;
import com.acme.risk.v1.CommentRiskScenarioApprovalRequest;
import com.acme.risk.v1.CommentRiskScenarioApprovalResponse;
import com.acme.risk.v1.DelegateRiskScenarioApprovalRequest;
import com.acme.risk.v1.DelegateRiskScenarioApprovalResponse;
import com.acme.risk.v1.EscalateRiskScenarioRequest;
import com.acme.risk.v1.EscalateRiskScenarioResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.GetRiskScenarioRulesRequest;
import com.acme.risk.v1.GetRiskScenarioRulesResponse;
import com.acme.risk.v1.ListPendingRiskRegistersRequest;
import com.acme.risk.v1.ListPendingRiskRegistersResponse;
import com.acme.risk.v1.ListPendingRiskScenariosRequest;
import com.acme.risk.v1.ListPendingRiskScenariosResponse;
import com.acme.risk.v1.RecallRiskRegisterRequest;
import com.acme.risk.v1.RecallRiskRegisterResponse;
import com.acme.risk.v1.RecallRiskScenarioRequest;
import com.acme.risk.v1.RecallRiskScenarioResponse;
import com.acme.risk.v1.RejectRiskScenarioRequest;
import com.acme.risk.v1.RejectRiskScenarioResponse;
import com.acme.risk.v1.ResolveRiskScenariosRequest;
import com.acme.risk.v1.ResolveRiskScenariosResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterApprovalServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioComment;
import com.acme.risk.v1.RiskScenarioQueryServiceGrpc;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestValidationServiceGrpc;
import com.acme.risk.v1.SubmitRiskScenarioRequest;
import com.acme.risk.v1.SubmitRiskScenarioResponse;
import com.acme.suppliers.v1.BrowseSupplierContractsRequest;
import com.acme.suppliers.v1.BrowseSupplierContractsResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskComment;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.acme.suppliers.v1.ValidateSupplierRiskRequest;
import com.acme.suppliers.v1.ValidateSupplierRiskResponse;
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

class RiskScenarioApprovalApplicationTest {
    @Test
    void recallRiskScenarioPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new RiskRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExposure(String tenant, Exposure value) {}
            public void saveRiskRegister(String tenant, RiskRegister value) {}
            public void saveRiskScenario(String tenant, RiskScenario value) {}
            public void saveStressTest(String tenant, StressTest value) {}
        };
        var application = new RiskScenarioApprovalApplication(
                AppointmentServiceGrpc.newBlockingStub(channel),
                StandingOrderWorkflowServiceGrpc.newBlockingStub(channel),
                StatementWorkflowServiceGrpc.newBlockingStub(channel),
                CustomerValidationServiceGrpc.newBlockingStub(channel),
                HouseholdQueryServiceGrpc.newBlockingStub(channel),
                FeatureFlagServiceGrpc.newStub(channel),
                UserServiceGrpc.newBlockingStub(channel),
                PremiumReportingServiceGrpc.newBlockingStub(channel),
                NotificationValidationServiceGrpc.newBlockingStub(channel),
                repository,
                RiskRegisterApprovalServiceGrpc.newBlockingStub(channel),
                RiskScenarioQueryServiceGrpc.newBlockingStub(channel),
                RiskScenarioValidationServiceGrpc.newBlockingStub(channel),
                StressTestValidationServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SupplierContractQueryServiceGrpc.newBlockingStub(channel),
                SupplierRiskValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RecallRiskScenarioRequest.getDefaultInstance());
        var response = application.recallRiskScenario(request);
        var sent0 = channel.request("acme.risk.v1.StressTestValidationService/CheckStressTestEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario", "assignee", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario", "customer", "email"),
                ProbeSupport.value(sent0, "stress_test", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario", "customer", "customer_id"),
                ProbeSupport.value(sent0, "stress_test", "counterparty", "customer_id"));
        var sent1 = channel.request("acme.banking.v1.StandingOrderWorkflowService/RetryStandingOrderWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario", "requester", "name"),
                ProbeSupport.value(sent1, "standing_order", "participants", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario", "assignee", "email"),
                ProbeSupport.value(sent1, "standing_order", "participants", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario", "requester", "preferred_channel"),
                ProbeSupport.value(sent1, "standing_order", "participants", "contact", "preferred_channel"));
        var sent2 = channel.request("acme.suppliers.v1.SupplierContractQueryService/BrowseSupplierContracts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.StressTestValidationService/CheckStressTestEligibility"), "stress_test", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        var sent3 = channel.request("acme.notifications.v1.NotificationValidationService/ValidateNotification");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.StressTestValidationService/CheckStressTestEligibility"), "stress_test", "counterparty", "email"),
                ProbeSupport.value(sent3, "notification", "requester", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.StressTestValidationService/CheckStressTestEligibility"), "stress_test", "counterparty", "display_name"),
                ProbeSupport.value(sent3, "notification", "customer", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.StressTestValidationService/CheckStressTestEligibility"), "stress_test", "audit", "etag"),
                ProbeSupport.value(sent3, "notification", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.StressTestValidationService/CheckStressTestEligibility"), "outcome", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderWorkflowService/RetryStandingOrderWorkflow"), "standing_order", "audit", "updated_at"),
                ProbeSupport.value(response, "risk_scenario", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierContractQueryService/BrowseSupplierContracts"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationValidationService/ValidateNotification"), "notification", "requester", "email"),
                ProbeSupport.value(response, "risk_scenario", "requester", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario_id"),
                ProbeSupport.value(response, "risk_scenario", "risk_scenario_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario", "status"),
                ProbeSupport.value(response, "risk_scenario", "status"));
    }
}
