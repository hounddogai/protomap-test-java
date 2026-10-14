package com.acme.finance.domains.financeportal;

import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetApprovalServiceGrpc;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.EscalateBudgetRequest;
import com.acme.budgeting.v1.EscalateBudgetResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegmentApprovalServiceGrpc;
import com.acme.customers.v1.ListPendingCustomerSegmentsRequest;
import com.acme.customers.v1.ListPendingCustomerSegmentsResponse;
import com.acme.customers.v1.PreferenceReportingServiceGrpc;
import com.acme.customers.v1.SchedulePreferenceReportRequest;
import com.acme.customers.v1.SchedulePreferenceReportResponse;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.PreviewDocumentRequest;
import com.acme.documents.v1.PreviewDocumentResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventRequest;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventResponse;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.CancelCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.CancelCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.CloseChecklistBankSettlement;
import com.acme.financeportal.v1.CloseChecklistCardSettlement;
import com.acme.financeportal.v1.CloseChecklistRef;
import com.acme.financeportal.v1.CloseChecklistWorkflowServiceGrpc;
import com.acme.financeportal.v1.ConnectApprovalInboxSourceRequest;
import com.acme.financeportal.v1.ConnectApprovalInboxSourceResponse;
import com.acme.financeportal.v1.DisconnectApprovalInboxSourceRequest;
import com.acme.financeportal.v1.DisconnectApprovalInboxSourceResponse;
import com.acme.financeportal.v1.DisconnectFinanceWorkspaceSourceRequest;
import com.acme.financeportal.v1.DisconnectFinanceWorkspaceSourceResponse;
import com.acme.financeportal.v1.FinanceWorkspace;
import com.acme.financeportal.v1.FinanceWorkspaceIntegrationServiceGrpc;
import com.acme.financeportal.v1.IngestApprovalInboxEventsRequest;
import com.acme.financeportal.v1.IngestApprovalInboxEventsResponse;
import com.acme.financeportal.v1.InspectApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.InspectApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.ListApprovalInboxSourcesRequest;
import com.acme.financeportal.v1.ListApprovalInboxSourcesResponse;
import com.acme.financeportal.v1.ListCloseChecklistWorkflowsRequest;
import com.acme.financeportal.v1.ListCloseChecklistWorkflowsResponse;
import com.acme.financeportal.v1.PauseCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.PauseCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.RelayApprovalInboxMessagesRequest;
import com.acme.financeportal.v1.RelayApprovalInboxMessagesResponse;
import com.acme.financeportal.v1.ReplayApprovalInboxEventsRequest;
import com.acme.financeportal.v1.ReplayApprovalInboxEventsResponse;
import com.acme.financeportal.v1.ResumeCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.ResumeCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.SignalCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.SignalCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.StartCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.StartCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.TestApprovalInboxConnectionRequest;
import com.acme.financeportal.v1.TestApprovalInboxConnectionResponse;
import com.acme.financeportal.v1.TestFinanceWorkspaceConnectionRequest;
import com.acme.financeportal.v1.TestFinanceWorkspaceConnectionResponse;
import com.acme.identity.v1.UserRef;
import com.acme.payroll.v1.AdvancePayStubWorkflowRequest;
import com.acme.payroll.v1.AdvancePayStubWorkflowResponse;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubWorkflowServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioWorkflowServiceGrpc;
import com.acme.risk.v1.StartRiskScenarioWorkflowRequest;
import com.acme.risk.v1.StartRiskScenarioWorkflowResponse;
import com.acme.webbff.v1.BatchGetPageCompositionsRequest;
import com.acme.webbff.v1.BatchGetPageCompositionsResponse;
import com.acme.webbff.v1.PageCompositionServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class ApprovalInboxIntegrationApplicationTest {
    @Test
    void ingestApprovalInboxEventsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var application = new ApprovalInboxIntegrationApplication(
                BudgetApprovalServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerSegmentApprovalServiceGrpc.newBlockingStub(channel),
                PreferenceReportingServiceGrpc.newBlockingStub(channel),
                DocumentValidationServiceGrpc.newBlockingStub(channel),
                CloseChecklistWorkflowServiceGrpc.newBlockingStub(channel),
                FinanceWorkspaceIntegrationServiceGrpc.newBlockingStub(channel),
                new ApprovalInboxIntegrationRequestMapperImpl(),
                PayStubWorkflowServiceGrpc.newBlockingStub(channel),
                repository,
                RiskScenarioWorkflowServiceGrpc.newBlockingStub(channel),
                PageCompositionServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(IngestApprovalInboxEventsRequest.getDefaultInstance());
        var response = application.ingestApprovalInboxEvents(request);
        var sent0 = channel.request("acme.financeportal.v1.CloseChecklistWorkflowService/SignalCloseChecklistWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "billing_address", "locality"),
                ProbeSupport.value(sent0, "close_checklist", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "billing_address", "lines"),
                ProbeSupport.value(sent0, "close_checklist", "billing_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "billing_address", "country_code"),
                ProbeSupport.value(sent0, "close_checklist", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "bank", "account_holder"),
                ProbeSupport.value(sent0, "close_checklist", "bank", "account_holder"));
        var sent1 = channel.request("acme.financeportal.v1.CloseChecklistWorkflowService/StartCloseChecklistWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistWorkflowService/SignalCloseChecklistWorkflow"), "close_checklist", "billing_address", "recipient"),
                ProbeSupport.value(sent1, "close_checklist", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "card", "holder_name"),
                ProbeSupport.value(sent1, "close_checklist", "card", "holder_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "created_at"),
                ProbeSupport.value(sent1, "close_checklist", "audit", "created_at"));
        var sent2 = channel.request("acme.financeportal.v1.CloseChecklistWorkflowService/CancelCloseChecklistWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "card", "holder_name"),
                ProbeSupport.value(sent2, "close_checklist", "card", "holder_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistWorkflowService/SignalCloseChecklistWorkflow"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistWorkflowService/SignalCloseChecklistWorkflow"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistWorkflowService/CancelCloseChecklistWorkflow"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        var endpoint = new ApprovalInboxIntegrationEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<IngestApprovalInboxEventsResponse> observer = new StreamObserver<>() {
            public void onNext(IngestApprovalInboxEventsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.ingestApprovalInboxEvents(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void relayApprovalInboxMessagesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var application = new ApprovalInboxIntegrationApplication(
                BudgetApprovalServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerSegmentApprovalServiceGrpc.newBlockingStub(channel),
                PreferenceReportingServiceGrpc.newBlockingStub(channel),
                DocumentValidationServiceGrpc.newBlockingStub(channel),
                CloseChecklistWorkflowServiceGrpc.newBlockingStub(channel),
                FinanceWorkspaceIntegrationServiceGrpc.newBlockingStub(channel),
                new ApprovalInboxIntegrationRequestMapperImpl(),
                PayStubWorkflowServiceGrpc.newBlockingStub(channel),
                repository,
                RiskScenarioWorkflowServiceGrpc.newBlockingStub(channel),
                PageCompositionServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RelayApprovalInboxMessagesRequest.getDefaultInstance());
        var response = application.relayApprovalInboxMessages(request);
        var sent0 = channel.request("acme.financeportal.v1.FinanceWorkspaceIntegrationService/TestFinanceWorkspaceConnection");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "audit", "created_at"),
                ProbeSupport.value(sent0, "details", "received_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "counterparty", "close_checklist_id"),
                ProbeSupport.value(sent0, "details", "message_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "metadata"),
                ProbeSupport.value(sent0, "annotations"));
        var sent1 = channel.request("acme.budgeting.v1.BudgetApprovalService/EscalateBudget");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/TestFinanceWorkspaceConnection"), "finance_workspace", "assignee", "user_id"),
                ProbeSupport.value(sent1, "details", "approver", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "card", "token"),
                ProbeSupport.value(sent1, "budget", "card", "token"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/TestFinanceWorkspaceConnection"), "finance_workspace", "requester", "email"),
                ProbeSupport.value(sent1, "details", "approver", "email"));
        var sent2 = channel.request("acme.risk.v1.RiskScenarioWorkflowService/StartRiskScenarioWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/TestFinanceWorkspaceConnection"), "finance_workspace", "audit", "created_at"),
                ProbeSupport.value(sent2, "risk_scenario", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetApprovalService/EscalateBudget"), "budget", "contact", "email"),
                ProbeSupport.value(sent2, "details", "notify", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/TestFinanceWorkspaceConnection"), "finance_workspace", "audit", "created_at"),
                ProbeSupport.value(response, "approval_inbox", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/TestFinanceWorkspaceConnection"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetApprovalService/EscalateBudget"), "budget", "audit", "created_by"),
                ProbeSupport.value(response, "approval_inbox", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "billing_address"),
                ProbeSupport.value(response, "approval_inbox", "billing_address"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "approval_inbox_id"),
                ProbeSupport.value(response, "approval_inbox", "approval_inbox_id"));
        var endpoint = new ApprovalInboxIntegrationEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<RelayApprovalInboxMessagesResponse> observer = new StreamObserver<>() {
            public void onNext(RelayApprovalInboxMessagesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.relayApprovalInboxMessages(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "value", "completed"), signals);
    }

    @Test
    void testApprovalInboxConnectionPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var application = new ApprovalInboxIntegrationApplication(
                BudgetApprovalServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerSegmentApprovalServiceGrpc.newBlockingStub(channel),
                PreferenceReportingServiceGrpc.newBlockingStub(channel),
                DocumentValidationServiceGrpc.newBlockingStub(channel),
                CloseChecklistWorkflowServiceGrpc.newBlockingStub(channel),
                FinanceWorkspaceIntegrationServiceGrpc.newBlockingStub(channel),
                new ApprovalInboxIntegrationRequestMapperImpl(),
                PayStubWorkflowServiceGrpc.newBlockingStub(channel),
                repository,
                RiskScenarioWorkflowServiceGrpc.newBlockingStub(channel),
                PageCompositionServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(TestApprovalInboxConnectionRequest.getDefaultInstance());
        var response = application.testApprovalInboxConnection(request);
        var sent0 = channel.request("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "source_system"),
                ProbeSupport.value(sent0, "details", "source_system"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "endpoint"),
                ProbeSupport.value(sent0, "details", "endpoint"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "message_id"),
                ProbeSupport.value(sent0, "details", "message_id"));
        var sent1 = channel.request("acme.financeportal.v1.CloseChecklistWorkflowService/PauseCloseChecklistWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent"), "finance_workspace", "assignee", "email"),
                ProbeSupport.value(sent1, "details", "notify", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent"), "finance_workspace", "audit", "updated_by"),
                ProbeSupport.value(sent1, "close_checklist", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent"), "finance_workspace", "requester", "phone"),
                ProbeSupport.value(sent1, "details", "notify", "phone"));
        var sent2 = channel.request("acme.payroll.v1.PayStubWorkflowService/AdvancePayStubWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent"), "finance_workspace", "requester", "preferred_channel"),
                ProbeSupport.value(sent2, "details", "notify", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistWorkflowService/PauseCloseChecklistWorkflow"), "close_checklist", "counterparty", "display_name"),
                ProbeSupport.value(sent2, "pay_stub", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent"), "finance_workspace", "audit", "etag"),
                ProbeSupport.value(response, "approval_inbox", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistWorkflowService/PauseCloseChecklistWorkflow"), "outcome", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistWorkflowService/PauseCloseChecklistWorkflow"), "close_checklist", "close_checklist_id"),
                ProbeSupport.value(response, "approval_inbox", "counterparty", "close_checklist_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubWorkflowService/AdvancePayStubWorkflow"), "pay_stub", "audit", "created_at"),
                ProbeSupport.value(response, "approval_inbox", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "approval_inbox_id"),
                ProbeSupport.value(response, "approval_inbox", "approval_inbox_id"));
    }
}
