package com.acme.finance.domains.financeportal;

import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationApprovalServiceGrpc;
import com.acme.accounting.v1.AllocationBlockAction;
import com.acme.accounting.v1.RecallAllocationRequest;
import com.acme.accounting.v1.RecallAllocationResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.ExplainAccessPolicyDecisionRequest;
import com.acme.authorization.v1.ExplainAccessPolicyDecisionResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseSla;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.collections.v1.PauseCollectionCaseWorkflowRequest;
import com.acme.collections.v1.PauseCollectionCaseWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.ListAttestationReportsRequest;
import com.acme.compliance.v1.ListAttestationReportsResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordWorkflowServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.StartConsentRecordWorkflowRequest;
import com.acme.customers.v1.StartConsentRecordWorkflowResponse;
import com.acme.datahub.v1.ExportWorkspacesRequest;
import com.acme.datahub.v1.ExportWorkspacesResponse;
import com.acme.datahub.v1.WatchWorkspacesRequest;
import com.acme.datahub.v1.WatchWorkspacesResponse;
import com.acme.datahub.v1.WorkspaceQueryServiceGrpc;
import com.acme.datahub.v1.WorkspaceServiceGrpc;
import com.acme.documents.v1.CreateDocumentRequest;
import com.acme.documents.v1.CreateDocumentResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.employeeportal.v1.FindPortalHomesRequest;
import com.acme.employeeportal.v1.FindPortalHomesResponse;
import com.acme.employeeportal.v1.PortalHomeQueryServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.financeportal.v1.AcknowledgeCloseChecklistEventRequest;
import com.acme.financeportal.v1.AcknowledgeCloseChecklistEventResponse;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventRequest;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventResponse;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.ApprovalInboxIntegrationServiceGrpc;
import com.acme.financeportal.v1.ApprovalInboxWorkflowServiceGrpc;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.ConnectApprovalInboxSourceRequest;
import com.acme.financeportal.v1.ConnectApprovalInboxSourceResponse;
import com.acme.financeportal.v1.ConnectCloseChecklistSourceRequest;
import com.acme.financeportal.v1.ConnectCloseChecklistSourceResponse;
import com.acme.financeportal.v1.ConnectFinanceWorkspaceSourceRequest;
import com.acme.financeportal.v1.ConnectFinanceWorkspaceSourceResponse;
import com.acme.financeportal.v1.DisconnectCloseChecklistSourceRequest;
import com.acme.financeportal.v1.DisconnectCloseChecklistSourceResponse;
import com.acme.financeportal.v1.DisconnectFinanceWorkspaceSourceRequest;
import com.acme.financeportal.v1.DisconnectFinanceWorkspaceSourceResponse;
import com.acme.financeportal.v1.FinanceWorkspace;
import com.acme.financeportal.v1.FinanceWorkspaceIntegrationServiceGrpc;
import com.acme.financeportal.v1.IngestCloseChecklistEventsRequest;
import com.acme.financeportal.v1.IngestCloseChecklistEventsResponse;
import com.acme.financeportal.v1.ListCloseChecklistSourcesRequest;
import com.acme.financeportal.v1.ListCloseChecklistSourcesResponse;
import com.acme.financeportal.v1.PauseApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.PauseApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.RelayCloseChecklistMessagesRequest;
import com.acme.financeportal.v1.RelayCloseChecklistMessagesResponse;
import com.acme.financeportal.v1.ReplayApprovalInboxEventsRequest;
import com.acme.financeportal.v1.ReplayApprovalInboxEventsResponse;
import com.acme.financeportal.v1.ReplayCloseChecklistEventsRequest;
import com.acme.financeportal.v1.ReplayCloseChecklistEventsResponse;
import com.acme.financeportal.v1.ReplayFinanceWorkspaceEventsRequest;
import com.acme.financeportal.v1.ReplayFinanceWorkspaceEventsResponse;
import com.acme.financeportal.v1.TestApprovalInboxConnectionRequest;
import com.acme.financeportal.v1.TestApprovalInboxConnectionResponse;
import com.acme.financeportal.v1.TestCloseChecklistConnectionRequest;
import com.acme.financeportal.v1.TestCloseChecklistConnectionResponse;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportRequest;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportResponse;
import com.acme.suppliers.v1.SubmitSupplierContractRequest;
import com.acme.suppliers.v1.SubmitSupplierContractResponse;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierContractCardSettlement;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Optional;
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

class CloseChecklistIntegrationApplicationTest {
    @Test
    void connectCloseChecklistSourcePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var application = new CloseChecklistIntegrationApplication(
                AllocationApprovalServiceGrpc.newBlockingStub(channel),
                AccessPolicyValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseWorkflowServiceGrpc.newFutureStub(channel),
                AttestationReportingServiceGrpc.newBlockingStub(channel),
                ConsentRecordWorkflowServiceGrpc.newBlockingStub(channel),
                WorkspaceQueryServiceGrpc.newBlockingStub(channel),
                WorkspaceServiceGrpc.newBlockingStub(channel),
                DocumentServiceGrpc.newBlockingStub(channel),
                PortalHomeQueryServiceGrpc.newFutureStub(channel),
                ApprovalInboxIntegrationServiceGrpc.newBlockingStub(channel),
                ApprovalInboxWorkflowServiceGrpc.newBlockingStub(channel),
                FinanceWorkspaceIntegrationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierContractApprovalServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ConnectCloseChecklistSourceRequest.getDefaultInstance());
        var response = application.connectCloseChecklistSource(request);
        var sent0 = channel.request("acme.accounting.v1.AllocationApprovalService/RecallAllocation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "allocation", "block", "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        var sent1 = channel.request("acme.compliance.v1.AttestationReportingService/ListAttestationReports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationApprovalService/RecallAllocation"), "allocation", "audit", "updated_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        var sent2 = channel.request("acme.financeportal.v1.ApprovalInboxIntegrationService/ReplayApprovalInboxEvents");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationApprovalService/RecallAllocation"), "allocation", "block", "reason"),
                ProbeSupport.value(sent2, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "endpoint"),
                ProbeSupport.value(sent2, "details", "endpoint"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "received_at"),
                ProbeSupport.value(sent2, "details", "received_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "source_system"),
                ProbeSupport.value(sent2, "details", "source_system"));
        var sent3 = channel.request("acme.financeportal.v1.ApprovalInboxIntegrationService/TestApprovalInboxConnection");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationApprovalService/RecallAllocation"), "allocation", "block", "reason"),
                ProbeSupport.value(sent3, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationApprovalService/RecallAllocation"), "outcome", "next_steps"),
                ProbeSupport.value(response, "outcome", "next_steps"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationApprovalService/RecallAllocation"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.AttestationReportingService/ListAttestationReports"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.AttestationReportingService/ListAttestationReports"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.ApprovalInboxIntegrationService/ReplayApprovalInboxEvents"), "approval_inbox", "due_at"),
                ProbeSupport.value(response, "close_checklist", "due_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.ApprovalInboxIntegrationService/TestApprovalInboxConnection"), "approval_inbox", "counterparty", "display_name"),
                ProbeSupport.value(response, "close_checklist", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.ApprovalInboxIntegrationService/TestApprovalInboxConnection"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "close_checklist_id"),
                ProbeSupport.value(response, "close_checklist", "close_checklist_id"));
    }

    @Test
    void ingestCloseChecklistEventsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var application = new CloseChecklistIntegrationApplication(
                AllocationApprovalServiceGrpc.newBlockingStub(channel),
                AccessPolicyValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseWorkflowServiceGrpc.newFutureStub(channel),
                AttestationReportingServiceGrpc.newBlockingStub(channel),
                ConsentRecordWorkflowServiceGrpc.newBlockingStub(channel),
                WorkspaceQueryServiceGrpc.newBlockingStub(channel),
                WorkspaceServiceGrpc.newBlockingStub(channel),
                DocumentServiceGrpc.newBlockingStub(channel),
                PortalHomeQueryServiceGrpc.newFutureStub(channel),
                ApprovalInboxIntegrationServiceGrpc.newBlockingStub(channel),
                ApprovalInboxWorkflowServiceGrpc.newBlockingStub(channel),
                FinanceWorkspaceIntegrationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierContractApprovalServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(IngestCloseChecklistEventsRequest.getDefaultInstance());
        var response = application.ingestCloseChecklistEvents(request);
        var sent0 = channel.request("acme.financeportal.v1.ApprovalInboxWorkflowService/PauseApprovalInboxWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "billing_address", "region"),
                ProbeSupport.value(sent0, "approval_inbox", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "updated_at"),
                ProbeSupport.value(sent0, "approval_inbox", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "billing_address", "postal_code"),
                ProbeSupport.value(sent0, "approval_inbox", "billing_address", "postal_code"));
        var sent1 = channel.request("acme.financeportal.v1.FinanceWorkspaceIntegrationService/ReplayFinanceWorkspaceEvents");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.ApprovalInboxWorkflowService/PauseApprovalInboxWorkflow"), "outcome", "message"),
                ProbeSupport.value(sent1, "details", "message_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.ApprovalInboxWorkflowService/PauseApprovalInboxWorkflow"), "approval_inbox", "audit", "updated_at"),
                ProbeSupport.value(sent1, "details", "received_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "metadata"),
                ProbeSupport.value(sent1, "annotations"));
        var sent2 = channel.request("acme.authorization.v1.AccessPolicyValidationService/ExplainAccessPolicyDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "counterparty", "email"),
                ProbeSupport.value(sent2, "access_policy", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/ReplayFinanceWorkspaceEvents"), "finance_workspace", "audit", "created_by"),
                ProbeSupport.value(sent2, "access_policy", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.ApprovalInboxWorkflowService/PauseApprovalInboxWorkflow"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/ReplayFinanceWorkspaceEvents"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        var endpoint = new CloseChecklistIntegrationEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<IngestCloseChecklistEventsResponse> observer = new StreamObserver<>() {
            public void onNext(IngestCloseChecklistEventsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.ingestCloseChecklistEvents(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void relayCloseChecklistMessagesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var application = new CloseChecklistIntegrationApplication(
                AllocationApprovalServiceGrpc.newBlockingStub(channel),
                AccessPolicyValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseWorkflowServiceGrpc.newFutureStub(channel),
                AttestationReportingServiceGrpc.newBlockingStub(channel),
                ConsentRecordWorkflowServiceGrpc.newBlockingStub(channel),
                WorkspaceQueryServiceGrpc.newBlockingStub(channel),
                WorkspaceServiceGrpc.newBlockingStub(channel),
                DocumentServiceGrpc.newBlockingStub(channel),
                PortalHomeQueryServiceGrpc.newFutureStub(channel),
                ApprovalInboxIntegrationServiceGrpc.newBlockingStub(channel),
                ApprovalInboxWorkflowServiceGrpc.newBlockingStub(channel),
                FinanceWorkspaceIntegrationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierContractApprovalServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RelayCloseChecklistMessagesRequest.getDefaultInstance());
        var response = application.relayCloseChecklistMessages(request);
        var sent0 = channel.request("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "audit", "updated_at"),
                ProbeSupport.value(sent0, "details", "received_at"));
        var sent1 = channel.request("acme.collections.v1.CollectionCaseWorkflowService/PauseCollectionCaseWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent"), "finance_workspace", "sla", "breached"),
                ProbeSupport.value(sent1, "collection_case", "sla", "breached"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent"), "finance_workspace", "audit", "etag"),
                ProbeSupport.value(sent1, "collection_case", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent"), "finance_workspace", "requester", "email"),
                ProbeSupport.value(sent1, "collection_case", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent"), "finance_workspace", "assignee", "email"),
                ProbeSupport.value(response, "close_checklist", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.FinanceWorkspaceIntegrationService/AcknowledgeFinanceWorkspaceEvent"), "finance_workspace", "sla", "due_at"),
                ProbeSupport.value(response, "close_checklist", "due_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.CollectionCaseWorkflowService/PauseCollectionCaseWorkflow"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.CollectionCaseWorkflowService/PauseCollectionCaseWorkflow"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "billing_address"),
                ProbeSupport.value(response, "close_checklist", "billing_address"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "ack", "sequence"),
                ProbeSupport.value(response, "sequence"));
        var endpoint = new CloseChecklistIntegrationEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<RelayCloseChecklistMessagesResponse> observer = new StreamObserver<>() {
            public void onNext(RelayCloseChecklistMessagesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.relayCloseChecklistMessages(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "value", "completed"), signals);
    }
}
