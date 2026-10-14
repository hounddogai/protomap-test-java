package com.acme.finance.domains.financeportal;

import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.SubscribeAuditTrailUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditTrailUpdatesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.CreateAccessPolicyRequest;
import com.acme.authorization.v1.CreateAccessPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CommentHouseholdApprovalRequest;
import com.acme.customers.v1.CommentHouseholdApprovalResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.Household;
import com.acme.driverapp.v1.CreateDriverHomeRequest;
import com.acme.driverapp.v1.CreateDriverHomeResponse;
import com.acme.driverapp.v1.DriverHome;
import com.acme.driverapp.v1.DriverHomeServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.financeportal.v1.AdvanceCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.AdvanceCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.ApprovalInboxAttachment;
import com.acme.financeportal.v1.ApprovalInboxBankSettlement;
import com.acme.financeportal.v1.ApprovalInboxLine;
import com.acme.financeportal.v1.ApprovalInboxLineTax;
import com.acme.financeportal.v1.ApprovalInboxWorkflowServiceGrpc;
import com.acme.financeportal.v1.CancelApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.CancelApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.CancelCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.CancelCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.FinanceWorkspace;
import com.acme.financeportal.v1.InspectCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.InspectCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.ListApprovalInboxWorkflowsRequest;
import com.acme.financeportal.v1.ListApprovalInboxWorkflowsResponse;
import com.acme.financeportal.v1.ListCloseChecklistWorkflowsRequest;
import com.acme.financeportal.v1.ListCloseChecklistWorkflowsResponse;
import com.acme.financeportal.v1.PauseApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.PauseApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.PauseCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.PauseCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.ResumeApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.ResumeApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.ResumeCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.ResumeCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.RetryApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.RetryApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.RetryCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.RetryCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.SignalApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.SignalApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.SignalCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.SignalCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.StartCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.StartCloseChecklistWorkflowResponse;
import com.acme.identity.v1.FindUsersRequest;
import com.acme.identity.v1.FindUsersResponse;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.RejectClaimRequest;
import com.acme.insurance.v1.RejectClaimResponse;
import com.acme.ledger.v1.CompareFiscalPeriodVersionsRequest;
import com.acme.ledger.v1.CompareFiscalPeriodVersionsResponse;
import com.acme.ledger.v1.ExportJournalEntriesRequest;
import com.acme.ledger.v1.ExportJournalEntriesResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodParticipant;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.metrics.v1.CountMetricSeriesesRequest;
import com.acme.metrics.v1.CountMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.suppliers.v1.SubmitSupplierContractRequest;
import com.acme.suppliers.v1.SubmitSupplierContractResponse;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierContractLine;
import com.acme.suppliers.v1.SupplierContractLineTax;
import com.google.protobuf.Any;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class CloseChecklistWorkflowApplicationTest {
    @Test
    void retryCloseChecklistWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var application = new CloseChecklistWorkflowApplication(
                AuditTrailNotificationServiceGrpc.newBlockingStub(channel),
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                DriverHomeServiceGrpc.newBlockingStub(channel),
                ApprovalInboxWorkflowServiceGrpc.newBlockingStub(channel),
                FiscalPeriodHistoryServiceGrpc.newBlockingStub(channel),
                JournalEntryQueryServiceGrpc.newBlockingStub(channel),
                new CloseChecklistWorkflowRequestMapperImpl(),
                MetricSeriesQueryServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierContractApprovalServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RetryCloseChecklistWorkflowRequest.getDefaultInstance());
        var response = application.retryCloseChecklistWorkflow(request);
        var sent0 = channel.request("acme.financeportal.v1.ApprovalInboxWorkflowService/RetryApprovalInboxWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "close_checklist", "attachments", "content_type"),
                ProbeSupport.value(sent0, "approval_inbox", "attachments", "content_type"));
        var sent1 = channel.request("acme.financeportal.v1.ApprovalInboxWorkflowService/SignalApprovalInboxWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.ApprovalInboxWorkflowService/RetryApprovalInboxWorkflow"), "approval_inbox", "bank", "iban"),
                ProbeSupport.value(sent1, "approval_inbox", "bank", "iban"));
        var sent2 = channel.request("acme.financeportal.v1.ApprovalInboxWorkflowService/CancelApprovalInboxWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "close_checklist", "lines", "unit_price"),
                ProbeSupport.value(sent2, "approval_inbox", "lines", "unit_price"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "close_checklist", "lines", "tax", "jurisdiction"),
                ProbeSupport.value(sent2, "approval_inbox", "lines", "tax", "jurisdiction"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "close_checklist", "lines", "description"),
                ProbeSupport.value(sent2, "approval_inbox", "lines", "description"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "close_checklist", "lines", "attributes"),
                ProbeSupport.value(sent2, "approval_inbox", "lines", "attributes"));
        var sent3 = channel.request("acme.suppliers.v1.SupplierContractApprovalService/SubmitSupplierContract");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "close_checklist", "lines", "tax", "rate"),
                ProbeSupport.value(sent3, "supplier_contract", "lines", "tax", "rate"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.ApprovalInboxWorkflowService/SignalApprovalInboxWorkflow"), "approval_inbox", "billing_address", "region"),
                ProbeSupport.value(response, "close_checklist", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.ApprovalInboxWorkflowService/SignalApprovalInboxWorkflow"), "approval_inbox", "issued_at"),
                ProbeSupport.value(response, "close_checklist", "issued_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "close_checklist_id"),
                ProbeSupport.value(response, "close_checklist", "close_checklist_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "close_checklist", "notes"),
                ProbeSupport.value(response, "close_checklist", "notes"));
    }
}
