package com.acme.finance.domains.expenses;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.PullAssetTransfersRequest;
import com.acme.assets.v1.PullAssetTransfersResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.collections.v1.AdvanceCollectionCaseWorkflowRequest;
import com.acme.collections.v1.AdvanceCollectionCaseWorkflowResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.expenses.v1.AdvanceReceiptWorkflowRequest;
import com.acme.expenses.v1.AdvanceReceiptWorkflowResponse;
import com.acme.expenses.v1.ApproveExpenseReportRequest;
import com.acme.expenses.v1.ApproveExpenseReportResponse;
import com.acme.expenses.v1.CommentExpenseReportApprovalRequest;
import com.acme.expenses.v1.CommentExpenseReportApprovalResponse;
import com.acme.expenses.v1.CountExpensePolicyTotalsRequest;
import com.acme.expenses.v1.CountExpensePolicyTotalsResponse;
import com.acme.expenses.v1.CountMileageClaimTotalsRequest;
import com.acme.expenses.v1.CountMileageClaimTotalsResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyReportingServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportApprovalServiceGrpc;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExpenseReportLineTax;
import com.acme.expenses.v1.ExpireReceiptRequest;
import com.acme.expenses.v1.ExpireReceiptResponse;
import com.acme.expenses.v1.GenerateExpensePolicyReportRequest;
import com.acme.expenses.v1.GenerateExpensePolicyReportResponse;
import com.acme.expenses.v1.InspectReceiptWorkflowRequest;
import com.acme.expenses.v1.InspectReceiptWorkflowResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimLine;
import com.acme.expenses.v1.MileageClaimLineTax;
import com.acme.expenses.v1.MileageClaimReportingServiceGrpc;
import com.acme.expenses.v1.MileageClaimValidationServiceGrpc;
import com.acme.expenses.v1.PauseReceiptWorkflowRequest;
import com.acme.expenses.v1.PauseReceiptWorkflowResponse;
import com.acme.expenses.v1.PreviewMileageClaimRequest;
import com.acme.expenses.v1.PreviewMileageClaimResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptLifecycleServiceGrpc;
import com.acme.expenses.v1.RetryReceiptWorkflowRequest;
import com.acme.expenses.v1.RetryReceiptWorkflowResponse;
import com.acme.expenses.v1.SignalReceiptWorkflowRequest;
import com.acme.expenses.v1.SignalReceiptWorkflowResponse;
import com.acme.expenses.v1.StartReceiptWorkflowRequest;
import com.acme.expenses.v1.StartReceiptWorkflowResponse;
import com.acme.expenses.v1.TerminateReceiptRequest;
import com.acme.expenses.v1.TerminateReceiptResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.CompareJournalEntryVersionsRequest;
import com.acme.ledger.v1.CompareJournalEntryVersionsResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryAttachment;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class ReceiptWorkflowApplicationTest {
    @Test
    void signalReceiptWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var application = new ReceiptWorkflowApplication(
                AssetTransferSyncServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseWorkflowServiceGrpc.newBlockingStub(channel),
                ExpensePolicyReportingServiceGrpc.newBlockingStub(channel),
                ExpenseReportApprovalServiceGrpc.newBlockingStub(channel),
                MileageClaimReportingServiceGrpc.newBlockingStub(channel),
                MileageClaimValidationServiceGrpc.newBlockingStub(channel),
                ReceiptLifecycleServiceGrpc.newBlockingStub(channel),
                JournalEntryHistoryServiceGrpc.newBlockingStub(channel),
                new ReceiptWorkflowRequestMapperImpl(),
                repository,
                RiskRegisterServiceGrpc.newStub(channel),
                Runnable::run);
        var request = ProbeSupport.sample(SignalReceiptWorkflowRequest.getDefaultInstance());
        var response = application.signalReceiptWorkflow(request);
        var sent0 = channel.request("acme.assets.v1.AssetTransferSyncService/PullAssetTransfers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "receipt", "audit", "created_by"),
                ProbeSupport.value(sent0, "asset_transfer", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "receipt", "audit", "created_at"),
                ProbeSupport.value(sent0, "asset_transfer", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "receipt", "billing_address", "lines"),
                ProbeSupport.value(sent0, "asset_transfer", "billing_address", "lines"));
        var sent1 = channel.request("acme.expenses.v1.ReceiptLifecycleService/ExpireReceipt");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "receipt", "receipt_id"),
                ProbeSupport.value(sent1, "receipt_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "preferred_channel"),
                ProbeSupport.value(sent1, "details", "notify_contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "phone"),
                ProbeSupport.value(sent1, "details", "notify_contact", "phone"));
        var sent2 = channel.request("acme.ledger.v1.JournalEntryHistoryService/CompareJournalEntryVersions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "receipt", "attachments", "content_type"),
                ProbeSupport.value(sent2, "journal_entry", "attachments", "content_type"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "receipt", "attachments", "uri"),
                ProbeSupport.value(sent2, "journal_entry", "attachments", "uri"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "receipt", "attachments", "size_bytes"),
                ProbeSupport.value(sent2, "journal_entry", "attachments", "size_bytes"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "receipt", "attachments", "checksum"),
                ProbeSupport.value(sent2, "journal_entry", "attachments", "checksum"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferSyncService/PullAssetTransfers"), "asset_transfer", "billing_address", "recipient"),
                ProbeSupport.value(response, "receipt", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferSyncService/PullAssetTransfers"), "asset_transfer", "billing_address", "locality"),
                ProbeSupport.value(response, "receipt", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ReceiptLifecycleService/ExpireReceipt"), "receipt", "counterparty", "number"),
                ProbeSupport.value(response, "receipt", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "receipt_id"),
                ProbeSupport.value(response, "receipt", "receipt_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "receipt", "issued_at"),
                ProbeSupport.value(response, "receipt", "issued_at"));
    }
}
