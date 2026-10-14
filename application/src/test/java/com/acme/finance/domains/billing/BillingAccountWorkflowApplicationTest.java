package com.acme.finance.domains.billing;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalApprovalServiceGrpc;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.RecallDisposalRequest;
import com.acme.assets.v1.RecallDisposalResponse;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.StreamAuditTrailExportRequest;
import com.acme.audit.v1.StreamAuditTrailExportResponse;
import com.acme.billing.v1.AdvanceBillingAccountWorkflowRequest;
import com.acme.billing.v1.AdvanceBillingAccountWorkflowResponse;
import com.acme.billing.v1.ApproveInvoiceRequest;
import com.acme.billing.v1.ApproveInvoiceResponse;
import com.acme.billing.v1.BatchGetBillingAccountsRequest;
import com.acme.billing.v1.BatchGetBillingAccountsResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountApprovalServiceGrpc;
import com.acme.billing.v1.BillingAccountBankFunding;
import com.acme.billing.v1.BillingAccountLimit;
import com.acme.billing.v1.BillingAccountServiceGrpc;
import com.acme.billing.v1.BillingAccountValidationServiceGrpc;
import com.acme.billing.v1.CancelBillingAccountWorkflowRequest;
import com.acme.billing.v1.CancelBillingAccountWorkflowResponse;
import com.acme.billing.v1.CommentInvoiceApprovalRequest;
import com.acme.billing.v1.CommentInvoiceApprovalResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteCalculationServiceGrpc;
import com.acme.billing.v1.CreditNoteCardSettlement;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.DunningReportingServiceGrpc;
import com.acme.billing.v1.GenerateDunningReportRequest;
import com.acme.billing.v1.GenerateDunningReportResponse;
import com.acme.billing.v1.InspectBillingAccountWorkflowRequest;
import com.acme.billing.v1.InspectBillingAccountWorkflowResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceApprovalServiceGrpc;
import com.acme.billing.v1.InvoiceBankSettlement;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.LintBillingAccountsRequest;
import com.acme.billing.v1.LintBillingAccountsResponse;
import com.acme.billing.v1.ListBillingAccountWorkflowsRequest;
import com.acme.billing.v1.ListBillingAccountWorkflowsResponse;
import com.acme.billing.v1.PauseBillingAccountWorkflowRequest;
import com.acme.billing.v1.PauseBillingAccountWorkflowResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.RecalculateCreditNotesRequest;
import com.acme.billing.v1.RecalculateCreditNotesResponse;
import com.acme.billing.v1.ResumeBillingAccountWorkflowRequest;
import com.acme.billing.v1.ResumeBillingAccountWorkflowResponse;
import com.acme.billing.v1.RetryBillingAccountWorkflowRequest;
import com.acme.billing.v1.RetryBillingAccountWorkflowResponse;
import com.acme.billing.v1.SignalBillingAccountWorkflowRequest;
import com.acme.billing.v1.SignalBillingAccountWorkflowResponse;
import com.acme.billing.v1.StartBillingAccountWorkflowRequest;
import com.acme.billing.v1.StartBillingAccountWorkflowResponse;
import com.acme.billing.v1.SubmitBillingAccountRequest;
import com.acme.billing.v1.SubmitBillingAccountResponse;
import com.acme.billing.v1.UpdateBillingAccountRequest;
import com.acme.billing.v1.UpdateBillingAccountResponse;
import com.acme.collections.v1.BatchCreateCollectionCasesRequest;
import com.acme.collections.v1.BatchCreateCollectionCasesResponse;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.QuotePreferenceRequest;
import com.acme.customers.v1.QuotePreferenceResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptLine;
import com.acme.expenses.v1.ReceiptWorkflowServiceGrpc;
import com.acme.expenses.v1.StartReceiptWorkflowRequest;
import com.acme.expenses.v1.StartReceiptWorkflowResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.AdvanceCoverageWorkflowRequest;
import com.acme.insurance.v1.AdvanceCoverageWorkflowResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageWorkflowServiceGrpc;
import com.acme.insurance.v1.SignalCoverageWorkflowRequest;
import com.acme.insurance.v1.SignalCoverageWorkflowResponse;
import com.acme.pricing.v1.BatchGetPriceListsRequest;
import com.acme.pricing.v1.BatchGetPriceListsResponse;
import com.acme.pricing.v1.PriceListServiceGrpc;
import com.acme.recruiting.v1.ExportJobOpeningsRequest;
import com.acme.recruiting.v1.ExportJobOpeningsResponse;
import com.acme.recruiting.v1.JobOpeningQueryServiceGrpc;
import com.acme.search.v1.BatchGetSearchIndexesRequest;
import com.acme.search.v1.BatchGetSearchIndexesResponse;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class BillingAccountWorkflowApplicationTest {
    @Test
    void signalBillingAccountWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BillingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBillingAccount(String tenant, BillingAccount value) {}
            public void saveCreditNote(String tenant, CreditNote value) {}
            public void saveDunning(String tenant, Dunning value) {}
            public void saveInvoice(String tenant, Invoice value) {}
            public void savePaymentTerm(String tenant, PaymentTerm value) {}
        };
        var application = new BillingAccountWorkflowApplication(
                DisposalApprovalServiceGrpc.newBlockingStub(channel),
                DisposalSyncServiceGrpc.newStub(channel),
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                BillingAccountApprovalServiceGrpc.newBlockingStub(channel),
                BillingAccountServiceGrpc.newBlockingStub(channel),
                BillingAccountValidationServiceGrpc.newBlockingStub(channel),
                CreditNoteCalculationServiceGrpc.newBlockingStub(channel),
                DunningReportingServiceGrpc.newBlockingStub(channel),
                InvoiceApprovalServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseServiceGrpc.newStub(channel),
                PreferenceCalculationServiceGrpc.newBlockingStub(channel),
                ReceiptWorkflowServiceGrpc.newBlockingStub(channel),
                CoverageWorkflowServiceGrpc.newBlockingStub(channel),
                PriceListServiceGrpc.newBlockingStub(channel),
                JobOpeningQueryServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SearchIndexServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SignalBillingAccountWorkflowRequest.getDefaultInstance());
        var response = application.signalBillingAccountWorkflow(request);
        var sent0 = channel.request("acme.billing.v1.InvoiceApprovalService/ApproveInvoice");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "billing_account", "audit", "etag"),
                ProbeSupport.value(sent0, "invoice", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "billing_account", "audit", "updated_at"),
                ProbeSupport.value(sent0, "invoice", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "billing_account", "audit", "created_at"),
                ProbeSupport.value(sent0, "invoice", "audit", "created_at"));
        var sent1 = channel.request("acme.billing.v1.BillingAccountValidationService/LintBillingAccounts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceApprovalService/ApproveInvoice"), "invoice", "counterparty", "email"),
                ProbeSupport.value(sent1, "billing_account", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "billing_account", "holder", "number"),
                ProbeSupport.value(sent1, "billing_account", "holder", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "billing_account", "bank", "account_holder"),
                ProbeSupport.value(sent1, "billing_account", "bank", "account_holder"));
        var sent2 = channel.request("acme.insurance.v1.CoverageWorkflowService/SignalCoverageWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "preferred_channel"),
                ProbeSupport.value(sent2, "details", "notify", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "phone"),
                ProbeSupport.value(sent2, "details", "notify", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.BillingAccountValidationService/LintBillingAccounts"), "billing_account", "contact", "name"),
                ProbeSupport.value(sent2, "details", "notify", "name"));
        var sent3 = channel.request("acme.billing.v1.CreditNoteCalculationService/RecalculateCreditNotes");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "billing_account", "card", "holder_name"),
                ProbeSupport.value(sent3, "credit_note", "card", "holder_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceApprovalService/ApproveInvoice"), "invoice", "billing_address", "locality"),
                ProbeSupport.value(sent3, "details", "destination", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "billing_account", "audit", "updated_by"),
                ProbeSupport.value(sent3, "credit_note", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceApprovalService/ApproveInvoice"), "invoice", "audit", "created_at"),
                ProbeSupport.value(sent3, "credit_note", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceApprovalService/ApproveInvoice"), "invoice", "audit", "created_by"),
                ProbeSupport.value(response, "billing_account", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceApprovalService/ApproveInvoice"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.BillingAccountValidationService/LintBillingAccounts"), "billing_account", "status"),
                ProbeSupport.value(response, "billing_account", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageWorkflowService/SignalCoverageWorkflow"), "outcome", "next_steps"),
                ProbeSupport.value(response, "outcome", "next_steps"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageWorkflowService/SignalCoverageWorkflow"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteCalculationService/RecalculateCreditNotes"), "credit_note", "counterparty", "number"),
                ProbeSupport.value(response, "billing_account", "holder", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "billing_account_id"),
                ProbeSupport.value(response, "billing_account", "billing_account_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "billing_account", "balance"),
                ProbeSupport.value(response, "billing_account", "balance"));
    }
}
