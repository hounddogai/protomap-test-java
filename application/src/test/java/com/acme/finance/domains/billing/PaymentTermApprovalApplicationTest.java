package com.acme.finance.domains.billing;

import com.acme.billing.v1.ApprovePaymentTermRequest;
import com.acme.billing.v1.ApprovePaymentTermResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountApprovalServiceGrpc;
import com.acme.billing.v1.BillingAccountCardFunding;
import com.acme.billing.v1.BillingAccountWorkflowServiceGrpc;
import com.acme.billing.v1.ComparePaymentTermOptionsRequest;
import com.acme.billing.v1.ComparePaymentTermOptionsResponse;
import com.acme.billing.v1.CountInvoicesRequest;
import com.acme.billing.v1.CountInvoicesResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteLifecycleServiceGrpc;
import com.acme.billing.v1.DelegatePaymentTermApprovalRequest;
import com.acme.billing.v1.DelegatePaymentTermApprovalResponse;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.DunningReportingServiceGrpc;
import com.acme.billing.v1.GenerateDunningReportRequest;
import com.acme.billing.v1.GenerateDunningReportResponse;
import com.acme.billing.v1.GetPaymentTermApprovalStatusRequest;
import com.acme.billing.v1.GetPaymentTermApprovalStatusResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceQueryServiceGrpc;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.InvoiceWorkflowServiceGrpc;
import com.acme.billing.v1.ListBillingAccountWorkflowsRequest;
import com.acme.billing.v1.ListBillingAccountWorkflowsResponse;
import com.acme.billing.v1.ListInvoiceWorkflowsRequest;
import com.acme.billing.v1.ListInvoiceWorkflowsResponse;
import com.acme.billing.v1.ListPendingBillingAccountsRequest;
import com.acme.billing.v1.ListPendingBillingAccountsResponse;
import com.acme.billing.v1.ListPendingPaymentTermsRequest;
import com.acme.billing.v1.ListPendingPaymentTermsResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermCalculationServiceGrpc;
import com.acme.billing.v1.PaymentTermDiscountAction;
import com.acme.billing.v1.PaymentTermLifecycleServiceGrpc;
import com.acme.billing.v1.RecallPaymentTermRequest;
import com.acme.billing.v1.RecallPaymentTermResponse;
import com.acme.billing.v1.ScheduleCreditNoteExpiryRequest;
import com.acme.billing.v1.ScheduleCreditNoteExpiryResponse;
import com.acme.billing.v1.ShareDunningReportRequest;
import com.acme.billing.v1.ShareDunningReportResponse;
import com.acme.billing.v1.StartBillingAccountWorkflowRequest;
import com.acme.billing.v1.StartBillingAccountWorkflowResponse;
import com.acme.billing.v1.SubmitPaymentTermRequest;
import com.acme.billing.v1.SubmitPaymentTermResponse;
import com.acme.billing.v1.SuspendPaymentTermRequest;
import com.acme.billing.v1.SuspendPaymentTermResponse;
import com.acme.budgeting.v1.ValidateVarianceImportRequest;
import com.acme.budgeting.v1.ValidateVarianceImportResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseWorkflowServiceGrpc;
import com.acme.collections.v1.ResumePaymentPromiseWorkflowRequest;
import com.acme.collections.v1.ResumePaymentPromiseWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceBlockAction;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.SimulatePreferenceRequest;
import com.acme.customers.v1.SimulatePreferenceResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportRequest;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportResponse;
import com.acme.suppliers.v1.CancelSupplierWorkflowRequest;
import com.acme.suppliers.v1.CancelSupplierWorkflowResponse;
import com.acme.suppliers.v1.CommentSupplierContractApprovalRequest;
import com.acme.suppliers.v1.CommentSupplierContractApprovalResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierContractLine;
import com.acme.suppliers.v1.SupplierContractLineTax;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierWorkflowServiceGrpc;
import com.google.protobuf.Any;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class PaymentTermApprovalApplicationTest {
    @Test
    void submitPaymentTermPreservesPlannedFields() {
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
        var application = new PaymentTermApprovalApplication(
                BillingAccountApprovalServiceGrpc.newBlockingStub(channel),
                BillingAccountWorkflowServiceGrpc.newBlockingStub(channel),
                CreditNoteLifecycleServiceGrpc.newBlockingStub(channel),
                DunningReportingServiceGrpc.newBlockingStub(channel),
                InvoiceQueryServiceGrpc.newBlockingStub(channel),
                InvoiceWorkflowServiceGrpc.newBlockingStub(channel),
                PaymentTermCalculationServiceGrpc.newBlockingStub(channel),
                PaymentTermLifecycleServiceGrpc.newBlockingStub(channel),
                VarianceImportServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                PaymentPromiseWorkflowServiceGrpc.newBlockingStub(channel),
                PreferenceCalculationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierContractApprovalServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SubmitPaymentTermRequest.getDefaultInstance());
        var response = application.submitPaymentTerm(request);
        var sent0 = channel.request("acme.billing.v1.BillingAccountApprovalService/ListPendingBillingAccounts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_term", "audit", "created_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "user_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        var sent1 = channel.request("acme.billing.v1.DunningReportingService/ShareDunningReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_term", "owner", "email"),
                ProbeSupport.value(sent1, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_term", "block", "reason"),
                ProbeSupport.value(sent1, "reason"));
        var sent2 = channel.request("acme.suppliers.v1.SupplierWorkflowService/CancelSupplierWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_term", "audit", "created_by"),
                ProbeSupport.value(sent2, "supplier", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.DunningReportingService/ShareDunningReport"), "dunning", "requester", "email"),
                ProbeSupport.value(sent2, "supplier", "relationship_manager", "email"));
        var sent3 = channel.request("acme.billing.v1.BillingAccountWorkflowService/ListBillingAccountWorkflows");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierWorkflowService/CancelSupplierWorkflow"), "supplier", "primary_address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.BillingAccountApprovalService/ListPendingBillingAccounts"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.BillingAccountApprovalService/ListPendingBillingAccounts"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.DunningReportingService/ShareDunningReport"), "dunning", "audit", "created_by"),
                ProbeSupport.value(response, "payment_term", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.DunningReportingService/ShareDunningReport"), "outcome", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.BillingAccountWorkflowService/ListBillingAccountWorkflows"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_term_id"),
                ProbeSupport.value(response, "payment_term", "payment_term_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_term", "parameters"),
                ProbeSupport.value(response, "payment_term", "parameters"));
    }
}
