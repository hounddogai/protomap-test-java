package com.acme.finance.domains.billing;

import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.UpdateAccessPolicyRequest;
import com.acme.authorization.v1.UpdateAccessPolicyResponse;
import com.acme.billing.v1.AdvanceInvoiceWorkflowRequest;
import com.acme.billing.v1.AdvanceInvoiceWorkflowResponse;
import com.acme.billing.v1.ApproveCreditNoteRequest;
import com.acme.billing.v1.ApproveCreditNoteResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountApprovalServiceGrpc;
import com.acme.billing.v1.CancelInvoiceWorkflowRequest;
import com.acme.billing.v1.CancelInvoiceWorkflowResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteApprovalServiceGrpc;
import com.acme.billing.v1.CreditNoteCalculationServiceGrpc;
import com.acme.billing.v1.CreditNoteCardSettlement;
import com.acme.billing.v1.CreditNoteValidationServiceGrpc;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.EscalateBillingAccountRequest;
import com.acme.billing.v1.EscalateBillingAccountResponse;
import com.acme.billing.v1.ExplainCreditNoteDecisionRequest;
import com.acme.billing.v1.ExplainCreditNoteDecisionResponse;
import com.acme.billing.v1.GetInvoiceApprovalStatusRequest;
import com.acme.billing.v1.GetInvoiceApprovalStatusResponse;
import com.acme.billing.v1.InspectInvoiceWorkflowRequest;
import com.acme.billing.v1.InspectInvoiceWorkflowResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceApprovalServiceGrpc;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.ListInvoiceWorkflowsRequest;
import com.acme.billing.v1.ListInvoiceWorkflowsResponse;
import com.acme.billing.v1.ListPendingCreditNotesRequest;
import com.acme.billing.v1.ListPendingCreditNotesResponse;
import com.acme.billing.v1.PauseInvoiceWorkflowRequest;
import com.acme.billing.v1.PauseInvoiceWorkflowResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.QuoteCreditNoteRequest;
import com.acme.billing.v1.QuoteCreditNoteResponse;
import com.acme.billing.v1.ResumeInvoiceWorkflowRequest;
import com.acme.billing.v1.ResumeInvoiceWorkflowResponse;
import com.acme.billing.v1.RetryInvoiceWorkflowRequest;
import com.acme.billing.v1.RetryInvoiceWorkflowResponse;
import com.acme.billing.v1.SignalInvoiceWorkflowRequest;
import com.acme.billing.v1.SignalInvoiceWorkflowResponse;
import com.acme.billing.v1.SimulateCreditNoteRequest;
import com.acme.billing.v1.SimulateCreditNoteResponse;
import com.acme.billing.v1.StartInvoiceWorkflowRequest;
import com.acme.billing.v1.StartInvoiceWorkflowResponse;
import com.acme.budgeting.v1.EscalateVarianceRequest;
import com.acme.budgeting.v1.EscalateVarianceResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceApprovalServiceGrpc;
import com.acme.budgeting.v1.VarianceComment;
import com.acme.collections.v1.CommentPaymentPromiseApprovalRequest;
import com.acme.collections.v1.CommentPaymentPromiseApprovalResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityRequest;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.NormalizeConsentRecordRequest;
import com.acme.customers.v1.NormalizeConsentRecordResponse;
import com.acme.customers.v1.ResolveCustomersRequest;
import com.acme.customers.v1.ResolveCustomersResponse;
import com.acme.customers.v1.ScoreConsentRecordRequest;
import com.acme.customers.v1.ScoreConsentRecordResponse;
import com.acme.expenses.v1.AdvanceExpenseReportWorkflowRequest;
import com.acme.expenses.v1.AdvanceExpenseReportWorkflowResponse;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.itsm.v1.Incident;
import com.acme.itsm.v1.IncidentCalculationServiceGrpc;
import com.acme.itsm.v1.RecalculateIncidentsRequest;
import com.acme.itsm.v1.RecalculateIncidentsResponse;
import com.acme.ledger.v1.BrowseLedgerAccountsRequest;
import com.acme.ledger.v1.BrowseLedgerAccountsResponse;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.surveys.v1.BrowseSurveysRequest;
import com.acme.surveys.v1.BrowseSurveysResponse;
import com.acme.surveys.v1.SurveyQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class InvoiceWorkflowApplicationTest {
    @Test
    void pauseInvoiceWorkflowPreservesPlannedFields() {
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
        var application = new InvoiceWorkflowApplication(
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                BillingAccountApprovalServiceGrpc.newBlockingStub(channel),
                CreditNoteApprovalServiceGrpc.newBlockingStub(channel),
                CreditNoteApprovalServiceGrpc.newFutureStub(channel),
                CreditNoteCalculationServiceGrpc.newBlockingStub(channel),
                CreditNoteValidationServiceGrpc.newBlockingStub(channel),
                InvoiceApprovalServiceGrpc.newBlockingStub(channel),
                VarianceApprovalServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                PaymentPromiseApprovalServiceGrpc.newBlockingStub(channel),
                ConsentRecordValidationServiceGrpc.newBlockingStub(channel),
                ConsentRecordValidationServiceGrpc.newFutureStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                CustomerSegmentValidationServiceGrpc.newBlockingStub(channel),
                IncidentCalculationServiceGrpc.newBlockingStub(channel),
                LedgerAccountQueryServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SurveyQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(PauseInvoiceWorkflowRequest.getDefaultInstance());
        var response = application.pauseInvoiceWorkflow(request);
        var sent0 = channel.request("acme.billing.v1.InvoiceApprovalService/GetInvoiceApprovalStatus");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_id"),
                ProbeSupport.value(sent0, "invoice_id"));
        var sent1 = channel.request("acme.billing.v1.CreditNoteApprovalService/ListPendingCreditNotes");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceApprovalService/GetInvoiceApprovalStatus"), "invoice", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice", "audit", "updated_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        var sent2 = channel.request("acme.surveys.v1.SurveyQueryService/BrowseSurveys");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceApprovalService/GetInvoiceApprovalStatus"), "invoice", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceApprovalService/GetInvoiceApprovalStatus"), "invoice", "audit", "updated_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteApprovalService/ListPendingCreditNotes"), "result", "details"),
                ProbeSupport.value(sent2, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceApprovalService/GetInvoiceApprovalStatus"), "invoice", "counterparty", "customer_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        var sent3 = channel.request("acme.customers.v1.ConsentRecordValidationService/ScoreConsentRecord");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice", "counterparty", "display_name"),
                ProbeSupport.value(sent3, "consent_record", "subject", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceApprovalService/GetInvoiceApprovalStatus"), "invoice", "billing_address", "locality"),
                ProbeSupport.value(sent3, "details", "destination", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceApprovalService/GetInvoiceApprovalStatus"), "invoice", "billing_address", "region"),
                ProbeSupport.value(response, "invoice", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceApprovalService/GetInvoiceApprovalStatus"), "invoice", "notes"),
                ProbeSupport.value(response, "invoice", "notes"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteApprovalService/ListPendingCreditNotes"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteApprovalService/ListPendingCreditNotes"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.ConsentRecordValidationService/ScoreConsentRecord"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.ConsentRecordValidationService/ScoreConsentRecord"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_id"),
                ProbeSupport.value(response, "invoice", "invoice_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice", "metadata"),
                ProbeSupport.value(response, "invoice", "metadata"));
    }
}
