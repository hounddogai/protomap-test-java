package com.acme.finance.domains.billing;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.PreviewAuditTrailRequest;
import com.acme.audit.v1.PreviewAuditTrailResponse;
import com.acme.billing.v1.AdvanceCreditNoteWorkflowRequest;
import com.acme.billing.v1.AdvanceCreditNoteWorkflowResponse;
import com.acme.billing.v1.BatchGetBillingAccountsRequest;
import com.acme.billing.v1.BatchGetBillingAccountsResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountApprovalServiceGrpc;
import com.acme.billing.v1.BillingAccountCardFunding;
import com.acme.billing.v1.BillingAccountLimit;
import com.acme.billing.v1.BillingAccountServiceGrpc;
import com.acme.billing.v1.BillingAccountWorkflowServiceGrpc;
import com.acme.billing.v1.CancelCreditNoteWorkflowRequest;
import com.acme.billing.v1.CancelCreditNoteWorkflowResponse;
import com.acme.billing.v1.CommentCreditNoteApprovalRequest;
import com.acme.billing.v1.CommentCreditNoteApprovalResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteApprovalServiceGrpc;
import com.acme.billing.v1.CreditNoteAttachment;
import com.acme.billing.v1.CreditNoteCalculationServiceGrpc;
import com.acme.billing.v1.CreditNoteCardSettlement;
import com.acme.billing.v1.CreditNoteValidationServiceGrpc;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.EscalateBillingAccountRequest;
import com.acme.billing.v1.EscalateBillingAccountResponse;
import com.acme.billing.v1.GetCreditNoteRatesRequest;
import com.acme.billing.v1.GetCreditNoteRatesResponse;
import com.acme.billing.v1.GetCreditNoteRulesRequest;
import com.acme.billing.v1.GetCreditNoteRulesResponse;
import com.acme.billing.v1.InspectCreditNoteWorkflowRequest;
import com.acme.billing.v1.InspectCreditNoteWorkflowResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceLine;
import com.acme.billing.v1.InvoiceLineTax;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.InvoiceWorkflowServiceGrpc;
import com.acme.billing.v1.ListCreditNoteWorkflowsRequest;
import com.acme.billing.v1.ListCreditNoteWorkflowsResponse;
import com.acme.billing.v1.ListPendingPaymentTermsRequest;
import com.acme.billing.v1.ListPendingPaymentTermsResponse;
import com.acme.billing.v1.PauseCreditNoteWorkflowRequest;
import com.acme.billing.v1.PauseCreditNoteWorkflowResponse;
import com.acme.billing.v1.PauseInvoiceWorkflowRequest;
import com.acme.billing.v1.PauseInvoiceWorkflowResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermApprovalServiceGrpc;
import com.acme.billing.v1.RejectCreditNoteRequest;
import com.acme.billing.v1.RejectCreditNoteResponse;
import com.acme.billing.v1.ResumeCreditNoteWorkflowRequest;
import com.acme.billing.v1.ResumeCreditNoteWorkflowResponse;
import com.acme.billing.v1.ResumeInvoiceWorkflowRequest;
import com.acme.billing.v1.ResumeInvoiceWorkflowResponse;
import com.acme.billing.v1.RetryBillingAccountWorkflowRequest;
import com.acme.billing.v1.RetryBillingAccountWorkflowResponse;
import com.acme.billing.v1.RetryCreditNoteWorkflowRequest;
import com.acme.billing.v1.RetryCreditNoteWorkflowResponse;
import com.acme.billing.v1.SignalCreditNoteWorkflowRequest;
import com.acme.billing.v1.SignalCreditNoteWorkflowResponse;
import com.acme.billing.v1.StartCreditNoteWorkflowRequest;
import com.acme.billing.v1.StartCreditNoteWorkflowResponse;
import com.acme.billing.v1.SubmitBillingAccountRequest;
import com.acme.billing.v1.SubmitBillingAccountResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.BatchCreateCustomersRequest;
import com.acme.customers.v1.BatchCreateCustomersResponse;
import com.acme.customers.v1.BatchGetCustomersRequest;
import com.acme.customers.v1.BatchGetCustomersResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.RecalculateCustomerSegmentsRequest;
import com.acme.customers.v1.RecalculateCustomerSegmentsResponse;
import com.acme.customers.v1.ResolveCustomersRequest;
import com.acme.customers.v1.ResolveCustomersResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.CancelCollateralWorkflowRequest;
import com.acme.lending.v1.CancelCollateralWorkflowResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralComponent;
import com.acme.lending.v1.CollateralWorkflowServiceGrpc;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentLifecycleServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.ScheduleRepaymentExpiryRequest;
import com.acme.lending.v1.ScheduleRepaymentExpiryResponse;
import com.acme.messaging.v1.CalculateConversationRequest;
import com.acme.messaging.v1.CalculateConversationResponse;
import com.acme.messaging.v1.Conversation;
import com.acme.messaging.v1.ConversationCalculationServiceGrpc;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastCardSettlement;
import com.acme.treasury.v1.LiquidityForecastImportServiceGrpc;
import com.acme.treasury.v1.RetryLiquidityForecastImportRequest;
import com.acme.treasury.v1.RetryLiquidityForecastImportResponse;
import java.util.ArrayList;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class CreditNoteWorkflowApplicationTest {
    @Test
    void retryCreditNoteWorkflowPreservesPlannedFields() {
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
        var listener = new CreditNoteWorkflowApplicationEvents(CreditNoteCalculationServiceGrpc.newStub(channel), CustomerServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof CreditNoteWorkflowApplication.PauseCreditNoteWorkflowStage1Requested command) {
                listener.on(command);
            }
            else if (event instanceof CreditNoteWorkflowApplication.InspectCreditNoteWorkflowStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new CreditNoteWorkflowApplication(
                AuditTrailValidationServiceGrpc.newBlockingStub(channel),
                BillingAccountApprovalServiceGrpc.newBlockingStub(channel),
                BillingAccountServiceGrpc.newBlockingStub(channel),
                BillingAccountWorkflowServiceGrpc.newBlockingStub(channel),
                CreditNoteApprovalServiceGrpc.newBlockingStub(channel),
                CreditNoteValidationServiceGrpc.newBlockingStub(channel),
                InvoiceWorkflowServiceGrpc.newBlockingStub(channel),
                PaymentTermApprovalServiceGrpc.newBlockingStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                CustomerServiceGrpc.newStub(channel),
                events,
                CollateralWorkflowServiceGrpc.newBlockingStub(channel),
                RepaymentLifecycleServiceGrpc.newBlockingStub(channel),
                new CreditNoteWorkflowRequestMapperImpl(),
                ConversationCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                LiquidityForecastImportServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RetryCreditNoteWorkflowRequest.getDefaultInstance());
        var response = application.retryCreditNoteWorkflow(request);
        var sent0 = channel.request("acme.billing.v1.BillingAccountApprovalService/SubmitBillingAccount");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "lines", "amount"),
                ProbeSupport.value(sent0, "billing_account", "limits", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "audit", "created_by"),
                ProbeSupport.value(sent0, "billing_account", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "counterparty", "display_name"),
                ProbeSupport.value(sent0, "billing_account", "holder", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "lines", "tax", "amount"),
                ProbeSupport.value(sent0, "billing_account", "balance"));
        var sent1 = channel.request("acme.billing.v1.PaymentTermApprovalService/ListPendingPaymentTerms");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.BillingAccountApprovalService/SubmitBillingAccount"), "billing_account", "labels"),
                ProbeSupport.value(sent1, "filter", "labels"));
        var sent2 = channel.request("acme.billing.v1.CreditNoteApprovalService/RejectCreditNote");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "audit", "created_by"),
                ProbeSupport.value(sent2, "credit_note", "audit", "created_by"));
        var sent3 = channel.request("acme.lending.v1.CollateralWorkflowService/CancelCollateralWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "lines", "description"),
                ProbeSupport.value(sent3, "collateral", "components", "description"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "lines", "quantity"),
                ProbeSupport.value(sent3, "collateral", "components", "quantity"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.BillingAccountApprovalService/SubmitBillingAccount"), "billing_account", "audit", "created_by"),
                ProbeSupport.value(sent3, "collateral", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.PaymentTermApprovalService/ListPendingPaymentTerms"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteApprovalService/RejectCreditNote"), "credit_note", "metadata"),
                ProbeSupport.value(response, "credit_note", "metadata"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteApprovalService/RejectCreditNote"), "credit_note", "audit", "etag"),
                ProbeSupport.value(response, "credit_note", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note_id"),
                ProbeSupport.value(response, "credit_note", "credit_note_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "notes"),
                ProbeSupport.value(response, "credit_note", "notes"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new CreditNoteWorkflowApplicationEvents(CreditNoteCalculationServiceGrpc.newStub(channel), CustomerServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<GetCreditNoteRatesResponse>();
        listener.on(new CreditNoteWorkflowApplication.PauseCreditNoteWorkflowStage1Requested(ProbeSupport.sample(GetCreditNoteRatesRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<BatchGetCustomersResponse>();
        listener.on(new CreditNoteWorkflowApplication.InspectCreditNoteWorkflowStage0Requested(ProbeSupport.sample(BatchGetCustomersRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
