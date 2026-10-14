package com.acme.finance.domains.invoicing;

import com.acme.audiences.v1.AudienceServiceGrpc;
import com.acme.audiences.v1.BatchGetAudiencesRequest;
import com.acme.audiences.v1.BatchGetAudiencesResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.RevertAuditTrailRequest;
import com.acme.audit.v1.RevertAuditTrailResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseWorkflowServiceGrpc;
import com.acme.collections.v1.RetryPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.RetryPaymentPromiseWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CalculatePreferenceRequest;
import com.acme.customers.v1.CalculatePreferenceResponse;
import com.acme.customers.v1.CreateCustomerRequest;
import com.acme.customers.v1.CreateCustomerResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.FindHouseholdsRequest;
import com.acme.customers.v1.FindHouseholdsResponse;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.ScoreCustomerRequest;
import com.acme.customers.v1.ScoreCustomerResponse;
import com.acme.documents.v1.CountDocumentsRequest;
import com.acme.documents.v1.CountDocumentsResponse;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.ApproveInvoiceBatchRequest;
import com.acme.invoicing.v1.ApproveInvoiceBatchResponse;
import com.acme.invoicing.v1.CommentInvoiceBatchApprovalRequest;
import com.acme.invoicing.v1.CommentInvoiceBatchApprovalResponse;
import com.acme.invoicing.v1.CountEInvoiceTotalsRequest;
import com.acme.invoicing.v1.CountEInvoiceTotalsResponse;
import com.acme.invoicing.v1.DelegateInvoiceBatchApprovalRequest;
import com.acme.invoicing.v1.DelegateInvoiceBatchApprovalResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EInvoiceReportingServiceGrpc;
import com.acme.invoicing.v1.EscalateInvoiceBatchRequest;
import com.acme.invoicing.v1.EscalateInvoiceBatchResponse;
import com.acme.invoicing.v1.ExplainInvoiceBatchDecisionRequest;
import com.acme.invoicing.v1.ExplainInvoiceBatchDecisionResponse;
import com.acme.invoicing.v1.ExportInvoiceBatchesRequest;
import com.acme.invoicing.v1.ExportInvoiceBatchesResponse;
import com.acme.invoicing.v1.GetInvoiceBatchRequest;
import com.acme.invoicing.v1.GetInvoiceBatchResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchBankSettlement;
import com.acme.invoicing.v1.InvoiceBatchQueryServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchValidationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateCalculationServiceGrpc;
import com.acme.invoicing.v1.RecallInvoiceBatchRequest;
import com.acme.invoicing.v1.RecallInvoiceBatchResponse;
import com.acme.invoicing.v1.RejectInvoiceBatchRequest;
import com.acme.invoicing.v1.RejectInvoiceBatchResponse;
import com.acme.invoicing.v1.SimulateInvoiceTemplateRequest;
import com.acme.invoicing.v1.SimulateInvoiceTemplateResponse;
import com.acme.invoicing.v1.SubmitInvoiceBatchRequest;
import com.acme.invoicing.v1.SubmitInvoiceBatchResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeRequest;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeResponse;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesRequest;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesResponse;
import com.acme.payroll.v1.SubmitTaxWithholdingRequest;
import com.acme.payroll.v1.SubmitTaxWithholdingResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingApprovalServiceGrpc;
import com.acme.suppliers.v1.RetrySupplierContractWorkflowRequest;
import com.acme.suppliers.v1.RetrySupplierContractWorkflowResponse;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractAttachment;
import com.acme.suppliers.v1.SupplierContractWorkflowServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferWorkflowServiceGrpc;
import com.acme.treasury.v1.CancelBankTransferWorkflowRequest;
import com.acme.treasury.v1.CancelBankTransferWorkflowResponse;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class InvoiceBatchApprovalApplicationTest {
    @Test
    void escalateInvoiceBatchPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InvoicingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveEInvoice(String tenant, EInvoice value) {}
            public void saveInvoiceBatch(String tenant, InvoiceBatch value) {}
            public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {}
        };
        var listener = new InvoiceBatchApprovalApplicationEvents(PreferenceCalculationServiceGrpc.newStub(channel), BankTransferWorkflowServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof InvoiceBatchApprovalApplication.EscalateInvoiceBatchStage1Requested command) {
                listener.on(command);
            }
            else if (event instanceof InvoiceBatchApprovalApplication.DelegateInvoiceBatchApprovalStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new InvoiceBatchApprovalApplication(
                AudienceServiceGrpc.newBlockingStub(channel),
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                PaymentPromiseWorkflowServiceGrpc.newBlockingStub(channel),
                CustomerServiceGrpc.newBlockingStub(channel),
                CustomerValidationServiceGrpc.newBlockingStub(channel),
                HouseholdQueryServiceGrpc.newBlockingStub(channel),
                DocumentQueryServiceGrpc.newBlockingStub(channel),
                events,
                EInvoiceReportingServiceGrpc.newBlockingStub(channel),
                InvoiceBatchQueryServiceGrpc.newBlockingStub(channel),
                InvoiceBatchServiceGrpc.newBlockingStub(channel),
                InvoiceBatchValidationServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                new InvoiceBatchApprovalRequestMapperImpl(),
                TaxWithholdingApprovalServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierContractWorkflowServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(EscalateInvoiceBatchRequest.getDefaultInstance());
        var response = application.escalateInvoiceBatch(request);
        var sent0 = channel.request("acme.suppliers.v1.SupplierContractWorkflowService/RetrySupplierContractWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "attachments", "content_type"),
                ProbeSupport.value(sent0, "supplier_contract", "attachments", "content_type"));
        var sent1 = channel.request("acme.customers.v1.PreferenceCalculationService/CalculatePreference");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "billing_address", "postal_code"),
                ProbeSupport.value(sent1, "details", "destination", "postal_code"));
        var sent2 = channel.request("acme.collections.v1.PaymentPromiseWorkflowService/RetryPaymentPromiseWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierContractWorkflowService/RetrySupplierContractWorkflow"), "supplier_contract", "counterparty", "display_name"),
                ProbeSupport.value(sent2, "payment_promise", "organizer", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierContractWorkflowService/RetrySupplierContractWorkflow"), "supplier_contract", "billing_address", "postal_code"),
                ProbeSupport.value(sent2, "payment_promise", "location", "postal_code"));
        var sent3 = channel.request("acme.customers.v1.CustomerValidationService/ScoreCustomer");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.PreferenceCalculationService/CalculatePreference"), "preference", "audit", "updated_by"),
                ProbeSupport.value(sent3, "customer", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.PaymentPromiseWorkflowService/RetryPaymentPromiseWorkflow"), "payment_promise", "location", "country_code"),
                ProbeSupport.value(sent3, "customer", "primary_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.PaymentPromiseWorkflowService/RetryPaymentPromiseWorkflow"), "payment_promise", "audit", "updated_at"),
                ProbeSupport.value(sent3, "customer", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.PreferenceCalculationService/CalculatePreference"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.PreferenceCalculationService/CalculatePreference"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.PaymentPromiseWorkflowService/RetryPaymentPromiseWorkflow"), "payment_promise", "location", "region"),
                ProbeSupport.value(response, "invoice_batch", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.PaymentPromiseWorkflowService/RetryPaymentPromiseWorkflow"), "payment_promise", "location", "locality"),
                ProbeSupport.value(response, "invoice_batch", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerValidationService/ScoreCustomer"), "customer", "primary_address", "lines"),
                ProbeSupport.value(response, "invoice_batch", "billing_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch_id"),
                ProbeSupport.value(response, "invoice_batch", "invoice_batch_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "total"),
                ProbeSupport.value(response, "invoice_batch", "total"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new InvoiceBatchApprovalApplicationEvents(PreferenceCalculationServiceGrpc.newStub(channel), BankTransferWorkflowServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CalculatePreferenceResponse>();
        listener.on(new InvoiceBatchApprovalApplication.EscalateInvoiceBatchStage1Requested(ProbeSupport.sample(CalculatePreferenceRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<CancelBankTransferWorkflowResponse>();
        listener.on(new InvoiceBatchApprovalApplication.DelegateInvoiceBatchApprovalStage2Requested(ProbeSupport.sample(CancelBankTransferWorkflowRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
