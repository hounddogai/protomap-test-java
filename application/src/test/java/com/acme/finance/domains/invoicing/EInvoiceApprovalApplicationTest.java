package com.acme.finance.domains.invoicing;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.audit.v1.AnnotateAuditEventVersionRequest;
import com.acme.audit.v1.AnnotateAuditEventVersionResponse;
import com.acme.audit.v1.AnnotateRetentionPolicyVersionRequest;
import com.acme.audit.v1.AnnotateRetentionPolicyVersionResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.PreviewAuditEventRequest;
import com.acme.audit.v1.PreviewAuditEventResponse;
import com.acme.audit.v1.RecordRetentionPolicyVersionRequest;
import com.acme.audit.v1.RecordRetentionPolicyVersionResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.ImportVariancesRequest;
import com.acme.budgeting.v1.ImportVariancesResponse;
import com.acme.budgeting.v1.SyncForecastsRequest;
import com.acme.budgeting.v1.SyncForecastsResponse;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CompareCustomerSegmentOptionsRequest;
import com.acme.customers.v1.CompareCustomerSegmentOptionsResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerNationalId;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.EstimateCustomerSegmentRequest;
import com.acme.customers.v1.EstimateCustomerSegmentResponse;
import com.acme.customers.v1.PreviewCustomerRequest;
import com.acme.customers.v1.PreviewCustomerResponse;
import com.acme.customers.v1.ScoreCustomerRequest;
import com.acme.customers.v1.ScoreCustomerResponse;
import com.acme.documents.v1.BatchGetDocumentsRequest;
import com.acme.documents.v1.BatchGetDocumentsResponse;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.giftcards.v1.ExportGiftCardsRequest;
import com.acme.giftcards.v1.ExportGiftCardsResponse;
import com.acme.giftcards.v1.GiftCardQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.insurance.v1.StreamPremiumMetricsRequest;
import com.acme.insurance.v1.StreamPremiumMetricsResponse;
import com.acme.invoicing.v1.ApproveEInvoiceRequest;
import com.acme.invoicing.v1.ApproveEInvoiceResponse;
import com.acme.invoicing.v1.CommentEInvoiceApprovalRequest;
import com.acme.invoicing.v1.CommentEInvoiceApprovalResponse;
import com.acme.invoicing.v1.CommentInvoiceBatchApprovalRequest;
import com.acme.invoicing.v1.CommentInvoiceBatchApprovalResponse;
import com.acme.invoicing.v1.DelegateEInvoiceApprovalRequest;
import com.acme.invoicing.v1.DelegateEInvoiceApprovalResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EscalateEInvoiceRequest;
import com.acme.invoicing.v1.EscalateEInvoiceResponse;
import com.acme.invoicing.v1.ExportInvoiceTemplatesRequest;
import com.acme.invoicing.v1.ExportInvoiceTemplatesResponse;
import com.acme.invoicing.v1.GetEInvoiceApprovalStatusRequest;
import com.acme.invoicing.v1.GetEInvoiceApprovalStatusResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchApprovalServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchRef;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateCardSettlement;
import com.acme.invoicing.v1.InvoiceTemplateQueryServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplateValidationServiceGrpc;
import com.acme.invoicing.v1.LintInvoiceTemplatesRequest;
import com.acme.invoicing.v1.LintInvoiceTemplatesResponse;
import com.acme.invoicing.v1.ListPendingEInvoicesRequest;
import com.acme.invoicing.v1.ListPendingEInvoicesResponse;
import com.acme.invoicing.v1.RecallEInvoiceRequest;
import com.acme.invoicing.v1.RecallEInvoiceResponse;
import com.acme.invoicing.v1.RecallInvoiceBatchRequest;
import com.acme.invoicing.v1.RecallInvoiceBatchResponse;
import com.acme.invoicing.v1.RejectEInvoiceRequest;
import com.acme.invoicing.v1.RejectEInvoiceResponse;
import com.acme.invoicing.v1.SubmitEInvoiceRequest;
import com.acme.invoicing.v1.SubmitEInvoiceResponse;
import com.acme.lending.v1.CancelLoanApplicationWorkflowRequest;
import com.acme.lending.v1.CancelLoanApplicationWorkflowResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationLine;
import com.acme.lending.v1.LoanApplicationLineTax;
import com.acme.lending.v1.LoanApplicationWorkflowServiceGrpc;
import com.acme.patients.v1.EstimatePatientRequest;
import com.acme.patients.v1.EstimatePatientResponse;
import com.acme.patients.v1.Patient;
import com.acme.patients.v1.PatientCalculationServiceGrpc;
import com.acme.patients.v1.PatientPassport;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class EInvoiceApprovalApplicationTest {
    @Test
    void rejectEInvoicePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InvoicingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveEInvoice(String tenant, EInvoice value) {}
            public void saveInvoiceBatch(String tenant, InvoiceBatch value) {}
            public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {}
        };
        var application = new EInvoiceApprovalApplication(
                DisposalSyncServiceGrpc.newStub(channel),
                AuditEventHistoryServiceGrpc.newBlockingStub(channel),
                AuditEventValidationServiceGrpc.newBlockingStub(channel),
                RetentionPolicyHistoryServiceGrpc.newBlockingStub(channel),
                ForecastSyncServiceGrpc.newBlockingStub(channel),
                VarianceImportServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                CustomerValidationServiceGrpc.newBlockingStub(channel),
                DocumentServiceGrpc.newBlockingStub(channel),
                GiftCardQueryServiceGrpc.newBlockingStub(channel),
                PremiumReportingServiceGrpc.newBlockingStub(channel),
                InvoiceBatchApprovalServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateQueryServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateValidationServiceGrpc.newBlockingStub(channel),
                LoanApplicationWorkflowServiceGrpc.newBlockingStub(channel),
                new EInvoiceApprovalRequestMapperImpl(),
                repository);
        var request = ProbeSupport.sample(RejectEInvoiceRequest.getDefaultInstance());
        var response = application.rejectEInvoice(request);
        var sent0 = channel.request("acme.customers.v1.CustomerSegmentCalculationService/CompareCustomerSegmentOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "audit", "updated_by"),
                ProbeSupport.value(sent0, "customer_segment", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "audit", "created_by"),
                ProbeSupport.value(sent0, "customer_segment", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "billing_address", "locality"),
                ProbeSupport.value(sent0, "details", "destination", "locality"));
        var sent1 = channel.request("acme.giftcards.v1.GiftCardQueryService/ExportGiftCards");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "audit", "created_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        var sent2 = channel.request("acme.lending.v1.LoanApplicationWorkflowService/CancelLoanApplicationWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "lines", "tax", "jurisdiction"),
                ProbeSupport.value(sent2, "loan_application", "lines", "tax", "jurisdiction"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "lines", "tax", "rate"),
                ProbeSupport.value(sent2, "loan_application", "lines", "tax", "rate"));
        var sent3 = channel.request("acme.customers.v1.CustomerValidationService/PreviewCustomer");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "counterparty", "display_name"),
                ProbeSupport.value(sent3, "customer", "relationship_manager", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationWorkflowService/CancelLoanApplicationWorkflow"), "loan_application", "audit", "etag"),
                ProbeSupport.value(sent3, "customer", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationWorkflowService/CancelLoanApplicationWorkflow"), "loan_application", "counterparty", "email"),
                ProbeSupport.value(sent3, "customer", "relationship_manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationWorkflowService/CancelLoanApplicationWorkflow"), "loan_application", "audit", "created_at"),
                ProbeSupport.value(sent3, "customer", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerSegmentCalculationService/CompareCustomerSegmentOptions"), "customer_segment", "audit", "created_by"),
                ProbeSupport.value(response, "e_invoice", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerSegmentCalculationService/CompareCustomerSegmentOptions"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerValidationService/PreviewCustomer"), "customer", "primary_address", "lines"),
                ProbeSupport.value(response, "e_invoice", "billing_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice_id"),
                ProbeSupport.value(response, "e_invoice", "e_invoice_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "notes"),
                ProbeSupport.value(response, "e_invoice", "notes"));
    }
}
