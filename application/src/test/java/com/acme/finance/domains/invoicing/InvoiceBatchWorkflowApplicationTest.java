package com.acme.finance.domains.invoicing;

import com.acme.admissions.v1.Admission;
import com.acme.admissions.v1.AdmissionCalculationServiceGrpc;
import com.acme.admissions.v1.EstimateAdmissionRequest;
import com.acme.admissions.v1.EstimateAdmissionResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.PreviewAuditTrailRequest;
import com.acme.audit.v1.PreviewAuditTrailResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.BrowseCustomersRequest;
import com.acme.customers.v1.BrowseCustomersResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationRequest;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.AdvanceInvoiceBatchWorkflowRequest;
import com.acme.invoicing.v1.AdvanceInvoiceBatchWorkflowResponse;
import com.acme.invoicing.v1.CancelInvoiceBatchWorkflowRequest;
import com.acme.invoicing.v1.CancelInvoiceBatchWorkflowResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EscalateInvoiceBatchRequest;
import com.acme.invoicing.v1.EscalateInvoiceBatchResponse;
import com.acme.invoicing.v1.EscalateInvoiceTemplateRequest;
import com.acme.invoicing.v1.EscalateInvoiceTemplateResponse;
import com.acme.invoicing.v1.FindInvoiceTemplatesRequest;
import com.acme.invoicing.v1.FindInvoiceTemplatesResponse;
import com.acme.invoicing.v1.InspectInvoiceBatchWorkflowRequest;
import com.acme.invoicing.v1.InspectInvoiceBatchWorkflowResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchApprovalServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchCardSettlement;
import com.acme.invoicing.v1.InvoiceBatchLine;
import com.acme.invoicing.v1.InvoiceBatchLineTax;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateApprovalServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplateCardSettlement;
import com.acme.invoicing.v1.InvoiceTemplateLine;
import com.acme.invoicing.v1.InvoiceTemplateLineTax;
import com.acme.invoicing.v1.InvoiceTemplateQueryServiceGrpc;
import com.acme.invoicing.v1.ListInvoiceBatchWorkflowsRequest;
import com.acme.invoicing.v1.ListInvoiceBatchWorkflowsResponse;
import com.acme.invoicing.v1.PauseInvoiceBatchWorkflowRequest;
import com.acme.invoicing.v1.PauseInvoiceBatchWorkflowResponse;
import com.acme.invoicing.v1.RejectInvoiceBatchRequest;
import com.acme.invoicing.v1.RejectInvoiceBatchResponse;
import com.acme.invoicing.v1.ResumeInvoiceBatchWorkflowRequest;
import com.acme.invoicing.v1.ResumeInvoiceBatchWorkflowResponse;
import com.acme.invoicing.v1.RetryInvoiceBatchWorkflowRequest;
import com.acme.invoicing.v1.RetryInvoiceBatchWorkflowResponse;
import com.acme.invoicing.v1.StartInvoiceBatchWorkflowRequest;
import com.acme.invoicing.v1.StartInvoiceBatchWorkflowResponse;
import com.acme.procurement.v1.CommentGoodsReceiptApprovalRequest;
import com.acme.procurement.v1.CommentGoodsReceiptApprovalResponse;
import com.acme.procurement.v1.GoodsReceiptApprovalServiceGrpc;
import com.acme.procurement.v1.ListPurchaseRequisitionWorkflowsRequest;
import com.acme.procurement.v1.ListPurchaseRequisitionWorkflowsResponse;
import com.acme.procurement.v1.PurchaseRequisitionWorkflowServiceGrpc;
import com.acme.search.v1.CreateSearchIndexRequest;
import com.acme.search.v1.CreateSearchIndexResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class InvoiceBatchWorkflowApplicationTest {
    @Test
    void startInvoiceBatchWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InvoicingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveEInvoice(String tenant, EInvoice value) {}
            public void saveInvoiceBatch(String tenant, InvoiceBatch value) {}
            public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {}
        };
        var application = new InvoiceBatchWorkflowApplication(
                AdmissionCalculationServiceGrpc.newBlockingStub(channel),
                AuditTrailValidationServiceGrpc.newBlockingStub(channel),
                CustomerQueryServiceGrpc.newFutureStub(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceBatchApprovalServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateApprovalServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateQueryServiceGrpc.newBlockingStub(channel),
                GoodsReceiptApprovalServiceGrpc.newFutureStub(channel),
                PurchaseRequisitionWorkflowServiceGrpc.newBlockingStub(channel),
                repository,
                SearchIndexServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(StartInvoiceBatchWorkflowRequest.getDefaultInstance());
        var response = application.startInvoiceBatchWorkflow(request);
        var sent0 = channel.request("acme.invoicing.v1.InvoiceTemplateQueryService/FindInvoiceTemplates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "audit", "created_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "invoice_batch_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "parameters"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.procurement.v1.PurchaseRequisitionWorkflowService/ListPurchaseRequisitionWorkflows");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "audit", "updated_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateQueryService/FindInvoiceTemplates"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateQueryService/FindInvoiceTemplates"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch_id"),
                ProbeSupport.value(response, "invoice_batch", "invoice_batch_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "notes"),
                ProbeSupport.value(response, "invoice_batch", "notes"));
    }
}
