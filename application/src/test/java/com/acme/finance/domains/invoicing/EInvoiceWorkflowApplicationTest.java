package com.acme.finance.domains.invoicing;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferAttachment;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.ReconcileAssetTransfersRequest;
import com.acme.assets.v1.ReconcileAssetTransfersResponse;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.ShareRetentionPolicyExportRequest;
import com.acme.audit.v1.ShareRetentionPolicyExportResponse;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.BatchCreateBankAccountsRequest;
import com.acme.banking.v1.BatchCreateBankAccountsResponse;
import com.acme.budgeting.v1.ForecastReportingServiceGrpc;
import com.acme.budgeting.v1.ListForecastReportsRequest;
import com.acme.budgeting.v1.ListForecastReportsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.QuoteConsentRecordRequest;
import com.acme.customers.v1.QuoteConsentRecordResponse;
import com.acme.customers.v1.QuoteHouseholdRequest;
import com.acme.customers.v1.QuoteHouseholdResponse;
import com.acme.customers.v1.RecalculateCustomersRequest;
import com.acme.customers.v1.RecalculateCustomersResponse;
import com.acme.customers.v1.ResolveCustomersRequest;
import com.acme.customers.v1.ResolveCustomersResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.invoicing.v1.AdvanceEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.AdvanceEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.CalculateEInvoiceRequest;
import com.acme.invoicing.v1.CalculateEInvoiceResponse;
import com.acme.invoicing.v1.CancelEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.CancelEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.CommentEInvoiceApprovalRequest;
import com.acme.invoicing.v1.CommentEInvoiceApprovalResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EInvoiceApprovalServiceGrpc;
import com.acme.invoicing.v1.EInvoiceAttachment;
import com.acme.invoicing.v1.EInvoiceCalculationServiceGrpc;
import com.acme.invoicing.v1.EInvoiceLifecycleServiceGrpc;
import com.acme.invoicing.v1.EInvoiceLine;
import com.acme.invoicing.v1.EInvoiceLineTax;
import com.acme.invoicing.v1.EInvoiceReportingServiceGrpc;
import com.acme.invoicing.v1.InspectEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.InspectEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchRef;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateApprovalServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplateCardSettlement;
import com.acme.invoicing.v1.InvoiceTemplateWorkflowServiceGrpc;
import com.acme.invoicing.v1.ListEInvoiceWorkflowsRequest;
import com.acme.invoicing.v1.ListEInvoiceWorkflowsResponse;
import com.acme.invoicing.v1.ListPendingInvoiceTemplatesRequest;
import com.acme.invoicing.v1.ListPendingInvoiceTemplatesResponse;
import com.acme.invoicing.v1.PauseEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.PauseEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.QuoteEInvoiceRequest;
import com.acme.invoicing.v1.QuoteEInvoiceResponse;
import com.acme.invoicing.v1.RejectInvoiceTemplateRequest;
import com.acme.invoicing.v1.RejectInvoiceTemplateResponse;
import com.acme.invoicing.v1.ResumeEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.ResumeEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.RetryEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.RetryEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.RetryInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.RetryInvoiceTemplateWorkflowResponse;
import com.acme.invoicing.v1.ShareEInvoiceReportRequest;
import com.acme.invoicing.v1.ShareEInvoiceReportResponse;
import com.acme.invoicing.v1.SignalEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.SignalEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.StartEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.StartEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.TerminateEInvoiceRequest;
import com.acme.invoicing.v1.TerminateEInvoiceResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCardSettlement;
import com.acme.lending.v1.LoanApplicationWorkflowServiceGrpc;
import com.acme.lending.v1.RetryLoanApplicationWorkflowRequest;
import com.acme.lending.v1.RetryLoanApplicationWorkflowResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.metrics.v1.SummarizeMetricSeriesesRequest;
import com.acme.metrics.v1.SummarizeMetricSeriesesResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexComponent;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.UpdateSearchIndexRequest;
import com.acme.search.v1.UpdateSearchIndexResponse;
import com.acme.suppliers.v1.RejectSupplierRiskRequest;
import com.acme.suppliers.v1.RejectSupplierRiskResponse;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskApprovalServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
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

class EInvoiceWorkflowApplicationTest {
    @Test
    void signalEInvoiceWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InvoicingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveEInvoice(String tenant, EInvoice value) {}
            public void saveInvoiceBatch(String tenant, InvoiceBatch value) {}
            public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {}
        };
        var application = new EInvoiceWorkflowApplication(
                AssetTransferSyncServiceGrpc.newBlockingStub(channel),
                RetentionPolicyExportServiceGrpc.newBlockingStub(channel),
                BankAccountServiceGrpc.newStub(channel),
                ForecastReportingServiceGrpc.newBlockingStub(channel),
                ConsentRecordCalculationServiceGrpc.newBlockingStub(channel),
                CustomerCalculationServiceGrpc.newBlockingStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                HouseholdCalculationServiceGrpc.newBlockingStub(channel),
                UnderwritingSyncServiceGrpc.newStub(channel),
                EInvoiceApprovalServiceGrpc.newBlockingStub(channel),
                EInvoiceCalculationServiceGrpc.newBlockingStub(channel),
                EInvoiceLifecycleServiceGrpc.newBlockingStub(channel),
                EInvoiceReportingServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateApprovalServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateWorkflowServiceGrpc.newBlockingStub(channel),
                LoanApplicationWorkflowServiceGrpc.newBlockingStub(channel),
                new EInvoiceWorkflowRequestMapperImpl(),
                MetricSeriesQueryServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SearchIndexServiceGrpc.newBlockingStub(channel),
                SupplierRiskApprovalServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SignalEInvoiceWorkflowRequest.getDefaultInstance());
        var response = application.signalEInvoiceWorkflow(request);
        var sent0 = channel.request("acme.metrics.v1.MetricSeriesQueryService/SummarizeMetricSerieses");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        var sent1 = channel.request("acme.customers.v1.CustomerCalculationService/RecalculateCustomers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "billing_address", "lines"),
                ProbeSupport.value(sent1, "customer", "primary_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "audit", "updated_at"),
                ProbeSupport.value(sent1, "customer", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "billing_address", "recipient"),
                ProbeSupport.value(sent1, "details", "destination", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "customer", "relationship_manager", "display_name"));
        var sent2 = channel.request("acme.search.v1.SearchIndexService/UpdateSearchIndex");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "lines", "quantity"),
                ProbeSupport.value(sent2, "search_index", "components", "quantity"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "lines", "description"),
                ProbeSupport.value(sent2, "search_index", "components", "description"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "audit", "updated_by"),
                ProbeSupport.value(sent2, "search_index", "audit", "updated_by"));
        var sent3 = channel.request("acme.suppliers.v1.SupplierRiskApprovalService/RejectSupplierRisk");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerCalculationService/RecalculateCustomers"), "customer", "relationship_manager", "display_name"),
                ProbeSupport.value(sent3, "supplier_risk", "assignee", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerCalculationService/RecalculateCustomers"), "customer", "audit", "created_at"),
                ProbeSupport.value(sent3, "supplier_risk", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerCalculationService/RecalculateCustomers"), "customer", "contact", "name"),
                ProbeSupport.value(sent3, "supplier_risk", "requester", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesQueryService/SummarizeMetricSerieses"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesQueryService/SummarizeMetricSerieses"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerCalculationService/RecalculateCustomers"), "customer", "primary_address", "region"),
                ProbeSupport.value(response, "e_invoice", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRiskApprovalService/RejectSupplierRisk"), "outcome", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice_id"),
                ProbeSupport.value(response, "e_invoice", "e_invoice_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "notes"),
                ProbeSupport.value(response, "e_invoice", "notes"));
    }
}
