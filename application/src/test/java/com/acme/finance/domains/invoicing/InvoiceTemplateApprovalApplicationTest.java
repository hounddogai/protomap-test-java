package com.acme.finance.domains.invoicing;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalApprovalServiceGrpc;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.RecallDisposalRequest;
import com.acme.assets.v1.RecallDisposalResponse;
import com.acme.assets.v1.RejectDisposalRequest;
import com.acme.assets.v1.RejectDisposalResponse;
import com.acme.assets.v1.SubmitDisposalRequest;
import com.acme.assets.v1.SubmitDisposalResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.AuditTrailLine;
import com.acme.audit.v1.AuditTrailLineTax;
import com.acme.audit.v1.RetryAuditTrailExportRequest;
import com.acme.audit.v1.RetryAuditTrailExportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.BrowseHouseholdsRequest;
import com.acme.customers.v1.BrowseHouseholdsResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.ExplainHouseholdCalculationRequest;
import com.acme.customers.v1.ExplainHouseholdCalculationResponse;
import com.acme.customers.v1.FindHouseholdsRequest;
import com.acme.customers.v1.FindHouseholdsResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.LintCustomerSegmentsRequest;
import com.acme.customers.v1.LintCustomerSegmentsResponse;
import com.acme.customers.v1.RecalculateCustomersRequest;
import com.acme.customers.v1.RecalculateCustomersResponse;
import com.acme.customers.v1.ResolveCustomersRequest;
import com.acme.customers.v1.ResolveCustomersResponse;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.SuggestDocumentsRequest;
import com.acme.documents.v1.SuggestDocumentsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.ApproveInvoiceTemplateRequest;
import com.acme.invoicing.v1.ApproveInvoiceTemplateResponse;
import com.acme.invoicing.v1.CalculateInvoiceBatchRequest;
import com.acme.invoicing.v1.CalculateInvoiceBatchResponse;
import com.acme.invoicing.v1.CommentInvoiceTemplateApprovalRequest;
import com.acme.invoicing.v1.CommentInvoiceTemplateApprovalResponse;
import com.acme.invoicing.v1.DelegateInvoiceTemplateApprovalRequest;
import com.acme.invoicing.v1.DelegateInvoiceTemplateApprovalResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EInvoiceValidationServiceGrpc;
import com.acme.invoicing.v1.EscalateInvoiceTemplateRequest;
import com.acme.invoicing.v1.EscalateInvoiceTemplateResponse;
import com.acme.invoicing.v1.GetInvoiceTemplateApprovalStatusRequest;
import com.acme.invoicing.v1.GetInvoiceTemplateApprovalStatusResponse;
import com.acme.invoicing.v1.GetInvoiceTemplateRulesRequest;
import com.acme.invoicing.v1.GetInvoiceTemplateRulesResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchApprovalServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchAttachment;
import com.acme.invoicing.v1.InvoiceBatchCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchCardSettlement;
import com.acme.invoicing.v1.InvoiceBatchLine;
import com.acme.invoicing.v1.InvoiceBatchLineTax;
import com.acme.invoicing.v1.InvoiceBatchRef;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateLifecycleServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplateValidationServiceGrpc;
import com.acme.invoicing.v1.ListPendingInvoiceTemplatesRequest;
import com.acme.invoicing.v1.ListPendingInvoiceTemplatesResponse;
import com.acme.invoicing.v1.NormalizeEInvoiceRequest;
import com.acme.invoicing.v1.NormalizeEInvoiceResponse;
import com.acme.invoicing.v1.RecalculateInvoiceBatchesRequest;
import com.acme.invoicing.v1.RecalculateInvoiceBatchesResponse;
import com.acme.invoicing.v1.RecallInvoiceBatchRequest;
import com.acme.invoicing.v1.RecallInvoiceBatchResponse;
import com.acme.invoicing.v1.RecallInvoiceTemplateRequest;
import com.acme.invoicing.v1.RecallInvoiceTemplateResponse;
import com.acme.invoicing.v1.RejectInvoiceBatchRequest;
import com.acme.invoicing.v1.RejectInvoiceBatchResponse;
import com.acme.invoicing.v1.RejectInvoiceTemplateRequest;
import com.acme.invoicing.v1.RejectInvoiceTemplateResponse;
import com.acme.invoicing.v1.RenewInvoiceTemplateRequest;
import com.acme.invoicing.v1.RenewInvoiceTemplateResponse;
import com.acme.invoicing.v1.SubmitInvoiceTemplateRequest;
import com.acme.invoicing.v1.SubmitInvoiceTemplateResponse;
import com.acme.notifications.v1.ListNotificationsRequest;
import com.acme.notifications.v1.ListNotificationsResponse;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.risk.v1.ApproveStressTestRequest;
import com.acme.risk.v1.ApproveStressTestResponse;
import com.acme.risk.v1.RetryRiskRegisterWorkflowRequest;
import com.acme.risk.v1.RetryRiskRegisterWorkflowResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterWorkflowServiceGrpc;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestApprovalServiceGrpc;
import com.acme.risk.v1.StressTestCardSettlement;
import com.acme.suppliers.v1.ListPendingSupplierRatingsRequest;
import com.acme.suppliers.v1.ListPendingSupplierRatingsResponse;
import com.acme.suppliers.v1.SupplierRatingApprovalServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class InvoiceTemplateApprovalApplicationTest {
    @Test
    void escalateInvoiceTemplatePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InvoicingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveEInvoice(String tenant, EInvoice value) {}
            public void saveInvoiceBatch(String tenant, InvoiceBatch value) {}
            public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {}
        };
        var listener = new InvoiceTemplateApprovalApplicationEvents(HouseholdCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof InvoiceTemplateApprovalApplication.RejectInvoiceTemplateStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new InvoiceTemplateApprovalApplication(
                DisposalApprovalServiceGrpc.newBlockingStub(channel),
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                CustomerCalculationServiceGrpc.newBlockingStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                CustomerSegmentValidationServiceGrpc.newBlockingStub(channel),
                HouseholdQueryServiceGrpc.newBlockingStub(channel),
                DocumentQueryServiceGrpc.newBlockingStub(channel),
                events,
                EInvoiceValidationServiceGrpc.newBlockingStub(channel),
                InvoiceBatchApprovalServiceGrpc.newBlockingStub(channel),
                InvoiceBatchCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateLifecycleServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateValidationServiceGrpc.newBlockingStub(channel),
                new InvoiceTemplateApprovalRequestMapperImpl(),
                NotificationServiceGrpc.newBlockingStub(channel),
                repository,
                RiskRegisterWorkflowServiceGrpc.newBlockingStub(channel),
                StressTestApprovalServiceGrpc.newBlockingStub(channel),
                SupplierRatingApprovalServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(EscalateInvoiceTemplateRequest.getDefaultInstance());
        var response = application.escalateInvoiceTemplate(request);
        var sent0 = channel.request("acme.invoicing.v1.InvoiceTemplateValidationService/GetInvoiceTemplateRules");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "invoice_template_id"),
                ProbeSupport.value(sent0, "invoice_template_id"));
        var sent1 = channel.request("acme.risk.v1.StressTestApprovalService/ApproveStressTest");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateValidationService/GetInvoiceTemplateRules"), "invoice_template", "card", "network"),
                ProbeSupport.value(sent1, "stress_test", "card", "network"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateValidationService/GetInvoiceTemplateRules"), "invoice_template", "billing_address", "postal_code"),
                ProbeSupport.value(sent1, "stress_test", "billing_address", "postal_code"));
        var sent2 = channel.request("acme.customers.v1.HouseholdQueryService/BrowseHouseholds");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateValidationService/GetInvoiceTemplateRules"), "invoice_template", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "audit", "created_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.StressTestApprovalService/ApproveStressTest"), "stress_test", "counterparty", "customer_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        var sent3 = channel.request("acme.customers.v1.CustomerSegmentValidationService/LintCustomerSegments");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.StressTestApprovalService/ApproveStressTest"), "stress_test", "audit", "updated_by"),
                ProbeSupport.value(sent3, "customer_segment", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.StressTestApprovalService/ApproveStressTest"), "stress_test", "counterparty", "email"),
                ProbeSupport.value(sent3, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.StressTestApprovalService/ApproveStressTest"), "stress_test", "audit", "updated_at"),
                ProbeSupport.value(sent3, "customer_segment", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent3, "customer_segment", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateValidationService/GetInvoiceTemplateRules"), "invoice_template", "total"),
                ProbeSupport.value(response, "invoice_template", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.StressTestApprovalService/ApproveStressTest"), "outcome", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.StressTestApprovalService/ApproveStressTest"), "stress_test", "notes"),
                ProbeSupport.value(response, "invoice_template", "notes"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdQueryService/BrowseHouseholds"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdQueryService/BrowseHouseholds"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerSegmentValidationService/LintCustomerSegments"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template_id"),
                ProbeSupport.value(response, "invoice_template", "invoice_template_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new InvoiceTemplateApprovalApplicationEvents(HouseholdCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ExplainHouseholdCalculationResponse>();
        listener.on(new InvoiceTemplateApprovalApplication.RejectInvoiceTemplateStage1Requested(ProbeSupport.sample(ExplainHouseholdCalculationRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
