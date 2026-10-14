package com.acme.finance.domains.billing;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalReportingServiceGrpc;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetApprovalServiceGrpc;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.assets.v1.StreamDisposalMetricsRequest;
import com.acme.assets.v1.StreamDisposalMetricsResponse;
import com.acme.assets.v1.SubmitFixedAssetRequest;
import com.acme.assets.v1.SubmitFixedAssetResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.RevertAuditEventRequest;
import com.acme.audit.v1.RevertAuditEventResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountQueryServiceGrpc;
import com.acme.billing.v1.CompareInvoiceOptionsRequest;
import com.acme.billing.v1.CompareInvoiceOptionsResponse;
import com.acme.billing.v1.CountBillingAccountsRequest;
import com.acme.billing.v1.CountBillingAccountsResponse;
import com.acme.billing.v1.CountDunningTotalsRequest;
import com.acme.billing.v1.CountDunningTotalsResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.DunningComment;
import com.acme.billing.v1.DunningValidationServiceGrpc;
import com.acme.billing.v1.EstimateInvoiceRequest;
import com.acme.billing.v1.EstimateInvoiceResponse;
import com.acme.billing.v1.FetchDunningReportRequest;
import com.acme.billing.v1.FetchDunningReportResponse;
import com.acme.billing.v1.GenerateDunningReportRequest;
import com.acme.billing.v1.GenerateDunningReportResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceCalculationServiceGrpc;
import com.acme.billing.v1.InvoiceLine;
import com.acme.billing.v1.InvoiceLineTax;
import com.acme.billing.v1.ListDunningReportsRequest;
import com.acme.billing.v1.ListDunningReportsResponse;
import com.acme.billing.v1.NormalizeDunningRequest;
import com.acme.billing.v1.NormalizeDunningResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.ScheduleDunningReportRequest;
import com.acme.billing.v1.ScheduleDunningReportResponse;
import com.acme.billing.v1.ShareDunningReportRequest;
import com.acme.billing.v1.ShareDunningReportResponse;
import com.acme.billing.v1.StreamDunningMetricsRequest;
import com.acme.billing.v1.StreamDunningMetricsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.contracts.v1.AdvanceContractClauseWorkflowRequest;
import com.acme.contracts.v1.AdvanceContractClauseWorkflowResponse;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.ContractClauseBlockAction;
import com.acme.contracts.v1.ContractClauseWorkflowServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentBlockAction;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.PreviewCustomerSegmentRequest;
import com.acme.customers.v1.PreviewCustomerSegmentResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.learning.v1.CourseQueryServiceGrpc;
import com.acme.learning.v1.SummarizeCoursesRequest;
import com.acme.learning.v1.SummarizeCoursesResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.SendFiscalPeriodTestNoticeRequest;
import com.acme.ledger.v1.SendFiscalPeriodTestNoticeResponse;
import com.acme.ledger.v1.StreamFiscalPeriodNoticesRequest;
import com.acme.ledger.v1.StreamFiscalPeriodNoticesResponse;
import com.acme.notifications.v1.CountNotificationsRequest;
import com.acme.notifications.v1.CountNotificationsResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.suppliers.v1.BatchCreateSuppliersRequest;
import com.acme.suppliers.v1.BatchCreateSuppliersResponse;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class DunningReportingApplicationTest {
    @Test
    void streamDunningMetricsPreservesPlannedFields() {
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
        var application = new DunningReportingApplication(
                DisposalReportingServiceGrpc.newBlockingStub(channel),
                DisposalSyncServiceGrpc.newStub(channel),
                FixedAssetApprovalServiceGrpc.newBlockingStub(channel),
                AuditEventHistoryServiceGrpc.newBlockingStub(channel),
                BillingAccountQueryServiceGrpc.newBlockingStub(channel),
                DunningValidationServiceGrpc.newBlockingStub(channel),
                InvoiceCalculationServiceGrpc.newBlockingStub(channel),
                ContractClauseWorkflowServiceGrpc.newBlockingStub(channel),
                CustomerSegmentValidationServiceGrpc.newBlockingStub(channel),
                CourseQueryServiceGrpc.newBlockingStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                new DunningReportingRequestMapperImpl(),
                NotificationQueryServiceGrpc.newBlockingStub(channel),
                repository,
                SupplierServiceGrpc.newStub(channel));
        var request = ProbeSupport.sample(StreamDunningMetricsRequest.getDefaultInstance());
        var response = application.streamDunningMetrics(request);
        var sent0 = channel.request("acme.notifications.v1.NotificationQueryService/CountNotifications");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "owner_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(sent0, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "query"),
                ProbeSupport.value(sent0, "filter", "query"));
        var sent1 = channel.request("acme.audit.v1.AuditEventHistoryService/RevertAuditEvent");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationQueryService/CountNotifications"), "result", "correlation_id"),
                ProbeSupport.value(sent1, "audit_event", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent1, "details", "author_email"));
        var sent2 = channel.request("acme.billing.v1.InvoiceCalculationService/EstimateInvoice");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventHistoryService/RevertAuditEvent"), "audit_event", "subject", "display_name"),
                ProbeSupport.value(sent2, "invoice", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventHistoryService/RevertAuditEvent"), "audit_event", "subject", "number"),
                ProbeSupport.value(sent2, "invoice", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationQueryService/CountNotifications"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationQueryService/CountNotifications"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventHistoryService/RevertAuditEvent"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventHistoryService/RevertAuditEvent"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceCalculationService/EstimateInvoice"), "invoice", "audit", "created_at"),
                ProbeSupport.value(response, "dunning", "comments", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(response, "dunning", "requester", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "name"),
                ProbeSupport.value(response, "dunning", "requester", "name"));
        var endpoint = new DunningReportingEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamDunningMetricsResponse> observer = new StreamObserver<>() {
            public void onNext(StreamDunningMetricsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamDunningMetrics(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
