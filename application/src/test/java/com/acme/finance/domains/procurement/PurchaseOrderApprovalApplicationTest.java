package com.acme.finance.domains.procurement;

import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetLifecycleServiceGrpc;
import com.acme.assets.v1.ScheduleFixedAssetExpiryRequest;
import com.acme.assets.v1.ScheduleFixedAssetExpiryResponse;
import com.acme.budgeting.v1.DelegateForecastApprovalRequest;
import com.acme.budgeting.v1.DelegateForecastApprovalResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.ExportCustomersRequest;
import com.acme.customers.v1.ExportCustomersResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.QuotePreferenceRequest;
import com.acme.customers.v1.QuotePreferenceResponse;
import com.acme.customers.v1.SimulatePreferenceRequest;
import com.acme.customers.v1.SimulatePreferenceResponse;
import com.acme.customers.v1.ValidatePreferenceRequest;
import com.acme.customers.v1.ValidatePreferenceResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentCardSettlement;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.ValidateDocumentRequest;
import com.acme.documents.v1.ValidateDocumentResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageApprovalServiceGrpc;
import com.acme.insurance.v1.CoverageRouteAction;
import com.acme.insurance.v1.SubmitCoverageRequest;
import com.acme.insurance.v1.SubmitCoverageResponse;
import com.acme.notifications.v1.CloneNotificationRequest;
import com.acme.notifications.v1.CloneNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.procurement.v1.ApprovePurchaseOrderRequest;
import com.acme.procurement.v1.ApprovePurchaseOrderResponse;
import com.acme.procurement.v1.CommentPurchaseOrderApprovalRequest;
import com.acme.procurement.v1.CommentPurchaseOrderApprovalResponse;
import com.acme.procurement.v1.EscalatePurchaseOrderRequest;
import com.acme.procurement.v1.EscalatePurchaseOrderResponse;
import com.acme.procurement.v1.ExpirePurchaseOrderRequest;
import com.acme.procurement.v1.ExpirePurchaseOrderResponse;
import com.acme.procurement.v1.FindPurchaseRequisitionsRequest;
import com.acme.procurement.v1.FindPurchaseRequisitionsResponse;
import com.acme.procurement.v1.GetPurchaseOrderApprovalStatusRequest;
import com.acme.procurement.v1.GetPurchaseOrderApprovalStatusResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.LintPurchaseOrdersRequest;
import com.acme.procurement.v1.LintPurchaseOrdersResponse;
import com.acme.procurement.v1.ListPendingPurchaseOrdersRequest;
import com.acme.procurement.v1.ListPendingPurchaseOrdersResponse;
import com.acme.procurement.v1.NormalizeRfqRequest;
import com.acme.procurement.v1.NormalizeRfqResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderLifecycleServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderValidationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionQueryServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.QuotePurchaseRequisitionRequest;
import com.acme.procurement.v1.QuotePurchaseRequisitionResponse;
import com.acme.procurement.v1.RecallPurchaseOrderRequest;
import com.acme.procurement.v1.RecallPurchaseOrderResponse;
import com.acme.procurement.v1.RejectPurchaseOrderRequest;
import com.acme.procurement.v1.RejectPurchaseOrderResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqComment;
import com.acme.procurement.v1.RfqReportingServiceGrpc;
import com.acme.procurement.v1.RfqValidationServiceGrpc;
import com.acme.procurement.v1.StreamRfqMetricsRequest;
import com.acme.procurement.v1.StreamRfqMetricsResponse;
import com.acme.procurement.v1.SubmitPurchaseOrderRequest;
import com.acme.procurement.v1.SubmitPurchaseOrderResponse;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class PurchaseOrderApprovalApplicationTest {
    @Test
    void commentPurchaseOrderApprovalPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ProcurementRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveGoodsReceipt(String tenant, GoodsReceipt value) {}
            public void savePurchaseOrder(String tenant, PurchaseOrder value) {}
            public void savePurchaseRequisition(String tenant, PurchaseRequisition value) {}
            public void saveRfq(String tenant, Rfq value) {}
        };
        var application = new PurchaseOrderApprovalApplication(
                FixedAssetLifecycleServiceGrpc.newBlockingStub(channel),
                ForecastApprovalServiceGrpc.newBlockingStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                PreferenceCalculationServiceGrpc.newBlockingStub(channel),
                PreferenceValidationServiceGrpc.newBlockingStub(channel),
                DocumentValidationServiceGrpc.newBlockingStub(channel),
                CoverageApprovalServiceGrpc.newBlockingStub(channel),
                new PurchaseOrderApprovalRequestMapperImpl(),
                NotificationServiceGrpc.newBlockingStub(channel),
                PurchaseOrderLifecycleServiceGrpc.newBlockingStub(channel),
                PurchaseOrderValidationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionCalculationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionQueryServiceGrpc.newBlockingStub(channel),
                RfqReportingServiceGrpc.newBlockingStub(channel),
                RfqValidationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(CommentPurchaseOrderApprovalRequest.getDefaultInstance());
        var response = application.commentPurchaseOrderApproval(request);
        var sent0 = channel.request("acme.budgeting.v1.ForecastApprovalService/DelegateForecastApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_order", "audit", "etag"),
                ProbeSupport.value(sent0, "forecast", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_order", "audit", "updated_by"),
                ProbeSupport.value(sent0, "forecast", "audit", "updated_by"));
        var sent1 = channel.request("acme.procurement.v1.PurchaseOrderLifecycleService/ExpirePurchaseOrder");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_order", "purchase_order_id"),
                ProbeSupport.value(sent1, "purchase_order_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastApprovalService/DelegateForecastApproval"), "forecast", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "details", "new_owner", "display_name"));
        var sent2 = channel.request("acme.procurement.v1.RfqReportingService/StreamRfqMetrics");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_order", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastApprovalService/DelegateForecastApproval"), "forecast", "number"),
                ProbeSupport.value(response, "purchase_order", "counterparty", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastApprovalService/DelegateForecastApproval"), "outcome", "reference_id"),
                ProbeSupport.value(response, "outcome", "reference_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseOrderLifecycleService/ExpirePurchaseOrder"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_order_id"),
                ProbeSupport.value(response, "purchase_order", "purchase_order_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_order", "billing_address"),
                ProbeSupport.value(response, "purchase_order", "billing_address"));
    }
}
