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
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class PurchaseOrderApprovalRequestMapperImpl implements PurchaseOrderApprovalRequestMapper {
    @Override
    public SimulatePreferenceRequest submitPurchaseOrderStage1(SubmitPurchaseOrderRequest request, LintPurchaseOrdersResponse reply0) {
        return SimulatePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulatePreferenceRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getPurchaseOrder().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getPurchaseOrder().getAudit().getCreatedBy())
                                .setUpdatedAt(reply0.getPurchaseOrder().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
    }
}
