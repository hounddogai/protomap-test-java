package com.acme.finance.domains.procurement;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.concessions.v1.BatchGetConcessionStandsRequest;
import com.acme.concessions.v1.BatchGetConcessionStandsResponse;
import com.acme.concessions.v1.ConcessionStandServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.SearchUsersRequest;
import com.acme.identity.v1.SearchUsersResponse;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.QuotePolicyRequest;
import com.acme.insurance.v1.QuotePolicyResponse;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.ListLedgerAccountsRequest;
import com.acme.ledger.v1.ListLedgerAccountsResponse;
import com.acme.notifications.v1.NormalizeNotificationRequest;
import com.acme.notifications.v1.NormalizeNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.procurement.v1.CalculateRfqRequest;
import com.acme.procurement.v1.CalculateRfqResponse;
import com.acme.procurement.v1.CheckRfqEligibilityRequest;
import com.acme.procurement.v1.CheckRfqEligibilityResponse;
import com.acme.procurement.v1.EstimateRfqRequest;
import com.acme.procurement.v1.EstimateRfqResponse;
import com.acme.procurement.v1.ExplainRfqDecisionRequest;
import com.acme.procurement.v1.ExplainRfqDecisionResponse;
import com.acme.procurement.v1.GetRfqRulesRequest;
import com.acme.procurement.v1.GetRfqRulesResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.LintRfqsRequest;
import com.acme.procurement.v1.LintRfqsResponse;
import com.acme.procurement.v1.NormalizeRfqRequest;
import com.acme.procurement.v1.NormalizeRfqResponse;
import com.acme.procurement.v1.PreviewRfqRequest;
import com.acme.procurement.v1.PreviewRfqResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderRef;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionCalculationServiceGrpc;
import com.acme.procurement.v1.QuotePurchaseOrderRequest;
import com.acme.procurement.v1.QuotePurchaseOrderResponse;
import com.acme.procurement.v1.QuotePurchaseRequisitionRequest;
import com.acme.procurement.v1.QuotePurchaseRequisitionResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.procurement.v1.RfqComment;
import com.acme.procurement.v1.RfqSla;
import com.acme.procurement.v1.ScoreRfqRequest;
import com.acme.procurement.v1.ScoreRfqResponse;
import com.acme.procurement.v1.ValidateRfqRequest;
import com.acme.procurement.v1.ValidateRfqResponse;
import com.acme.telehealth.v1.VideoVisitServiceGrpc;
import com.acme.telehealth.v1.WatchVideoVisitsRequest;
import com.acme.telehealth.v1.WatchVideoVisitsResponse;
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

class RfqValidationApplicationTest {
    @Test
    void previewRfqPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ProcurementRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveGoodsReceipt(String tenant, GoodsReceipt value) {}
            public void savePurchaseOrder(String tenant, PurchaseOrder value) {}
            public void savePurchaseRequisition(String tenant, PurchaseRequisition value) {}
            public void saveRfq(String tenant, Rfq value) {}
        };
        var application = new RfqValidationApplication(
                ConcessionStandServiceGrpc.newBlockingStub(channel),
                UserServiceGrpc.newBlockingStub(channel),
                PolicyCalculationServiceGrpc.newBlockingStub(channel),
                LedgerAccountServiceGrpc.newBlockingStub(channel),
                NotificationValidationServiceGrpc.newBlockingStub(channel),
                PurchaseOrderCalculationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionCalculationServiceGrpc.newBlockingStub(channel),
                RfqCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                VideoVisitServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(PreviewRfqRequest.getDefaultInstance());
        var response = application.previewRfq(request);
        var sent0 = channel.request("acme.procurement.v1.PurchaseRequisitionCalculationService/QuotePurchaseRequisition");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "rfq", "audit", "updated_by"),
                ProbeSupport.value(sent0, "purchase_requisition", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "rfq", "customer", "display_name"),
                ProbeSupport.value(sent0, "purchase_requisition", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "rfq", "audit", "etag"),
                ProbeSupport.value(sent0, "purchase_requisition", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "rfq", "audit", "created_by"),
                ProbeSupport.value(sent0, "purchase_requisition", "audit", "created_by"));
        var sent1 = channel.request("acme.notifications.v1.NotificationValidationService/NormalizeNotification");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "preferred_channel"),
                ProbeSupport.value(sent1, "notification", "requester", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "rfq", "assignee", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseRequisitionCalculationService/QuotePurchaseRequisition"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationValidationService/NormalizeNotification"), "notification", "requester", "phone"),
                ProbeSupport.value(response, "rfq", "requester", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationValidationService/NormalizeNotification"), "notification", "assignee", "email"),
                ProbeSupport.value(response, "rfq", "requester", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "rfq_id"),
                ProbeSupport.value(response, "rfq", "rfq_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "rfq", "custom_fields"),
                ProbeSupport.value(response, "rfq", "custom_fields"));
    }
}
