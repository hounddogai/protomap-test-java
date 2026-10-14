package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.PreviewAuditTrailRequest;
import com.acme.audit.v1.PreviewAuditTrailResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineApprovalServiceGrpc;
import com.acme.budgeting.v1.EscalateBudgetLineRequest;
import com.acme.budgeting.v1.EscalateBudgetLineResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CompareHouseholdOptionsRequest;
import com.acme.customers.v1.CompareHouseholdOptionsResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.ExplainConsentRecordDecisionRequest;
import com.acme.customers.v1.ExplainConsentRecordDecisionResponse;
import com.acme.customers.v1.ExplainHouseholdDecisionRequest;
import com.acme.customers.v1.ExplainHouseholdDecisionResponse;
import com.acme.customers.v1.FindCustomersRequest;
import com.acme.customers.v1.FindCustomersResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.ScoreCustomerRequest;
import com.acme.customers.v1.ScoreCustomerResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunBankSettlement;
import com.acme.payroll.v1.PayRunWorkflowServiceGrpc;
import com.acme.payroll.v1.RetryPayRunWorkflowRequest;
import com.acme.payroll.v1.RetryPayRunWorkflowResponse;
import com.acme.performance.v1.CreateReviewRequest;
import com.acme.performance.v1.CreateReviewResponse;
import com.acme.performance.v1.Review;
import com.acme.performance.v1.ReviewLine;
import com.acme.performance.v1.ReviewServiceGrpc;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderLifecycleServiceGrpc;
import com.acme.procurement.v1.SchedulePurchaseOrderExpiryRequest;
import com.acme.procurement.v1.SchedulePurchaseOrderExpiryResponse;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionServiceGrpc;
import com.acme.treasury.v1.CountLiquidityForecastTotalsRequest;
import com.acme.treasury.v1.CountLiquidityForecastTotalsResponse;
import com.acme.treasury.v1.DeleteLiquidityForecastReportRequest;
import com.acme.treasury.v1.DeleteLiquidityForecastReportResponse;
import com.acme.treasury.v1.FetchLiquidityForecastReportRequest;
import com.acme.treasury.v1.FetchLiquidityForecastReportResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeReportingServiceGrpc;
import com.acme.treasury.v1.FxHedgeSyncServiceGrpc;
import com.acme.treasury.v1.GenerateFxHedgeReportRequest;
import com.acme.treasury.v1.GenerateFxHedgeReportResponse;
import com.acme.treasury.v1.GenerateLiquidityForecastReportRequest;
import com.acme.treasury.v1.GenerateLiquidityForecastReportResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastCardSettlement;
import com.acme.treasury.v1.LiquidityForecastValidationServiceGrpc;
import com.acme.treasury.v1.ScheduleLiquidityForecastReportRequest;
import com.acme.treasury.v1.ScheduleLiquidityForecastReportResponse;
import com.acme.treasury.v1.ScoreLiquidityForecastRequest;
import com.acme.treasury.v1.ScoreLiquidityForecastResponse;
import com.acme.treasury.v1.ShareLiquidityForecastReportRequest;
import com.acme.treasury.v1.ShareLiquidityForecastReportResponse;
import com.acme.treasury.v1.StreamLiquidityForecastMetricsRequest;
import com.acme.treasury.v1.StreamLiquidityForecastMetricsResponse;
import com.acme.treasury.v1.SyncFxHedgesRequest;
import com.acme.treasury.v1.SyncFxHedgesResponse;
import com.acme.treasury.v1.UpdateCashPositionRequest;
import com.acme.treasury.v1.UpdateCashPositionResponse;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class LiquidityForecastReportingApplicationTest {
    @Test
    void streamLiquidityForecastMetricsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new LiquidityForecastReportingApplication(
                AuditTrailValidationServiceGrpc.newBlockingStub(channel),
                BudgetLineApprovalServiceGrpc.newBlockingStub(channel),
                ConsentRecordValidationServiceGrpc.newBlockingStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                CustomerValidationServiceGrpc.newBlockingStub(channel),
                HouseholdCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                new LiquidityForecastReportingRequestMapperImpl(),
                PayRunWorkflowServiceGrpc.newBlockingStub(channel),
                ReviewServiceGrpc.newBlockingStub(channel),
                PurchaseOrderLifecycleServiceGrpc.newBlockingStub(channel),
                repository,
                CashPositionServiceGrpc.newBlockingStub(channel),
                FxHedgeReportingServiceGrpc.newBlockingStub(channel),
                FxHedgeSyncServiceGrpc.newBlockingStub(channel),
                LiquidityForecastValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(StreamLiquidityForecastMetricsRequest.getDefaultInstance());
        var response = application.streamLiquidityForecastMetrics(request);
        var sent0 = channel.request("acme.treasury.v1.CashPositionService/UpdateCashPosition");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent0, "cash_position", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "phone"),
                ProbeSupport.value(sent0, "cash_position", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "preferred_channel"),
                ProbeSupport.value(sent0, "cash_position", "contact", "preferred_channel"));
        var sent1 = channel.request("acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionService/UpdateCashPosition"), "cash_position", "audit", "created_at"),
                ProbeSupport.value(sent1, "household", "children", "children", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionService/UpdateCashPosition"), "cash_position", "contact", "email"),
                ProbeSupport.value(sent1, "household", "children", "children", "manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionService/UpdateCashPosition"), "cash_position", "audit", "updated_at"),
                ProbeSupport.value(sent1, "household", "children", "children", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionService/UpdateCashPosition"), "cash_position", "audit", "etag"),
                ProbeSupport.value(response, "liquidity_forecast", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionService/UpdateCashPosition"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision"), "household", "location", "postal_code"),
                ProbeSupport.value(response, "liquidity_forecast", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "liquidity_forecast", "billing_address", "region"));
        var endpoint = new LiquidityForecastReportingEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamLiquidityForecastMetricsResponse> observer = new StreamObserver<>() {
            public void onNext(StreamLiquidityForecastMetricsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamLiquidityForecastMetrics(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void deleteLiquidityForecastReportPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new LiquidityForecastReportingApplication(
                AuditTrailValidationServiceGrpc.newBlockingStub(channel),
                BudgetLineApprovalServiceGrpc.newBlockingStub(channel),
                ConsentRecordValidationServiceGrpc.newBlockingStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                CustomerValidationServiceGrpc.newBlockingStub(channel),
                HouseholdCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                new LiquidityForecastReportingRequestMapperImpl(),
                PayRunWorkflowServiceGrpc.newBlockingStub(channel),
                ReviewServiceGrpc.newBlockingStub(channel),
                PurchaseOrderLifecycleServiceGrpc.newBlockingStub(channel),
                repository,
                CashPositionServiceGrpc.newBlockingStub(channel),
                FxHedgeReportingServiceGrpc.newBlockingStub(channel),
                FxHedgeSyncServiceGrpc.newBlockingStub(channel),
                LiquidityForecastValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(DeleteLiquidityForecastReportRequest.getDefaultInstance());
        var response = application.deleteLiquidityForecastReport(request);
        var sent0 = channel.request("acme.budgeting.v1.BudgetLineApprovalService/EscalateBudgetLine");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "etag"),
                ProbeSupport.value(sent0, "budget_line", "audit", "etag"));
        var sent1 = channel.request("acme.customers.v1.CustomerValidationService/ScoreCustomer");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineApprovalService/EscalateBudgetLine"), "budget_line", "counterparty", "email"),
                ProbeSupport.value(sent1, "customer", "contact", "email"));
        var sent2 = channel.request("acme.audit.v1.AuditTrailValidationService/PreviewAuditTrail");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerValidationService/ScoreCustomer"), "customer", "primary_address", "country_code"),
                ProbeSupport.value(sent2, "audit_trail", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineApprovalService/EscalateBudgetLine"), "budget_line", "card", "network"),
                ProbeSupport.value(sent2, "audit_trail", "card", "network"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineApprovalService/EscalateBudgetLine"), "budget_line", "audit", "etag"),
                ProbeSupport.value(sent2, "audit_trail", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerValidationService/ScoreCustomer"), "customer", "contact", "phone"),
                ProbeSupport.value(sent2, "details", "contact", "phone"));
        var sent3 = channel.request("acme.customers.v1.ConsentRecordValidationService/ExplainConsentRecordDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerValidationService/ScoreCustomer"), "customer", "contact", "phone"),
                ProbeSupport.value(sent3, "details", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineApprovalService/EscalateBudgetLine"), "budget_line", "counterparty", "email"),
                ProbeSupport.value(sent3, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineApprovalService/EscalateBudgetLine"), "budget_line", "counterparty", "display_name"),
                ProbeSupport.value(sent3, "consent_record", "subject", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerValidationService/ScoreCustomer"), "customer", "name"),
                ProbeSupport.value(sent3, "details", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineApprovalService/EscalateBudgetLine"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineApprovalService/EscalateBudgetLine"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailValidationService/PreviewAuditTrail"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.ConsentRecordValidationService/ExplainConsentRecordDecision"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
    }
}
