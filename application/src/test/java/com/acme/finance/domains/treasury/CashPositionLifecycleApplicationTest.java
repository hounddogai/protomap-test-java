package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.ScoreAuditEventRequest;
import com.acme.audit.v1.ScoreAuditEventResponse;
import com.acme.audit.v1.StreamRetentionPolicyNoticesRequest;
import com.acme.audit.v1.StreamRetentionPolicyNoticesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CreateCustomerRequest;
import com.acme.customers.v1.CreateCustomerResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.SimulateCustomerSegmentRequest;
import com.acme.customers.v1.SimulateCustomerSegmentResponse;
import com.acme.customers.v1.UpdateCustomerRequest;
import com.acme.customers.v1.UpdateCustomerResponse;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.FindDocumentsRequest;
import com.acme.documents.v1.FindDocumentsResponse;
import com.acme.expenses.v1.MileageClaimLifecycleServiceGrpc;
import com.acme.expenses.v1.ScheduleMileageClaimExpiryRequest;
import com.acme.expenses.v1.ScheduleMileageClaimExpiryResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.treasury.v1.ActivateCashPositionRequest;
import com.acme.treasury.v1.ActivateCashPositionResponse;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferReportingServiceGrpc;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.ExpireCashPositionRequest;
import com.acme.treasury.v1.ExpireCashPositionResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.GetCashPositionLifecycleRequest;
import com.acme.treasury.v1.GetCashPositionLifecycleResponse;
import com.acme.treasury.v1.LintLiquidityForecastsRequest;
import com.acme.treasury.v1.LintLiquidityForecastsResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastImportServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastValidationServiceGrpc;
import com.acme.treasury.v1.ListLiquidityForecastImportsRequest;
import com.acme.treasury.v1.ListLiquidityForecastImportsResponse;
import com.acme.treasury.v1.ReinstateCashPositionRequest;
import com.acme.treasury.v1.ReinstateCashPositionResponse;
import com.acme.treasury.v1.RenewCashPositionRequest;
import com.acme.treasury.v1.RenewCashPositionResponse;
import com.acme.treasury.v1.RetryLiquidityForecastImportRequest;
import com.acme.treasury.v1.RetryLiquidityForecastImportResponse;
import com.acme.treasury.v1.ScheduleBankTransferReportRequest;
import com.acme.treasury.v1.ScheduleBankTransferReportResponse;
import com.acme.treasury.v1.ScheduleCashPositionExpiryRequest;
import com.acme.treasury.v1.ScheduleCashPositionExpiryResponse;
import com.acme.treasury.v1.ShareBankTransferReportRequest;
import com.acme.treasury.v1.ShareBankTransferReportResponse;
import com.acme.treasury.v1.SimulateLiquidityForecastRequest;
import com.acme.treasury.v1.SimulateLiquidityForecastResponse;
import com.acme.treasury.v1.SuspendCashPositionRequest;
import com.acme.treasury.v1.SuspendCashPositionResponse;
import com.acme.treasury.v1.TerminateCashPositionRequest;
import com.acme.treasury.v1.TerminateCashPositionResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class CashPositionLifecycleApplicationTest {
    @Test
    void renewCashPositionPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new CashPositionLifecycleApplication(
                AuditEventValidationServiceGrpc.newBlockingStub(channel),
                RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                CustomerServiceGrpc.newBlockingStub(channel),
                CustomerServiceGrpc.newFutureStub(channel),
                MileageClaimLifecycleServiceGrpc.newBlockingStub(channel),
                new CashPositionLifecycleRequestMapperImpl(),
                repository,
                Runnable::run,
                BankTransferReportingServiceGrpc.newBlockingStub(channel),
                LiquidityForecastImportServiceGrpc.newBlockingStub(channel),
                LiquidityForecastImportServiceGrpc.newFutureStub(channel),
                LiquidityForecastValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RenewCashPositionRequest.getDefaultInstance());
        var response = application.renewCashPosition(request);
        var sent0 = channel.request("acme.treasury.v1.BankTransferReportingService/ShareBankTransferReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "email"),
                ProbeSupport.value(sent0, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "name"),
                ProbeSupport.value(sent0, "details", "recipient", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "recipient", "preferred_channel"));
        var sent1 = channel.request("acme.treasury.v1.BankTransferReportingService/ScheduleBankTransferReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent1, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent1, "annotations"));
        var sent2 = channel.request("acme.treasury.v1.LiquidityForecastValidationService/LintLiquidityForecasts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferReportingService/ShareBankTransferReport"), "bank_transfer", "billing_address", "region"),
                ProbeSupport.value(sent2, "liquidity_forecast", "billing_address", "region"));
        var sent3 = channel.request("acme.documents.v1.DocumentQueryService/FindDocuments");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferReportingService/ShareBankTransferReport"), "bank_transfer", "billing_address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferReportingService/ShareBankTransferReport"), "bank_transfer", "audit", "updated_at"),
                ProbeSupport.value(sent3, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.LiquidityForecastValidationService/LintLiquidityForecasts"), "result", "correlation_id"),
                ProbeSupport.value(sent3, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.LiquidityForecastValidationService/LintLiquidityForecasts"), "liquidity_forecast", "metadata"),
                ProbeSupport.value(sent3, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferReportingService/ShareBankTransferReport"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferReportingService/ShareBankTransferReport"), "outcome", "next_steps"),
                ProbeSupport.value(response, "outcome", "next_steps"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferReportingService/ScheduleBankTransferReport"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.LiquidityForecastValidationService/LintLiquidityForecasts"), "outcome", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.documents.v1.DocumentQueryService/FindDocuments"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position_id"),
                ProbeSupport.value(response, "cash_position", "cash_position_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }
}
