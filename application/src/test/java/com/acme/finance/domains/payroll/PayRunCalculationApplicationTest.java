package com.acme.finance.domains.payroll;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailBankSettlement;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.PullAuditTrailsRequest;
import com.acme.audit.v1.PullAuditTrailsResponse;
import com.acme.audit.v1.ResolveAuditTrailConflictRequest;
import com.acme.audit.v1.ResolveAuditTrailConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.RetryRetentionPolicyExportRequest;
import com.acme.audit.v1.RetryRetentionPolicyExportResponse;
import com.acme.banking.v1.CalculateMandateRequest;
import com.acme.banking.v1.CalculateMandateResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.forecasting.v1.DemandForecastQueryServiceGrpc;
import com.acme.forecasting.v1.SummarizeDemandForecastsRequest;
import com.acme.forecasting.v1.SummarizeDemandForecastsResponse;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.identity.v1.WatchUsersRequest;
import com.acme.identity.v1.WatchUsersResponse;
import com.acme.insurance.v1.CheckPolicyEligibilityRequest;
import com.acme.insurance.v1.CheckPolicyEligibilityResponse;
import com.acme.insurance.v1.ComparePolicyOptionsRequest;
import com.acme.insurance.v1.ComparePolicyOptionsResponse;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyBankFunding;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.RecalculatePoliciesRequest;
import com.acme.insurance.v1.RecalculatePoliciesResponse;
import com.acme.ledger.v1.ListPostingBatchVersionsRequest;
import com.acme.ledger.v1.ListPostingBatchVersionsResponse;
import com.acme.ledger.v1.PostingBatchHistoryServiceGrpc;
import com.acme.payroll.v1.CalculatePayRunRequest;
import com.acme.payroll.v1.CalculatePayRunResponse;
import com.acme.payroll.v1.ComparePayRunOptionsRequest;
import com.acme.payroll.v1.ComparePayRunOptionsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.EstimatePayRunRequest;
import com.acme.payroll.v1.EstimatePayRunResponse;
import com.acme.payroll.v1.ExplainPayRunCalculationRequest;
import com.acme.payroll.v1.ExplainPayRunCalculationResponse;
import com.acme.payroll.v1.GetPayRunRatesRequest;
import com.acme.payroll.v1.GetPayRunRatesResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.QuotePayRunRequest;
import com.acme.payroll.v1.QuotePayRunResponse;
import com.acme.payroll.v1.SimulatePayRunRequest;
import com.acme.payroll.v1.SimulatePayRunResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptValidationServiceGrpc;
import com.acme.procurement.v1.PreviewGoodsReceiptRequest;
import com.acme.procurement.v1.PreviewGoodsReceiptResponse;
import com.acme.procurement.v1.QuoteRfqRequest;
import com.acme.procurement.v1.QuoteRfqResponse;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.SearchSearchIndexesRequest;
import com.acme.search.v1.SearchSearchIndexesResponse;
import com.acme.search.v1.SummarizeSearchIndexesRequest;
import com.acme.search.v1.SummarizeSearchIndexesResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class PayRunCalculationApplicationTest {
    @Test
    void simulatePayRunPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var application = new PayRunCalculationApplication(
                AuditTrailSyncServiceGrpc.newBlockingStub(channel),
                RetentionPolicyExportServiceGrpc.newBlockingStub(channel),
                MandateCalculationServiceGrpc.newBlockingStub(channel),
                DemandForecastQueryServiceGrpc.newFutureStub(channel),
                UserServiceGrpc.newBlockingStub(channel),
                PolicyCalculationServiceGrpc.newBlockingStub(channel),
                PolicyValidationServiceGrpc.newBlockingStub(channel),
                PostingBatchHistoryServiceGrpc.newBlockingStub(channel),
                GoodsReceiptValidationServiceGrpc.newBlockingStub(channel),
                RfqCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SearchIndexQueryServiceGrpc.newBlockingStub(channel),
                SearchIndexServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SimulatePayRunRequest.getDefaultInstance());
        var response = application.simulatePayRun(request);
        var sent0 = channel.request("acme.audit.v1.RetentionPolicyExportService/RetryRetentionPolicyExport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "counterparty", "version"),
                ProbeSupport.value(sent0, "retention_policy", "version"));
        var sent1 = channel.request("acme.audit.v1.AuditTrailSyncService/ResolveAuditTrailConflict");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "billing_address", "recipient"),
                ProbeSupport.value(sent1, "audit_trail", "billing_address", "recipient"));
        var sent2 = channel.request("acme.procurement.v1.RfqCalculationService/QuoteRfq");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailSyncService/ResolveAuditTrailConflict"), "audit_trail", "billing_address", "country_code"),
                ProbeSupport.value(sent2, "details", "destination", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "postal_code"),
                ProbeSupport.value(sent2, "details", "destination", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyExportService/RetryRetentionPolicyExport"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyExportService/RetryRetentionPolicyExport"), "retention_policy", "audit", "etag"),
                ProbeSupport.value(response, "pay_run", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailSyncService/ResolveAuditTrailConflict"), "audit_trail", "audit", "updated_by"),
                ProbeSupport.value(response, "pay_run", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqCalculationService/QuoteRfq"), "rfq", "sla", "due_at"),
                ProbeSupport.value(response, "pay_run", "due_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run_id"),
                ProbeSupport.value(response, "pay_run", "pay_run_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "total"),
                ProbeSupport.value(response, "pay_run", "total"));
    }
}
