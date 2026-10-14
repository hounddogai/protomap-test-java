package com.acme.finance.domains.risk;

import com.acme.banking.v1.CommentStatementApprovalRequest;
import com.acme.banking.v1.CommentStatementApprovalResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.banking.v1.MandateLine;
import com.acme.banking.v1.MandateLineTax;
import com.acme.banking.v1.RecallMandateRequest;
import com.acme.banking.v1.RecallMandateResponse;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.StatementApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.EstimateConsentRecordRequest;
import com.acme.customers.v1.EstimateConsentRecordResponse;
import com.acme.customers.v1.EstimateHouseholdRequest;
import com.acme.customers.v1.EstimateHouseholdResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.QuoteHouseholdRequest;
import com.acme.customers.v1.QuoteHouseholdResponse;
import com.acme.customers.v1.QuotePreferenceRequest;
import com.acme.customers.v1.QuotePreferenceResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimWorkflowServiceGrpc;
import com.acme.insurance.v1.ListPremiumConflictsRequest;
import com.acme.insurance.v1.ListPremiumConflictsResponse;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.ReconcileUnderwritingsRequest;
import com.acme.insurance.v1.ReconcileUnderwritingsResponse;
import com.acme.insurance.v1.RetryClaimWorkflowRequest;
import com.acme.insurance.v1.RetryClaimWorkflowResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchBankSettlement;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.ScorePostingBatchRequest;
import com.acme.ledger.v1.ScorePostingBatchResponse;
import com.acme.risk.v1.AdvanceExposureWorkflowRequest;
import com.acme.risk.v1.AdvanceExposureWorkflowResponse;
import com.acme.risk.v1.CancelExposureWorkflowRequest;
import com.acme.risk.v1.CancelExposureWorkflowResponse;
import com.acme.risk.v1.EstimateExposureRequest;
import com.acme.risk.v1.EstimateExposureResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureBankFunding;
import com.acme.risk.v1.ExposureCalculationServiceGrpc;
import com.acme.risk.v1.InspectExposureWorkflowRequest;
import com.acme.risk.v1.InspectExposureWorkflowResponse;
import com.acme.risk.v1.PauseExposureWorkflowRequest;
import com.acme.risk.v1.PauseExposureWorkflowResponse;
import com.acme.risk.v1.RejectRiskRegisterRequest;
import com.acme.risk.v1.RejectRiskRegisterResponse;
import com.acme.risk.v1.ResumeExposureWorkflowRequest;
import com.acme.risk.v1.ResumeExposureWorkflowResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterApprovalServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.SignalExposureWorkflowRequest;
import com.acme.risk.v1.SignalExposureWorkflowResponse;
import com.acme.risk.v1.StartExposureWorkflowRequest;
import com.acme.risk.v1.StartExposureWorkflowResponse;
import com.acme.risk.v1.StressTest;
import com.acme.search.v1.BrowseSearchIndexesRequest;
import com.acme.search.v1.BrowseSearchIndexesResponse;
import com.acme.search.v1.PreviewSearchIndexRequest;
import com.acme.search.v1.PreviewSearchIndexResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.sellers.v1.SellerQueryServiceGrpc;
import com.acme.sellers.v1.SuggestSellersRequest;
import com.acme.sellers.v1.SuggestSellersResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.WatchSuppliersRequest;
import com.acme.suppliers.v1.WatchSuppliersResponse;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class ExposureWorkflowApplicationTest {
    @Test
    void startExposureWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new RiskRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExposure(String tenant, Exposure value) {}
            public void saveRiskRegister(String tenant, RiskRegister value) {}
            public void saveRiskScenario(String tenant, RiskScenario value) {}
            public void saveStressTest(String tenant, StressTest value) {}
        };
        var application = new ExposureWorkflowApplication(
                MandateApprovalServiceGrpc.newBlockingStub(channel),
                StatementApprovalServiceGrpc.newBlockingStub(channel),
                ConsentRecordCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdCalculationServiceGrpc.newBlockingStub(channel),
                PreferenceCalculationServiceGrpc.newBlockingStub(channel),
                ClaimWorkflowServiceGrpc.newBlockingStub(channel),
                PremiumSyncServiceGrpc.newBlockingStub(channel),
                UnderwritingSyncServiceGrpc.newBlockingStub(channel),
                PostingBatchValidationServiceGrpc.newBlockingStub(channel),
                repository,
                ExposureCalculationServiceGrpc.newBlockingStub(channel),
                RiskRegisterApprovalServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SearchIndexQueryServiceGrpc.newFutureStub(channel),
                SearchIndexValidationServiceGrpc.newBlockingStub(channel),
                SellerQueryServiceGrpc.newBlockingStub(channel),
                SupplierServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(StartExposureWorkflowRequest.getDefaultInstance());
        var response = application.startExposureWorkflow(request);
        var sent0 = channel.request("acme.suppliers.v1.SupplierService/WatchSuppliers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "phone"),
                ProbeSupport.value(sent0, "subscriber", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "preferred_channel"),
                ProbeSupport.value(sent0, "subscriber", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "contact", "name"),
                ProbeSupport.value(sent0, "subscriber", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "email"),
                ProbeSupport.value(sent0, "subscriber", "email"));
        var sent1 = channel.request("acme.customers.v1.ConsentRecordCalculationService/EstimateConsentRecord");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "holder", "display_name"),
                ProbeSupport.value(sent1, "consent_record", "subject", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "currency_code"),
                ProbeSupport.value(sent1, "details", "currency_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent1, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent1, "reason"));
        var sent2 = channel.request("acme.banking.v1.StatementApprovalService/CommentStatementApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "audit", "created_by"),
                ProbeSupport.value(sent2, "statement", "audit", "created_by"));
        var sent3 = channel.request("acme.search.v1.SearchIndexValidationService/PreviewSearchIndex");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "email"),
                ProbeSupport.value(sent3, "search_index", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "contact", "email"),
                ProbeSupport.value(sent3, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "preferred_channel"),
                ProbeSupport.value(sent3, "details", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.ConsentRecordCalculationService/EstimateConsentRecord"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.ConsentRecordCalculationService/EstimateConsentRecord"), "consent_record", "subject", "display_name"),
                ProbeSupport.value(response, "exposure", "holder", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StatementApprovalService/CommentStatementApproval"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.search.v1.SearchIndexValidationService/PreviewSearchIndex"), "search_index", "owner", "email"),
                ProbeSupport.value(response, "exposure", "holder", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure_id"),
                ProbeSupport.value(response, "exposure", "exposure_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "opened_at"),
                ProbeSupport.value(response, "exposure", "opened_at"));
    }
}
