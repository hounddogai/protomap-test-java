package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.CalculateAllocationRequest;
import com.acme.accounting.v1.CalculateAllocationResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsValidationServiceGrpc;
import com.acme.accounting.v1.CheckChartOfAccountsEligibilityRequest;
import com.acme.accounting.v1.CheckChartOfAccountsEligibilityResponse;
import com.acme.accounting.v1.CompareAllocationOptionsRequest;
import com.acme.accounting.v1.CompareAllocationOptionsResponse;
import com.acme.accounting.v1.EstimateAllocationRequest;
import com.acme.accounting.v1.EstimateAllocationResponse;
import com.acme.accounting.v1.GetAllocationRatesRequest;
import com.acme.accounting.v1.GetAllocationRatesResponse;
import com.acme.accounting.v1.RecalculateAllocationsRequest;
import com.acme.accounting.v1.RecalculateAllocationsResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ScoreChartOfAccountsRequest;
import com.acme.accounting.v1.ScoreChartOfAccountsResponse;
import com.acme.accounting.v1.SimulateAllocationRequest;
import com.acme.accounting.v1.SimulateAllocationResponse;
import com.acme.accounting.v1.ValidateChartOfAccountsRequest;
import com.acme.accounting.v1.ValidateChartOfAccountsResponse;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.ShareRetentionPolicyExportRequest;
import com.acme.audit.v1.ShareRetentionPolicyExportResponse;
import com.acme.banking.v1.ExplainStandingOrderDecisionRequest;
import com.acme.banking.v1.ExplainStandingOrderDecisionResponse;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.PreviewMandateRequest;
import com.acme.banking.v1.PreviewMandateResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.CreateUserRequest;
import com.acme.identity.v1.CreateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptValidationServiceGrpc;
import com.acme.procurement.v1.LintGoodsReceiptsRequest;
import com.acme.procurement.v1.LintGoodsReceiptsResponse;
import com.acme.risk.v1.CountRiskScenariosRequest;
import com.acme.risk.v1.CountRiskScenariosResponse;
import com.acme.risk.v1.RiskScenarioQueryServiceGrpc;
import com.acme.search.v1.BrowseSearchIndexesRequest;
import com.acme.search.v1.BrowseSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.venues.v1.SummarizeVenuesRequest;
import com.acme.venues.v1.SummarizeVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
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

class AllocationCalculationApplicationTest {
    @Test
    void estimateAllocationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AccountingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAccrual(String tenant, Accrual value) {}
            public void saveAllocation(String tenant, Allocation value) {}
            public void saveChartOfAccounts(String tenant, ChartOfAccounts value) {}
            public void saveReconciliation(String tenant, Reconciliation value) {}
        };
        var application = new AllocationCalculationApplication(
                ChartOfAccountsValidationServiceGrpc.newBlockingStub(channel),
                RetentionPolicyExportServiceGrpc.newBlockingStub(channel),
                StandingOrderValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                UserServiceGrpc.newBlockingStub(channel),
                GoodsReceiptValidationServiceGrpc.newBlockingStub(channel),
                repository,
                RiskScenarioQueryServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SearchIndexQueryServiceGrpc.newBlockingStub(channel),
                VenueQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(EstimateAllocationRequest.getDefaultInstance());
        var response = application.estimateAllocation(request);
        var sent0 = channel.request("acme.venues.v1.VenueQueryService/SummarizeVenues");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "allocation", "block", "reason"),
                ProbeSupport.value(sent0, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        var sent1 = channel.request("acme.identity.v1.UserService/CreateUser");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "allocation", "audit", "updated_by"),
                ProbeSupport.value(sent1, "user", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "allocation", "owner", "user_id"),
                ProbeSupport.value(sent1, "user", "relationship_manager", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "locality"),
                ProbeSupport.value(sent1, "user", "primary_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "allocation", "audit", "etag"),
                ProbeSupport.value(sent1, "user", "audit", "etag"));
        var sent2 = channel.request("acme.audit.v1.RetentionPolicyExportService/ShareRetentionPolicyExport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserService/CreateUser"), "user", "relationship_manager", "email"),
                ProbeSupport.value(sent2, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserService/CreateUser"), "user", "contact", "preferred_channel"),
                ProbeSupport.value(sent2, "details", "recipient", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserService/CreateUser"), "user", "contact", "name"),
                ProbeSupport.value(sent2, "details", "recipient", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueQueryService/SummarizeVenues"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueQueryService/SummarizeVenues"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyExportService/ShareRetentionPolicyExport"), "retention_policy", "audit", "updated_at"),
                ProbeSupport.value(response, "allocation", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyExportService/ShareRetentionPolicyExport"), "retention_policy", "effective"),
                ProbeSupport.value(response, "allocation", "effective"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "allocation_id"),
                ProbeSupport.value(response, "allocation", "allocation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "currency_code"),
                ProbeSupport.value(response, "breakdown", "currency_code"));
    }
}
