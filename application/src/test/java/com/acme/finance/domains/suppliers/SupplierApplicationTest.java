package com.acme.finance.domains.suppliers;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalValidationServiceGrpc;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.ScoreDisposalRequest;
import com.acme.assets.v1.ScoreDisposalResponse;
import com.acme.assets.v1.SearchFixedAssetsRequest;
import com.acme.assets.v1.SearchFixedAssetsResponse;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.ShareAuditTrailExportRequest;
import com.acme.audit.v1.ShareAuditTrailExportResponse;
import com.acme.budgeting.v1.BudgetLineQueryServiceGrpc;
import com.acme.budgeting.v1.ExportBudgetLinesRequest;
import com.acme.budgeting.v1.ExportBudgetLinesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.NormalizeObligationRequest;
import com.acme.contracts.v1.NormalizeObligationResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationValidationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ClaimQueryServiceGrpc;
import com.acme.insurance.v1.ResolveClaimsRequest;
import com.acme.insurance.v1.ResolveClaimsResponse;
import com.acme.metrics.v1.ListMetricSeriesesRequest;
import com.acme.metrics.v1.ListMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.suppliers.v1.BatchCreateSuppliersRequest;
import com.acme.suppliers.v1.BatchCreateSuppliersResponse;
import com.acme.suppliers.v1.BatchGetSuppliersRequest;
import com.acme.suppliers.v1.BatchGetSuppliersResponse;
import com.acme.suppliers.v1.BrowseSupplierContractsRequest;
import com.acme.suppliers.v1.BrowseSupplierContractsResponse;
import com.acme.suppliers.v1.CountSuppliersRequest;
import com.acme.suppliers.v1.CountSuppliersResponse;
import com.acme.suppliers.v1.CreateSupplierRequest;
import com.acme.suppliers.v1.CreateSupplierResponse;
import com.acme.suppliers.v1.DeleteSupplierRequest;
import com.acme.suppliers.v1.DeleteSupplierResponse;
import com.acme.suppliers.v1.FindSupplierContractsRequest;
import com.acme.suppliers.v1.FindSupplierContractsResponse;
import com.acme.suppliers.v1.GetSupplierRequest;
import com.acme.suppliers.v1.GetSupplierResponse;
import com.acme.suppliers.v1.LintSupplierRatingsRequest;
import com.acme.suppliers.v1.LintSupplierRatingsResponse;
import com.acme.suppliers.v1.ListSuppliersRequest;
import com.acme.suppliers.v1.ListSuppliersResponse;
import com.acme.suppliers.v1.ResolveSupplierContractsRequest;
import com.acme.suppliers.v1.ResolveSupplierContractsResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.UpdateSupplierRequest;
import com.acme.suppliers.v1.UpdateSupplierResponse;
import com.acme.suppliers.v1.WatchSuppliersRequest;
import com.acme.suppliers.v1.WatchSuppliersResponse;
import com.acme.yards.v1.ExplainYardCalculationRequest;
import com.acme.yards.v1.ExplainYardCalculationResponse;
import com.acme.yards.v1.Yard;
import com.acme.yards.v1.YardCalculationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
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

class SupplierApplicationTest {
    @Test
    void updateSupplierPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new SuppliersRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveSupplier(String tenant, Supplier value) {}
            public void saveSupplierContract(String tenant, SupplierContract value) {}
            public void saveSupplierRating(String tenant, SupplierRating value) {}
            public void saveSupplierRisk(String tenant, SupplierRisk value) {}
        };
        var application = new SupplierApplication(
                DisposalValidationServiceGrpc.newBlockingStub(channel),
                FixedAssetServiceGrpc.newBlockingStub(channel),
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                BudgetLineQueryServiceGrpc.newBlockingStub(channel),
                ObligationValidationServiceGrpc.newBlockingStub(channel),
                ClaimQueryServiceGrpc.newBlockingStub(channel),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierContractQueryServiceGrpc.newBlockingStub(channel),
                SupplierQueryServiceGrpc.newBlockingStub(channel),
                SupplierRatingValidationServiceGrpc.newBlockingStub(channel),
                YardCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(UpdateSupplierRequest.getDefaultInstance());
        var response = application.updateSupplier(request);
        var sent0 = channel.request("acme.budgeting.v1.BudgetLineQueryService/ExportBudgetLines");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier", "primary_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier", "passport", "expires_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        var sent1 = channel.request("acme.insurance.v1.ClaimQueryService/ResolveClaims");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        var sent2 = channel.request("acme.contracts.v1.ObligationValidationService/NormalizeObligation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier", "contact", "preferred_channel"),
                ProbeSupport.value(sent2, "obligation", "requester", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier", "audit", "updated_by"),
                ProbeSupport.value(sent2, "obligation", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier", "contact", "email"),
                ProbeSupport.value(sent2, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimQueryService/ResolveClaims"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationValidationService/NormalizeObligation"), "obligation", "assignee", "email"),
                ProbeSupport.value(response, "supplier", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationValidationService/NormalizeObligation"), "obligation", "requester", "preferred_channel"),
                ProbeSupport.value(response, "supplier", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier", "status"),
                ProbeSupport.value(response, "supplier", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier", "relationship_manager"),
                ProbeSupport.value(response, "supplier", "relationship_manager"));
    }

    @Test
    void watchSuppliersPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new SuppliersRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveSupplier(String tenant, Supplier value) {}
            public void saveSupplierContract(String tenant, SupplierContract value) {}
            public void saveSupplierRating(String tenant, SupplierRating value) {}
            public void saveSupplierRisk(String tenant, SupplierRisk value) {}
        };
        var application = new SupplierApplication(
                DisposalValidationServiceGrpc.newBlockingStub(channel),
                FixedAssetServiceGrpc.newBlockingStub(channel),
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                BudgetLineQueryServiceGrpc.newBlockingStub(channel),
                ObligationValidationServiceGrpc.newBlockingStub(channel),
                ClaimQueryServiceGrpc.newBlockingStub(channel),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierContractQueryServiceGrpc.newBlockingStub(channel),
                SupplierQueryServiceGrpc.newBlockingStub(channel),
                SupplierRatingValidationServiceGrpc.newBlockingStub(channel),
                YardCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(WatchSuppliersRequest.getDefaultInstance());
        var response = application.watchSuppliers(request);
        var sent0 = channel.request("acme.suppliers.v1.SupplierRatingValidationService/LintSupplierRatings");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "name"),
                ProbeSupport.value(sent0, "details", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent0, "supplier_rating", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "supplier_rating", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "phone"),
                ProbeSupport.value(sent0, "details", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRatingValidationService/LintSupplierRatings"), "supplier_rating", "billing_address", "lines"),
                ProbeSupport.value(response, "supplier", "primary_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "name"),
                ProbeSupport.value(response, "supplier", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "preferred_channel"),
                ProbeSupport.value(response, "supplier", "contact", "preferred_channel"));
        var endpoint = new SupplierEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<WatchSuppliersResponse> observer = new StreamObserver<>() {
            public void onNext(WatchSuppliersResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.watchSuppliers(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void batchCreateSuppliersPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new SuppliersRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveSupplier(String tenant, Supplier value) {}
            public void saveSupplierContract(String tenant, SupplierContract value) {}
            public void saveSupplierRating(String tenant, SupplierRating value) {}
            public void saveSupplierRisk(String tenant, SupplierRisk value) {}
        };
        var application = new SupplierApplication(
                DisposalValidationServiceGrpc.newBlockingStub(channel),
                FixedAssetServiceGrpc.newBlockingStub(channel),
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                BudgetLineQueryServiceGrpc.newBlockingStub(channel),
                ObligationValidationServiceGrpc.newBlockingStub(channel),
                ClaimQueryServiceGrpc.newBlockingStub(channel),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierContractQueryServiceGrpc.newBlockingStub(channel),
                SupplierQueryServiceGrpc.newBlockingStub(channel),
                SupplierRatingValidationServiceGrpc.newBlockingStub(channel),
                YardCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(BatchCreateSuppliersRequest.getDefaultInstance());
        var response = application.batchCreateSuppliers(request);
        var sent0 = channel.request("acme.yards.v1.YardCalculationService/ExplainYardCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "etag"),
                ProbeSupport.value(sent0, "yard", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "contact", "phone"),
                ProbeSupport.value(sent0, "details", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "updated_by"),
                ProbeSupport.value(sent0, "yard", "audit", "updated_by"));
        var sent1 = channel.request("acme.audit.v1.AuditTrailExportService/ShareAuditTrailExport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "header", "submitted_by", "email"),
                ProbeSupport.value(sent1, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "header", "submitted_by", "phone"),
                ProbeSupport.value(sent1, "details", "recipient", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "contact", "name"),
                ProbeSupport.value(sent1, "details", "recipient", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "header", "submitted_by", "preferred_channel"),
                ProbeSupport.value(sent1, "details", "recipient", "preferred_channel"));
        var sent2 = channel.request("acme.suppliers.v1.SupplierContractQueryService/BrowseSupplierContracts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailExportService/ShareAuditTrailExport"), "audit_trail", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.yards.v1.YardCalculationService/ExplainYardCalculation"), "outcome", "effective_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "relationship_manager", "user_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailExportService/ShareAuditTrailExport"), "audit_trail", "metadata"),
                ProbeSupport.value(sent2, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.yards.v1.YardCalculationService/ExplainYardCalculation"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailExportService/ShareAuditTrailExport"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        var endpoint = new SupplierEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<BatchCreateSuppliersResponse> observer = new StreamObserver<>() {
            public void onNext(BatchCreateSuppliersResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.batchCreateSuppliers(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
