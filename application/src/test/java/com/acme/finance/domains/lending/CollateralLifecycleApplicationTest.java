package com.acme.finance.domains.lending;

import com.acme.catalog.v1.CreateProductRequest;
import com.acme.catalog.v1.CreateProductResponse;
import com.acme.catalog.v1.Product;
import com.acme.catalog.v1.ProductComponent;
import com.acme.catalog.v1.ProductServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.EstimateHouseholdRequest;
import com.acme.customers.v1.EstimateHouseholdResponse;
import com.acme.customers.v1.ExplainCustomerSegmentDecisionRequest;
import com.acme.customers.v1.ExplainCustomerSegmentDecisionResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.ValidateHouseholdRequest;
import com.acme.customers.v1.ValidateHouseholdResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.ActivateCollateralRequest;
import com.acme.lending.v1.ActivateCollateralResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.CountRepaymentTotalsRequest;
import com.acme.lending.v1.CountRepaymentTotalsResponse;
import com.acme.lending.v1.EstimateRepaymentRequest;
import com.acme.lending.v1.EstimateRepaymentResponse;
import com.acme.lending.v1.GetCollateralLifecycleRequest;
import com.acme.lending.v1.GetCollateralLifecycleResponse;
import com.acme.lending.v1.LintLoanApplicationsRequest;
import com.acme.lending.v1.LintLoanApplicationsResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.LoanValidationServiceGrpc;
import com.acme.lending.v1.NormalizeCollateralRequest;
import com.acme.lending.v1.NormalizeCollateralResponse;
import com.acme.lending.v1.NormalizeLoanRequest;
import com.acme.lending.v1.NormalizeLoanResponse;
import com.acme.lending.v1.ReinstateCollateralRequest;
import com.acme.lending.v1.ReinstateCollateralResponse;
import com.acme.lending.v1.RenewCollateralRequest;
import com.acme.lending.v1.RenewCollateralResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentCalculationServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.lending.v1.SuspendCollateralRequest;
import com.acme.lending.v1.SuspendCollateralResponse;
import com.acme.lending.v1.TerminateCollateralRequest;
import com.acme.lending.v1.TerminateCollateralResponse;
import com.acme.lending.v1.ValidateRepaymentRequest;
import com.acme.lending.v1.ValidateRepaymentResponse;
import com.acme.maintenance.v1.EstimateWorkOrderRequest;
import com.acme.maintenance.v1.EstimateWorkOrderResponse;
import com.acme.maintenance.v1.WorkOrder;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexComponent;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.SearchSearchIndexesRequest;
import com.acme.search.v1.SearchSearchIndexesResponse;
import com.acme.search.v1.UpdateSearchIndexRequest;
import com.acme.search.v1.UpdateSearchIndexResponse;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class CollateralLifecycleApplicationTest {
    @Test
    void renewCollateralPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LendingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollateral(String tenant, Collateral value) {}
            public void saveLoan(String tenant, Loan value) {}
            public void saveLoanApplication(String tenant, LoanApplication value) {}
            public void saveRepayment(String tenant, Repayment value) {}
        };
        var listener = new CollateralLifecycleApplicationEvents(CustomerSegmentValidationServiceGrpc.newStub(channel), HouseholdCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof CollateralLifecycleApplication.ActivateCollateralStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof CollateralLifecycleApplication.GetCollateralLifecycleStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new CollateralLifecycleApplication(
                ProductServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                events,
                CollateralValidationServiceGrpc.newFutureStub(channel),
                LoanApplicationValidationServiceGrpc.newBlockingStub(channel),
                LoanValidationServiceGrpc.newBlockingStub(channel),
                RepaymentCalculationServiceGrpc.newBlockingStub(channel),
                RepaymentReportingServiceGrpc.newBlockingStub(channel),
                repository,
                SearchIndexServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RenewCollateralRequest.getDefaultInstance());
        var response = application.renewCollateral(request);
        var sent0 = channel.request("acme.catalog.v1.ProductService/CreateProduct");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "user_id"),
                ProbeSupport.value(sent0, "product", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "display_name"),
                ProbeSupport.value(sent0, "product", "owner", "display_name"));
        var sent1 = channel.request("acme.search.v1.SearchIndexService/UpdateSearchIndex");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.catalog.v1.ProductService/CreateProduct"), "product", "components", "quantity"),
                ProbeSupport.value(sent1, "search_index", "components", "quantity"));
        var sent2 = channel.request("acme.lending.v1.RepaymentValidationService/ValidateRepayment");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.catalog.v1.ProductService/CreateProduct"), "product", "owner", "email"),
                ProbeSupport.value(sent2, "repayment", "participants", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "preferred_channel"),
                ProbeSupport.value(sent2, "details", "contact", "preferred_channel"));
        var sent3 = channel.request("acme.lending.v1.CollateralValidationService/NormalizeCollateral");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.catalog.v1.ProductService/CreateProduct"), "product", "audit", "etag"),
                ProbeSupport.value(sent3, "collateral", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.catalog.v1.ProductService/CreateProduct"), "product", "audit", "updated_by"),
                ProbeSupport.value(response, "collateral", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.search.v1.SearchIndexService/UpdateSearchIndex"), "search_index", "code"),
                ProbeSupport.value(response, "collateral", "code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentValidationService/ValidateRepayment"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentValidationService/ValidateRepayment"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/NormalizeCollateral"), "collateral", "capacity"),
                ProbeSupport.value(response, "collateral", "capacity"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/NormalizeCollateral"), "collateral", "owner", "display_name"),
                ProbeSupport.value(response, "collateral", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collateral_id"),
                ProbeSupport.value(response, "collateral", "collateral_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new CollateralLifecycleApplicationEvents(CustomerSegmentValidationServiceGrpc.newStub(channel), HouseholdCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<EstimateHouseholdResponse>();
        listener.on(new CollateralLifecycleApplication.ActivateCollateralStage0Requested(ProbeSupport.sample(EstimateHouseholdRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<ExplainCustomerSegmentDecisionResponse>();
        listener.on(new CollateralLifecycleApplication.GetCollateralLifecycleStage0Requested(ProbeSupport.sample(ExplainCustomerSegmentDecisionRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
