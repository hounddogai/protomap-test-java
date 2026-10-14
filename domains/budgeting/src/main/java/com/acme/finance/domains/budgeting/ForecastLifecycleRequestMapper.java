package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.ActivateForecastRequest;
import com.acme.budgeting.v1.ActivateForecastResponse;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.ExpireForecastRequest;
import com.acme.budgeting.v1.ExpireForecastResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.GetForecastLifecycleRequest;
import com.acme.budgeting.v1.GetForecastLifecycleResponse;
import com.acme.budgeting.v1.ReconcileForecastsRequest;
import com.acme.budgeting.v1.ReconcileForecastsResponse;
import com.acme.budgeting.v1.ReinstateForecastRequest;
import com.acme.budgeting.v1.ReinstateForecastResponse;
import com.acme.budgeting.v1.RenewForecastRequest;
import com.acme.budgeting.v1.RenewForecastResponse;
import com.acme.budgeting.v1.ScheduleForecastExpiryRequest;
import com.acme.budgeting.v1.ScheduleForecastExpiryResponse;
import com.acme.budgeting.v1.ShareVarianceReportRequest;
import com.acme.budgeting.v1.ShareVarianceReportResponse;
import com.acme.budgeting.v1.SuspendForecastRequest;
import com.acme.budgeting.v1.SuspendForecastResponse;
import com.acme.budgeting.v1.TerminateForecastRequest;
import com.acme.budgeting.v1.TerminateForecastResponse;
import com.acme.budgeting.v1.VarianceReportingServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.SanctionReportingServiceGrpc;
import com.acme.compliance.v1.StreamSanctionMetricsRequest;
import com.acme.compliance.v1.StreamSanctionMetricsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.fulfillment.v1.BrowseFulfillmentsRequest;
import com.acme.fulfillment.v1.BrowseFulfillmentsResponse;
import com.acme.fulfillment.v1.FulfillmentQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.risk.v1.DelegateStressTestApprovalRequest;
import com.acme.risk.v1.DelegateStressTestApprovalResponse;
import com.acme.risk.v1.StressTestApprovalServiceGrpc;
import com.acme.suppliers.v1.BatchGetSuppliersRequest;
import com.acme.suppliers.v1.BatchGetSuppliersResponse;
import com.acme.suppliers.v1.BrowseSupplierContractsRequest;
import com.acme.suppliers.v1.BrowseSupplierContractsResponse;
import com.acme.suppliers.v1.CountSuppliersRequest;
import com.acme.suppliers.v1.CountSuppliersResponse;
import com.acme.suppliers.v1.ExplainSupplierRatingDecisionRequest;
import com.acme.suppliers.v1.ExplainSupplierRatingDecisionResponse;
import com.acme.suppliers.v1.ScoreSupplierRatingRequest;
import com.acme.suppliers.v1.ScoreSupplierRatingResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.UpdateSupplierRequest;
import com.acme.suppliers.v1.UpdateSupplierResponse;
import com.acme.suppliers.v1.ValidateSupplierRatingRequest;
import com.acme.suppliers.v1.ValidateSupplierRatingResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Maps workflow inputs to downstream contracts. Implementations keep wire construction explicit.
 */
public interface ForecastLifecycleRequestMapper {
    DelegateStressTestApprovalRequest terminateForecastStage0(TerminateForecastRequest request);

    BatchGetSuppliersRequest renewForecastStage1(RenewForecastRequest request);
}
