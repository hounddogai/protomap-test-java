package com.acme.finance.jobs.supplierreview;

import com.acme.common.v1.Actor;
import com.acme.common.v1.RequestContext;
import com.acme.finance.platform.CallIdentity;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.server.FinanceApplication;
import com.acme.procurement.v1.GetGoodsReceiptApprovalStatusRequest;
import com.acme.procurement.v1.GoodsReceiptApprovalServiceGrpc;
import com.acme.procurement.v1.ListPendingGoodsReceiptsRequest;
import io.grpc.Context;
import java.util.concurrent.TimeUnit;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Runs a bounded reconciliation pass. Scheduling and tenant selection belong to the job runner.
 */
@Configuration(proxyBeanMethods = false)
@Profile("job-supplierreview")
public class SupplierReview {
    public static void main(String[] args) {
        try (var application = new SpringApplicationBuilder(FinanceApplication.class).profiles("job-supplierreview")
                .properties("grpc.server.port=-1").run(args)) {
            application.getBean(ChannelRegistry.class);
        }
    }

    @Bean
    ApplicationRunner runSupplierReview(ChannelRegistry channels, org.springframework.core.env.Environment environment) {
        return arguments -> {
            String tenant = environment.getRequiredProperty("acme.job.tenant");
            var context = RequestContext.newBuilder().setTenantId(tenant)
                    .setActor(Actor.newBuilder().setServiceAccount(true)).build();
            // The channels' forwarding interceptor gives the job's calls its own credential and tenant.
            Context job = Context.current().withValue(CallIdentity.TENANT, tenant).withValue(CallIdentity.AUTHORIZATION,
                    "Bearer " + environment.getRequiredProperty("acme.job.token"));
            Context previous = job.attach();
            try {
                var source = GoodsReceiptApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.GoodsReceiptApprovalService"));
                var target = GoodsReceiptApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.procurement.v1.GoodsReceiptApprovalService"));
                var page = source.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingGoodsReceipts(
                        ListPendingGoodsReceiptsRequest.newBuilder().setContext(context).build());
                for (var record : page.getGoodsReceiptsList()) {
                    var request = GetGoodsReceiptApprovalStatusRequest.newBuilder()
        .setContext(context)
        .setGoodsReceiptId(record.getGoodsReceiptId())
        .build();
                    target.withDeadlineAfter(8, TimeUnit.SECONDS).getGoodsReceiptApprovalStatus(request);
                }
            } finally {
                job.detach(previous);
            }
        };
    }
}
