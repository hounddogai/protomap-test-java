package com.acme.finance.platform.clients;

import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the documents contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class DocumentsClients {
    @Bean
    public DocumentQueryServiceGrpc.DocumentQueryServiceStub documentsDocumentQueryServiceAsync(ChannelRegistry channels) {
        return DocumentQueryServiceGrpc.newStub(channels.lookup("acme.documents.v1.DocumentQueryService"));
    }

    @Bean
    public DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking(ChannelRegistry channels) {
        return DocumentQueryServiceGrpc.newBlockingStub(channels.lookup("acme.documents.v1.DocumentQueryService"));
    }

    @Bean
    public DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking(ChannelRegistry channels) {
        return DocumentServiceGrpc.newBlockingStub(channels.lookup("acme.documents.v1.DocumentService"));
    }

    @Bean
    public DocumentServiceGrpc.DocumentServiceFutureStub documentsDocumentServiceFuture(ChannelRegistry channels) {
        return DocumentServiceGrpc.newFutureStub(channels.lookup("acme.documents.v1.DocumentService"));
    }

    @Bean
    public DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking(ChannelRegistry channels) {
        return DocumentValidationServiceGrpc.newBlockingStub(channels.lookup("acme.documents.v1.DocumentValidationService"));
    }

    @Bean
    public DocumentValidationServiceGrpc.DocumentValidationServiceFutureStub documentsDocumentValidationServiceFuture(ChannelRegistry channels) {
        return DocumentValidationServiceGrpc.newFutureStub(channels.lookup("acme.documents.v1.DocumentValidationService"));
    }
}
