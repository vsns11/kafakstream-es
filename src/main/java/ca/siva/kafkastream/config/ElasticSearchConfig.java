package ca.siva.kafkastream.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.ssl.SSLContexts;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchConfiguration;

import javax.net.ssl.SSLContext;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

@Configuration
@Slf4j
public class ElasticSearchConfig extends ElasticsearchConfiguration {
    @Override
    public ClientConfiguration clientConfiguration() {
        SSLContext sslContext = null;
        try {
            sslContext = SSLContexts.custom()
                    .loadTrustMaterial(null, (x509Certificates, s) -> true)
                    .build();
        } catch (NoSuchAlgorithmException e) {
            log.error("NoSuchAlgorithmException occurred:{}", e.toString(), e);
        } catch (KeyManagementException e) {
            log.error("KeyManagementException occurred:{}", e.toString(), e);
        } catch (KeyStoreException e) {
            log.error("KeyStoreException occurred:{}", e.toString(), e);
        }
        return ClientConfiguration.builder()
                .connectedTo("localhost:9200")
                .usingSsl(sslContext)
                .withBasicAuth("elastic", "GNBil+AFXd77UCvb5mVl")
                .build();
    }
}
