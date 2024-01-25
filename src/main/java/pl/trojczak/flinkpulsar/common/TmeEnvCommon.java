package pl.trojczak.flinkpulsar.common;

public class TmeEnvCommon {

    protected static final String PULSAR_BROKER_URL = "pulsar+ssl://dev-05cb4436-c5c4-4010-848b-74e7a6bc2c73.aws-euw1-snci-duck-prod-snc.aws.snio.cloud:6651";
    protected static final String PULSAR_SERVICE_URL = "https://dev-05cb4436-c5c4-4010-848b-74e7a6bc2c73.aws-euw1-snci-duck-prod-snc.aws.snio.cloud:443";
    protected static final String AUTHENTICATION_OAUTH2 = "org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2";

    public static String prepareAuthenticationData() {
        return "{\"issuerUrl\":\"https://auth.streamnative.cloud/\",\"audience\":\"urn:sn:pulsar:tme:hosted-dev\",\"privateKey\":\"/home/rtk/programming/tme/kube/docker_flink17_tme_dev/keys/flinkjobprivatekeysecret\"}";
    }
}
