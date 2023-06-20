plugins {
    id("java")
    id("application")
    id("com.github.johnrengelman.shadow") version "7.1.2"
}

group = "org.eclipse.edc"
version = "0.1.1-SNAPSHOT"
var edc = "0.1.1-SNAPSHOT"
repositories {
    mavenCentral()
}

dependencies {
    implementation("org.eclipse.edc:iam-mock:${edc}")
    implementation("org.eclipse.edc:vault-hashicorp:${edc}")


    runtimeOnly("org.eclipse.edc:control-plane-core:${edc}")
    runtimeOnly("org.eclipse.edc:configuration-filesystem:${edc}")
    runtimeOnly("org.eclipse.edc:auth-tokenbased:${edc}")

    runtimeOnly("org.eclipse.edc:management-api:${edc}")
    runtimeOnly("org.eclipse.edc:management-api-configuration:${edc}")
    runtimeOnly("org.eclipse.edc:dsp:${edc}")
    runtimeOnly("org.eclipse.edc:jwt-spi:${edc}")

    runtimeOnly("org.eclipse.edc:data-plane-client:${edc}")
    runtimeOnly("org.eclipse.edc:data-plane-framework:${edc}")
    runtimeOnly("org.eclipse.edc:transfer-data-plane:${edc}")
    runtimeOnly("org.eclipse.edc:data-plane-selector-core:${edc}")
    runtimeOnly( "org.eclipse.edc:data-plane-selector-client:${edc}")

    runtimeOnly(project(":extensions:control-plane:provision:provision-gcs"))
    runtimeOnly(project(":extensions:data-plane:data-plane-google-storage"))
    runtimeOnly("org.eclipse.edc:data-plane-http:${edc}")


    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.8.1")
}

var createJar = tasks.withType<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar> {
    mergeServiceFiles()
    archiveFileName.set("app.jar")
}

application {
    mainClass.set("org.eclipse.edc.boot.system.runtime.BaseRuntime")
}
