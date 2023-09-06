plugins {
    id("java")
    id("application")
    id("com.github.johnrengelman.shadow") version "7.1.2"
}

group = "org.eclipse.edc"
version = "0.2.2-SNAPSHOT"

val edcVersion = "0.2.2-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.eclipse.edc:iam-mock:${edcVersion}")
    implementation("org.eclipse.edc:vault-hashicorp:${edcVersion}")


    runtimeOnly("org.eclipse.edc:control-plane-core:${edcVersion}")
    runtimeOnly("org.eclipse.edc:configuration-filesystem:${edcVersion}")
    runtimeOnly("org.eclipse.edc:auth-tokenbased:${edcVersion}")

    runtimeOnly("org.eclipse.edc:management-api:${edcVersion}")
    runtimeOnly("org.eclipse.edc:management-api-configuration:${edcVersion}")
    runtimeOnly("org.eclipse.edc:dsp:${edcVersion}")
    runtimeOnly("org.eclipse.edc:jwt-spi:${edcVersion}")

    runtimeOnly("org.eclipse.edc:data-plane-client:${edcVersion}")
    runtimeOnly("org.eclipse.edc:data-plane-framework:${edcVersion}")
    runtimeOnly("org.eclipse.edc:transfer-data-plane:${edcVersion}")
    runtimeOnly("org.eclipse.edc:data-plane-selector-core:${edcVersion}")
    runtimeOnly( "org.eclipse.edc:data-plane-selector-client:${edcVersion}")

    runtimeOnly(project(":extensions:control-plane:provision:provision-gcs"))
    runtimeOnly(project(":extensions:data-plane:data-plane-google-storage"))
    runtimeOnly("org.eclipse.edc:data-plane-http:${edcVersion}")


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
