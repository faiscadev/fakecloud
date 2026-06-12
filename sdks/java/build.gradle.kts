import com.vanniktech.maven.publish.JavaLibrary
import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.SonatypeHost

plugins {
    `java-library`
    id("com.vanniktech.maven.publish") version "0.30.0"
}

group = "dev.fakecloud"
version = "0.19.0"

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

dependencies {
    api("com.fasterxml.jackson.core:jackson-databind:2.17.2")
    api("com.fasterxml.jackson.core:jackson-annotations:2.17.2")

    testImplementation(platform("org.junit:junit-bom:5.10.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    val awsSdk = "2.27.21"
    testImplementation(platform("software.amazon.awssdk:bom:$awsSdk"))
    testImplementation("software.amazon.awssdk:sqs")
    testImplementation("software.amazon.awssdk:sns")
    testImplementation("software.amazon.awssdk:sesv2")
    testImplementation("software.amazon.awssdk:s3")
    testImplementation("software.amazon.awssdk:dynamodb")
    testImplementation("software.amazon.awssdk:cognitoidentityprovider")
    testImplementation("software.amazon.awssdk:eventbridge")
    testImplementation("software.amazon.awssdk:ec2")
    testImplementation("software.amazon.awssdk:rds")
    testImplementation("software.amazon.awssdk:elasticache")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = false
    }
}

tasks.javadoc {
    (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:none", "-quiet")
}

mavenPublishing {
    configure(JavaLibrary(javadocJar = JavadocJar.Javadoc(), sourcesJar = true))

    publishToMavenCentral(SonatypeHost.CENTRAL_PORTAL, automaticRelease = true)
    signAllPublications()

    coordinates(group.toString(), "fakecloud", version.toString())

    pom {
        name.set("fakecloud")
        description.set(
            "Java client SDK for fakecloud — a local AWS cloud emulator. Wraps the "
                + "/_fakecloud/* introspection and simulation endpoints for JUnit, Spring Boot, "
                + "Micronaut, and Quarkus tests."
        )
        url.set("https://github.com/faiscadev/fakecloud")
        inceptionYear.set("2026")

        licenses {
            license {
                name.set("GNU Affero General Public License v3.0")
                url.set("https://www.gnu.org/licenses/agpl-3.0.txt")
                distribution.set("repo")
            }
        }

        developers {
            developer {
                id.set("faiscadev")
                name.set("Faisca Dev")
                url.set("https://github.com/faiscadev")
            }
        }

        scm {
            url.set("https://github.com/faiscadev/fakecloud")
            connection.set("scm:git:git://github.com/faiscadev/fakecloud.git")
            developerConnection.set("scm:git:ssh://git@github.com/faiscadev/fakecloud.git")
        }

        issueManagement {
            system.set("GitHub")
            url.set("https://github.com/faiscadev/fakecloud/issues")
        }
    }
}
