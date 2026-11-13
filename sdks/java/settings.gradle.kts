pluginManagement {
    repositories {
        // Plugin dependencies (retrofit, moshi, okhttp, ...) resolve from Maven
        // Central first; the plugin portal serves what only it has, such as
        // plugin marker artifacts. The v0.52.0 Maven Central publish failed
        // when the portal served a dependency's POM but not its jar, and
        // Gradle does not retry an artifact in another repository once one
        // has served its metadata.
        mavenCentral()
        gradlePluginPortal()
    }
}

rootProject.name = "fakecloud"
