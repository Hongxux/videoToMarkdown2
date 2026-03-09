plugins {
    alias(libs.plugins.android.application)
    alias(libs.plugins.kotlin.android)
    alias(libs.plugins.kotlin.compose)
    id("org.jetbrains.kotlin.kapt")
}

val mobileApiBaseUrl = sequenceOf(
    (findProperty("mobileApiBaseUrl") as String?)?.trim(),
    System.getenv("MOBILE_APP_API_BASE_URL")?.trim(),
    System.getenv("MOBILE_API_BASE_URL")?.trim()
).mapNotNull { candidate ->
    candidate?.takeIf { it.isNotBlank() }?.trimEnd('/')?.let { trimmed ->
        if (trimmed.endsWith("/api/mobile")) {
            trimmed
        } else {
            "$trimmed/api/mobile"
        }
    }
}.firstOrNull() ?: "https://frp-box.com:41570/api/mobile"
val mobileApiBaseUrlEscaped = mobileApiBaseUrl
    .replace("\\", "\\\\")
    .replace("\"", "\\\"")
val mobileAutoUpdateEnabled = (findProperty("mobileAutoUpdateEnabled") as String?)
    ?.trim()
    ?.let { it.equals("true", ignoreCase = true) }
    ?: true
val mobileAppUpdateChunkSizeMb = (findProperty("mobileAppUpdateChunkSizeMb") as String?)
    ?.trim()
    ?.toIntOrNull()
    ?.coerceAtLeast(1)
    ?: 2
val mobileAppUpdateMaxParallelChunks = (findProperty("mobileAppUpdateMaxParallelChunks") as String?)
    ?.trim()
    ?.toIntOrNull()
    ?.coerceIn(1, 8)
    ?: 4
val mobileAppUpdateMinChunkedDownloadMb = (findProperty("mobileAppUpdateMinChunkedDownloadMb") as String?)
    ?.trim()
    ?.toIntOrNull()
    ?.coerceAtLeast(2)
    ?: 4

val androidReleaseKeystorePath = sequenceOf(
    (findProperty("androidReleaseKeystorePath") as String?)?.trim(),
    System.getenv("ANDROID_RELEASE_KEYSTORE_PATH")?.trim()
).firstOrNull { it != null && it.isNotBlank() }
val androidReleaseKeystorePassword = sequenceOf(
    (findProperty("androidReleaseKeystorePassword") as String?)?.trim(),
    System.getenv("ANDROID_RELEASE_KEYSTORE_PASSWORD")?.trim()
).firstOrNull { it != null && it.isNotBlank() }
val androidReleaseKeyAlias = sequenceOf(
    (findProperty("androidReleaseKeyAlias") as String?)?.trim(),
    System.getenv("ANDROID_RELEASE_KEY_ALIAS")?.trim()
).firstOrNull { it != null && it.isNotBlank() }
val androidReleaseKeyPassword = sequenceOf(
    (findProperty("androidReleaseKeyPassword") as String?)?.trim(),
    System.getenv("ANDROID_RELEASE_KEY_PASSWORD")?.trim()
).firstOrNull { it != null && it.isNotBlank() }
val hasReleaseSigning = !androidReleaseKeystorePath.isNullOrBlank() &&
    !androidReleaseKeystorePassword.isNullOrBlank() &&
    !androidReleaseKeyAlias.isNullOrBlank() &&
    !androidReleaseKeyPassword.isNullOrBlank()

android {
    namespace = "com.hongxu.videoToMarkdownTest2"
    compileSdk = 35

    signingConfigs {
        if (hasReleaseSigning) {
            create("release") {
                storeFile = file(androidReleaseKeystorePath!!)
                storePassword = androidReleaseKeystorePassword
                keyAlias = androidReleaseKeyAlias
                keyPassword = androidReleaseKeyPassword
            }
        }
    }

    defaultConfig {
        applicationId = "com.hongxu.videoToMarkdownTest2"
        minSdk = 24
        targetSdk = 35
        versionCode = 8
        versionName = "1.0.8"

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
        buildConfigField("String", "MOBILE_API_BASE_URL", "\"$mobileApiBaseUrlEscaped\"")
        buildConfigField("boolean", "MOBILE_AUTO_UPDATE_ENABLED", mobileAutoUpdateEnabled.toString())
        buildConfigField(
            "long",
            "MOBILE_APP_UPDATE_CHUNK_SIZE_BYTES",
            "${mobileAppUpdateChunkSizeMb.toLong() * 1024L * 1024L}L"
        )
        buildConfigField(
            "int",
            "MOBILE_APP_UPDATE_MAX_PARALLEL_CHUNKS",
            mobileAppUpdateMaxParallelChunks.toString()
        )
        buildConfigField(
            "long",
            "MOBILE_APP_UPDATE_MIN_CHUNKED_DOWNLOAD_BYTES",
            "${mobileAppUpdateMinChunkedDownloadMb.toLong() * 1024L * 1024L}L"
        )
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            if (hasReleaseSigning) {
                signingConfig = signingConfigs.getByName("release")
            }
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }
    kotlinOptions {
        jvmTarget = "11"
    }
    buildFeatures {
        compose = true
        buildConfig = true
    }
    lint {
        checkReleaseBuilds = false
        abortOnError = false
    }
}

configurations.configureEach {
    exclude(group = "org.jetbrains", module = "annotations-java5")
}

dependencies {
    implementation(libs.androidx.core.ktx)
    implementation(libs.androidx.lifecycle.runtime.ktx)
    implementation(libs.androidx.lifecycle.runtime.compose)
    implementation(libs.androidx.lifecycle.process)
    implementation(libs.androidx.lifecycle.viewmodel.ktx)
    implementation(libs.androidx.lifecycle.viewmodel.compose)
    implementation(libs.androidx.activity.compose)
    implementation(libs.androidx.work.runtime.ktx)
    implementation(libs.kotlinx.coroutines.android)
    implementation(libs.retrofit.core)
    implementation(libs.retrofit.converter.gson)
    implementation(libs.okhttp.core)
    implementation(libs.okhttp.logging)
    implementation(libs.androidx.room.runtime)
    implementation(libs.androidx.room.ktx)
    kapt(libs.androidx.room.compiler)
    implementation(libs.markwon.core)
    implementation(libs.markwon.html)
    implementation(libs.markwon.linkify)
    implementation(libs.markwon.ext.strikethrough)
    implementation(libs.markwon.ext.tables)
    implementation(libs.markwon.ext.latex)
    implementation(libs.markwon.inline.parser)
    implementation(libs.markwon.image.coil)
    implementation(libs.markwon.syntax.highlight)
    implementation(libs.coil)
    implementation(libs.coil.compose)
    implementation(libs.prism4j)
    kapt(libs.prism4j.bundler)
    implementation(platform(libs.androidx.compose.bom))
    implementation(libs.androidx.compose.ui)
    implementation(libs.androidx.compose.ui.graphics)
    implementation(libs.androidx.compose.ui.tooling.preview)
    implementation(libs.androidx.compose.material3)
    testImplementation(libs.junit)
    androidTestImplementation(libs.androidx.junit)
    androidTestImplementation(libs.androidx.espresso.core)
    androidTestImplementation(platform(libs.androidx.compose.bom))
    androidTestImplementation(libs.androidx.compose.ui.test.junit4)
    debugImplementation(libs.androidx.compose.ui.tooling)
    debugImplementation(libs.androidx.compose.ui.test.manifest)
}

