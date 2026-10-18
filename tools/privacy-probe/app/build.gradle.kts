plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

android {
    namespace = "dev.zacp.privacyprobe"
    compileSdk = 34

    defaultConfig {
        applicationId = "dev.zacp.privacyprobe"
        // 26 起才有 TelephonyManager.getImei 等 API；更低版本无测试意义
        minSdk = 26
        targetSdk = 34
        versionCode = 1
        versionName = "0.1.0"
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlinOptions { jvmTarget = "17" }
}
