package dev.zacp.privacyprobe

import android.annotation.SuppressLint
import android.bluetooth.BluetoothManager
import android.content.Context
import android.content.pm.PackageManager
import android.media.MediaDrm
import android.net.wifi.WifiManager
import android.os.Build
import android.provider.Settings
import android.telephony.SubscriptionManager
import android.telephony.TelephonyManager
import java.security.MessageDigest
import java.util.UUID

/** 单项探测的结论。 */
enum class Status(val icon: String, val label: String) {
    /** 拿到了真实、可用于追踪的值：存在隐私暴露面。 */
    EXPOSED("⚠️", "可读取"),
    /** 被系统权限/版本策略拦截，或只返回占位值：系统保护有效。 */
    PROTECTED("✅", "已被保护"),
    /** 设备不支持（无 SIM、无蓝牙等），无法判断。 */
    UNAVAILABLE("➖", "不可用"),
}

data class ProbeResult(
    val group: String,
    val name: String,
    val status: Status,
    val value: String,
    val note: String = "",
)

/**
 * 隐私探针：用普通第三方 App 的身份，逐项尝试读取设备/SIM 标识，并判定是否真的泄露。
 *
 * 关键不变量：每项探测都用 [probe] 包裹，SecurityException 视为"被保护"而不是崩溃——
 * 这正是本工具要观测的行为。结果只返回给调用方展示，不落盘、不联网。
 */
@SuppressLint("HardwareIds", "MissingPermission")
class PrivacyProbe(private val ctx: Context) {

    private val tm = ctx.getSystemService(Context.TELEPHONY_SERVICE) as TelephonyManager

    private fun granted(p: String) =
        ctx.checkSelfPermission(p) == PackageManager.PERMISSION_GRANTED

    /** 占位/空值一律视为未泄露（各厂商 ROM 会返回 "unknown"、全 0 MAC 等）。 */
    private fun isPlaceholder(v: String?): Boolean {
        if (v.isNullOrBlank()) return true
        val s = v.lowercase()
        return s == "unknown" || s == "02:00:00:00:00:00" || s.all { it == '0' || it == ':' }
    }

    private fun probe(
        group: String,
        name: String,
        note: String = "",
        block: () -> String?,
    ): ProbeResult = try {
        val v = block()
        if (isPlaceholder(v)) ProbeResult(group, name, Status.PROTECTED, v ?: "(空)", "返回空值/占位值" + suffix(note))
        else ProbeResult(group, name, Status.EXPOSED, v!!, note)
    } catch (e: SecurityException) {
        ProbeResult(group, name, Status.PROTECTED, "SecurityException", e.message?.lineSequence()?.first().orEmpty())
    } catch (e: Throwable) {
        ProbeResult(group, name, Status.UNAVAILABLE, e.javaClass.simpleName, e.message.orEmpty())
    }

    private fun suffix(note: String) = if (note.isEmpty()) "" else "；$note"

    /** 执行全部探测。调用线程无要求，但包含少量 Binder 调用，建议不要在高频路径使用。 */
    fun runAll(): List<ProbeResult> {
        val r = mutableListOf<ProbeResult>()
        val phoneState = granted(android.Manifest.permission.READ_PHONE_STATE)
        val phoneNumbers = granted(android.Manifest.permission.READ_PHONE_NUMBERS)

        // ---- 设备标识 ----
        r += probe("设备标识", "ANDROID_ID", "无需权限；8.0+ 按 App 签名隔离，同签名 App 间可关联") {
            Settings.Secure.getString(ctx.contentResolver, Settings.Secure.ANDROID_ID)
        }
        r += probe("设备标识", "Build.SERIAL / getSerial()", "10+ 需 READ_PRIVILEGED_PHONE_STATE") {
            if (Build.VERSION.SDK_INT >= 26) Build.getSerial() else @Suppress("DEPRECATION") Build.SERIAL
        }
        r += probe("设备标识", "IMEI", "10+ 仅系统/特权应用可读") {
            if (Build.VERSION.SDK_INT >= 26) tm.imei else @Suppress("DEPRECATION") tm.deviceId
        }
        r += probe("设备标识", "MEID", "同 IMEI") {
            if (Build.VERSION.SDK_INT >= 26) tm.meid else null
        }
        r += probe("设备标识", "DRM 设备 ID (Widevine)", "无需权限，重置系统前基本不变，常被用作指纹") {
            val drm = MediaDrm(UUID(-0x121074568629b532L, -0x5c37d8232ae2de13L))
            try {
                val id = drm.getPropertyByteArray(MediaDrm.PROPERTY_DEVICE_UNIQUE_ID)
                MessageDigest.getInstance("SHA-256").digest(id).joinToString("") { "%02x".format(it) }.take(32)
            } finally {
                if (Build.VERSION.SDK_INT >= 28) drm.close() else @Suppress("DEPRECATION") drm.release()
            }
        }
        r += probe("设备标识", "Wi-Fi MAC", "6.0+ 返回固定占位值，11+ 还会随机化") {
            @Suppress("DEPRECATION")
            (ctx.applicationContext.getSystemService(Context.WIFI_SERVICE) as WifiManager).connectionInfo?.macAddress
        }
        r += probe("设备标识", "蓝牙 MAC", "6.0+ 返回占位值或抛异常") {
            (ctx.getSystemService(Context.BLUETOOTH_SERVICE) as BluetoothManager).adapter?.address
        }

        // ---- SIM / 运营商 ----
        r += probe("SIM 与运营商", "SIM 状态", "无需权限") { simStateName(tm.simState) }
        r += probe("SIM 与运营商", "SIM 运营商 MCC+MNC", "无需权限；可定位运营商/国家") { tm.simOperator }
        r += probe("SIM 与运营商", "SIM 运营商名称", "无需权限") { tm.simOperatorName }
        r += probe("SIM 与运营商", "SIM 国家码", "无需权限") { tm.simCountryIso }
        r += probe("SIM 与运营商", "当前网络运营商", "无需权限") { tm.networkOperatorName }
        r += probe("SIM 与运营商", "ICCID (SIM 序列号)", "10+ 需特权/运营商权限") { tm.simSerialNumber }
        r += probe("SIM 与运营商", "IMSI (订阅者 ID)", "10+ 需特权/运营商权限") { tm.subscriberId }
        r += probe("SIM 与运营商", "本机号码 (Line1Number)", "需 READ_PHONE_NUMBERS，当前${if (phoneNumbers) "已授权" else "未授权"}") {
            tm.line1Number
        }

        // ---- 订阅信息（双卡）----
        r += probe("订阅信息", "活跃 SIM 订阅列表", "需 READ_PHONE_STATE，当前${if (phoneState) "已授权" else "未授权"}") {
            val sm = ctx.getSystemService(Context.TELEPHONY_SUBSCRIPTION_SERVICE) as SubscriptionManager
            val list = sm.activeSubscriptionInfoList.orEmpty()
            if (list.isEmpty()) null
            else list.joinToString("\n") {
                "卡${it.simSlotIndex + 1}: ${it.carrierName} ICCID=${it.iccId.orEmpty().ifEmpty { "(空)" }} " +
                    "号码=${it.number.orEmpty().ifEmpty { "(空)" }}"
            }
        }

        // ---- 设备指纹面（天然公开，组合起来仍可识别）----
        r += probe("指纹面", "型号 / 厂商", "无需权限") { "${Build.MANUFACTURER} ${Build.MODEL}" }
        r += probe("指纹面", "系统版本 / 补丁", "无需权限") { "Android ${Build.VERSION.RELEASE} (SDK ${Build.VERSION.SDK_INT}) ${Build.VERSION.SECURITY_PATCH}" }
        r += probe("指纹面", "Build.FINGERPRINT", "无需权限") { Build.FINGERPRINT }
        return r
    }

    private fun simStateName(s: Int) = when (s) {
        TelephonyManager.SIM_STATE_ABSENT -> "无卡"
        TelephonyManager.SIM_STATE_READY -> "就绪"
        TelephonyManager.SIM_STATE_PIN_REQUIRED -> "需 PIN"
        TelephonyManager.SIM_STATE_PUK_REQUIRED -> "需 PUK"
        TelephonyManager.SIM_STATE_NETWORK_LOCKED -> "网络锁定"
        else -> "未知($s)"
    }

    companion object {
        /** 生成可复制的纯文本报告，含系统版本，便于对比不同机型/系统的差异。 */
        fun report(results: List<ProbeResult>): String = buildString {
            appendLine("Privacy Probe 报告  Android ${Build.VERSION.RELEASE} / SDK ${Build.VERSION.SDK_INT}")
            var group = ""
            for (x in results) {
                if (x.group != group) { group = x.group; appendLine("\n[$group]") }
                appendLine("${x.status.icon} ${x.name}: ${x.value}")
            }
        }
    }
}
