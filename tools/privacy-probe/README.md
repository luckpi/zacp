# Privacy Probe

自检用 Android 小工具：以**普通第三方 App** 身份尝试读取设备 ID / SIM 信息，判断各项是否真的会泄露。
只在屏幕展示结果，**未声明 INTERNET 权限**，无法外传。

## 检测项
- 设备标识：ANDROID_ID、序列号、IMEI/MEID、Widevine DRM ID、Wi-Fi/蓝牙 MAC
- SIM：状态、MCC/MNC、运营商、国家码、ICCID、IMSI、本机号码、双卡订阅列表
- 指纹面：型号、系统版本、Build.FINGERPRINT

结论图例：⚠️ 可读取（有暴露面）　✅ 被系统保护　➖ 设备不支持

## 构建
用 Android Studio 打开本目录即可同步并运行；或命令行（需 Android SDK，`local.properties` 写 `sdk.dir`）：

```bash
gradle :app:assembleDebug   # 产物 app/build/outputs/apk/debug/app-debug.apk
adb install -r app/build/outputs/apk/debug/app-debug.apk
```

## 预期（Android 10+ 的典型结果）
IMEI/序列号/ICCID/IMSI 抛 `SecurityException`；MAC 为占位值；ANDROID_ID、DRM ID、运营商 MCC/MNC、型号等仍可读。
点"申请电话权限"后会自动重测，可对比授权前后差异。Android 9 及以下的真机可看到更多项变为 ⚠️。
