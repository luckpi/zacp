package dev.zacp.privacyprobe

import android.Manifest
import android.app.Activity
import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.os.Bundle
import android.view.ViewGroup
import android.widget.Button
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import android.widget.Toast

/** 单页界面：权限申请 + 重新检测 + 复制报告 + 结果列表。用代码构建视图，省去 AndroidX 依赖。 */
class MainActivity : Activity() {

    private lateinit var list: LinearLayout
    private var results: List<ProbeResult> = emptyList()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val dp = resources.displayMetrics.density
        val pad = (16 * dp).toInt()

        val root = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL; setPadding(pad, pad, pad, pad) }
        root.addView(TextView(this).apply {
            text = "以普通第三方 App 身份，试读设备/SIM 标识。⚠️=可读取（有暴露面）  ✅=被系统保护  ➖=设备不支持"
        })
        root.addView(button("申请电话权限（READ_PHONE_STATE / NUMBERS）") {
            requestPermissions(arrayOf(Manifest.permission.READ_PHONE_STATE, Manifest.permission.READ_PHONE_NUMBERS), 1)
        })
        root.addView(button("重新检测") { refresh() })
        root.addView(button("复制报告") {
            (getSystemService(Context.CLIPBOARD_SERVICE) as ClipboardManager)
                .setPrimaryClip(ClipData.newPlainText("privacy-probe", PrivacyProbe.report(results)))
            Toast.makeText(this, "已复制", Toast.LENGTH_SHORT).show()
        })
        list = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL }
        root.addView(list)
        setContentView(ScrollView(this).apply { addView(root) })
        refresh()
    }

    // 授权结果回来后自动重测，这样能直接对比授权前后的差异
    override fun onRequestPermissionsResult(code: Int, perms: Array<out String>, res: IntArray) = refresh()

    private fun refresh() {
        results = PrivacyProbe(this).runAll()
        list.removeAllViews()
        var group = ""
        for (x in results) {
            if (x.group != group) {
                group = x.group
                list.addView(TextView(this).apply { text = "\n【$group】"; textSize = 16f })
            }
            list.addView(TextView(this).apply {
                text = "${x.status.icon} ${x.name}\n    ${x.value}" + if (x.note.isNotEmpty()) "\n    · ${x.note}" else ""
                setTextIsSelectable(true)
                setPadding(0, 8, 0, 8)
            })
        }
    }

    private fun button(label: String, onClick: () -> Unit) = Button(this).apply {
        text = label
        layoutParams = LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT)
        setOnClickListener { onClick() }
    }
}
