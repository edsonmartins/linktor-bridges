package dev.whatsbridge.wa

import android.content.Context
import android.util.Log
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import wacore.Wacore

/**
 * Thread-safe wrapper around the gomobile bindings (`wacore.Wacore`).
 * All work is dispatched to the IO dispatcher because Go/gojni calls are
 * blocking and CPU-heavy (session IO, pairing).
 */
object WaGateway {

    private const val TAG = "WaGateway"

    @Volatile
    private var initialized = false

    private val io = Dispatchers.IO

    suspend fun ensureInitialized(context: Context) = withContext(io) {
        if (initialized) return@withContext
        val dataDir = context.filesDir.resolve("whatsmeow").absolutePath
        Wacore.initialize(dataDir)
        initialized = true
    }

    suspend fun isPaired(): Boolean = withContext(io) {
        Wacore.isPaired()
    }

    suspend fun isConnected(): Boolean = withContext(io) {
        Wacore.isConnected()
    }

    suspend fun beginPairing(): String = withContext(io) {
        Wacore.beginPairing()
    }

    suspend fun beginCodePairing(phone: String): String = withContext(io) {
        Wacore.beginCodePairing(phone)
    }

    suspend fun cancelPairing() = withContext(io) {
        Wacore.cancelPairing()
    }

    suspend fun connect() = withContext(io) {
        Wacore.connect()
    }

    suspend fun disconnect() = withContext(io) {
        Wacore.disconnect()
    }

    suspend fun sendText(destination: String, text: String): String = withContext(io) {
        Wacore.sendText(destination, text)
    }

    suspend fun drainInbound(): List<Map<String, Any?>> = withContext(io) {
        val raw = Wacore.drainInbound()
        parseJsonArray(raw)
    }

    suspend fun status(): String = withContext(io) {
        Wacore.getStatus()
    }

    fun ping(): String = Wacore.ping()

    private fun parseJsonArray(raw: String): List<Map<String, Any?>> {
        if (raw.isBlank()) return emptyList()
        return try {
            val array = org.json.JSONArray(raw)
            val result = mutableListOf<Map<String, Any?>>()
            for (i in 0 until array.length()) {
                val obj = array.getJSONObject(i)
                val map = mutableMapOf<String, Any?>()
                for (key in obj.keys()) {
                    map[key] = if (obj.isNull(key)) null else obj.get(key)
                }
                result.add(map)
            }
            result
        } catch (e: Exception) {
            Log.w(TAG, "parse inbound failed", e)
            emptyList()
        }
    }
}