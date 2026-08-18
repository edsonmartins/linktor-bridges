package dev.whatsbridge

import android.graphics.Bitmap
import android.graphics.Color
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.foundation.Image
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.asImageBitmap
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.unit.dp
import com.google.zxing.BarcodeFormat
import com.google.zxing.EncodeHintType
import com.google.zxing.MultiFormatWriter
import dev.whatsbridge.wa.WaGateway
import kotlinx.coroutines.launch
import org.json.JSONObject

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContent {
            MaterialTheme {
                BridgeScreen()
            }
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun BridgeScreen() {
    val context = LocalContext.current
    val scope = rememberCoroutineScope()

    var ping by remember { mutableStateOf("") }
    var paired by remember { mutableStateOf(false) }
    var connected by remember { mutableStateOf(false) }
    var number by remember { mutableStateOf("") }
    var qrText by remember { mutableStateOf("") }
    var message by remember { mutableStateOf("") }

    fun gateway(block: suspend () -> Unit) {
        scope.launch {
            try {
                WaGateway.ensureInitialized(context)
                block()
            } catch (e: Exception) {
                message = "Erro: ${e.message}"
            }
        }
    }

    fun refresh() = gateway {
        ping = WaGateway.ping()
        paired = WaGateway.isPaired()
        connected = WaGateway.isConnected()
        val st = JSONObject(WaGateway.status())
        number = st.optString("number", "")
    }

    LaunchedEffect(Unit) {
        refresh()
    }

    fun qrBitmap(payload: String): Bitmap? = try {
        val hints = mapOf(
            EncodeHintType.MARGIN to 1,
            EncodeHintType.CHARACTER_SET to "UTF-8",
        )
        val matrix = MultiFormatWriter().encode(payload, BarcodeFormat.QR_CODE, 512, 512, hints)
        val bmp = Bitmap.createBitmap(512, 512, Bitmap.Config.RGB_565)
        for (x in 0 until 512) for (y in 0 until 512) {
            bmp.setPixel(x, y, if (matrix[x, y]) Color.BLACK else Color.WHITE)
        }
        bmp
    } catch (e: Exception) {
        null
    }

    Scaffold(
        topBar = { TopAppBar(title = { Text("Linktor Bridge") }) }
    ) { padding ->
        Column(
            modifier = Modifier
                .padding(padding)
                .fillMaxSize()
                .verticalScroll(rememberScrollState())
                .padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(16.dp),
        ) {
            StatusCard(ping, paired, connected, number, onRefresh = ::refresh)

            if (!paired) {
                PairingCard(
                    qrText = qrText,
                    qrBitmap = qrText.takeIf { it.isNotEmpty() }?.let { qrBitmap(it) },
                    onGenerateQr = { gateway { qrText = WaGateway.beginPairing() } },
                    onCancel = { gateway { WaGateway.cancelPairing(); qrText = "" } },
                )
                CodeCard(
                    onPair = { phone -> gateway {
                        val code = WaGateway.beginCodePairing(phone)
                        message = "No WhatsApp do celular principal, digite: $code"
                    } },
                )
            }

            SendCard(
                enabled = paired && connected,
                onSend = { to, text -> gateway {
                    val id = WaGateway.sendText(to, text)
                    message = "Enviada. id=$id"
                    refresh()
                } },
            )

            InboundCard(
                onDrain = { callback -> gateway {
                    val events = WaGateway.drainInbound()
                    callback(events)
                } },
            )

            if (message.isNotEmpty()) {
                Text(message, style = MaterialTheme.typography.bodySmall)
            }
        }
    }
}

@Composable
fun StatusCard(ping: String, paired: Boolean, connected: Boolean, number: String, onRefresh: () -> Unit) {
    Card {
        Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(4.dp)) {
            Text("Status da Conexão", style = MaterialTheme.typography.titleMedium)
            Text("Bridge: ${if (ping.isEmpty()) "verificando…" else ping}", style = MaterialTheme.typography.bodySmall)
            Text(
                "Estado: ${if (connected) "Conectado" else "Desconectado"} · ${if (paired) "pareado" else "não pareado"}",
                style = MaterialTheme.typography.bodyMedium,
            )
            if (number.isNotEmpty()) {
                Text("Número: $number", style = MaterialTheme.typography.bodySmall)
            }
            if (!paired) {
                Text(
                    "Conecte sua conta WhatsApp para receber mensagens.",
                    style = MaterialTheme.typography.bodySmall,
                )
            }
            Button(onClick = onRefresh, modifier = Modifier.padding(top = 8.dp)) {
                Text("Atualizar")
            }
        }
    }
}

@Composable
fun PairingCard(qrText: String, qrBitmap: Bitmap?, onGenerateQr: () -> Unit, onCancel: () -> Unit) {
    Card {
        Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(12.dp)) {
            Text("Conectar sua conta WhatsApp", style = MaterialTheme.typography.titleMedium)
            if (qrBitmap != null) {
                Image(
                    bitmap = qrBitmap.asImageBitmap(),
                    contentDescription = "QR code",
                    modifier = Modifier
                        .size(240.dp)
                        .align(Alignment.CenterHorizontally)
                        .background(Color.White),
                )
                Text(
                    "Abra WhatsApp > Dispositivos conectados > Conectar dispositivo e escaneie.",
                    style = MaterialTheme.typography.bodySmall,
                )
                Text(
                    "QR expira em 3 min. Toque em Atualizar se o QR vencer.",
                    style = MaterialTheme.typography.bodySmall,
                )
            }
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                Button(onClick = onGenerateQr) { Text("Conectar com QR Code") }
                if (qrText.isNotEmpty()) {
                    OutlinedButton(onClick = onCancel) { Text("Cancelar pareamento") }
                }
            }
        }
    }
}

@Composable
fun CodeCard(onPair: (String) -> Unit) {
    var phone by remember { mutableStateOf("") }
    Card {
        Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(12.dp)) {
            Text("Ou use o número de telefone", style = MaterialTheme.typography.titleMedium)
            OutlinedTextField(
                value = phone,
                onValueChange = { phone = it },
                label = { Text("+55 11 99999-9999") },
                supportingText = { Text("Inclua o código do país (ex: +55 para Brasil)") },
                singleLine = true,
                keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Phone),
            )
            Button(onClick = { if (phone.isNotBlank()) onPair(phone.trim()) }) {
                Text("Obter Código de Pareamento")
            }
        }
    }
}

@Composable
fun SendCard(enabled: Boolean, onSend: (String, String) -> Unit) {
    var to by remember { mutableStateOf("") }
    var text by remember { mutableStateOf("") }
    Card {
        Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(12.dp)) {
            Text("Envio de teste", style = MaterialTheme.typography.titleMedium)
            OutlinedTextField(
                value = to,
                onValueChange = { to = it },
                label = { Text("Destino (número ou JID)") },
                singleLine = true,
                enabled = enabled,
            )
            OutlinedTextField(
                value = text,
                onValueChange = { text = it },
                label = { Text("Texto") },
                enabled = enabled,
            )
            Button(onClick = { if (to.isNotBlank() && text.isNotBlank()) onSend(to.trim(), text) }, enabled = enabled) {
                Text("Enviar")
            }
        }
    }
}

@Composable
fun InboundCard(onDrain: ((List<Map<String, Any?>>) -> Unit) -> Unit) {
    var events by remember { mutableStateOf<List<Map<String, Any?>>>(emptyList()) }
    Card {
        Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
            Text("Eventos recebidos", style = MaterialTheme.typography.titleMedium)
            Button(onClick = { onDrain { events = it } }) { Text("Buscar eventos") }
            if (events.isEmpty()) {
                Text("Nenhum evento ainda.", style = MaterialTheme.typography.bodySmall)
            } else {
                events.take(20).forEach { e ->
                    Text(e.toString(), style = MaterialTheme.typography.bodySmall)
                }
            }
        }
    }
}